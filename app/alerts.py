"""Consecutive-threshold alerting engine.

Semantics
---------
A rule belongs to one device and watches a single *top-level numeric*
payload field. Every record is evaluated exactly once: when it first
becomes part of the device's contiguous ``committed`` prefix (a direct
in-order commit or a ``buffered`` record drained when the window
advances). Records inserted as ``gap_filled`` (断档补录) are never
evaluated, and duplicate batches hit the idempotent record lookup, so
they never re-count.

Evaluation is driven by a per-device frontier (``devices.alert_seq``):
after every write the engine walks committed records past the frontier
in strictly ascending seq order, stopping at the first hole. This makes
rule order independent of commit order across concurrent transactions --
a record committed by a racing late transaction is reached by the walk
only once its predecessors make the prefix contiguous, so a run is
always advanced in seq order.

The very first walk pins its origin at the device's lowest committed
record. A window rebase (the mechanism that corrects a window start
anchored too high by racing first batches) that later commits seqs below
that origin is not a 断档 and does not break runs: those records arrive
physically after the run ahead of them, so they are not evaluated, and
the existing run continues.

Per rule, a run counts consecutive evaluated records whose field
violates ``threshold`` via ``gt``/``lt``. The run is broken (counter
back to zero) when:

* a 断档 is declared for the device -- the missing seqs are skipped and
  no longer available to extend the run; the frontier jumps past the
  hole with all runs reset, and backfilling the gap later neither
  resumes nor recomputes anything;
* the watched field is missing from the payload or is not numeric
  (booleans/strings/null/NaN/Infinity all break the run);
* a record whose field is present, numeric and does not violate the
  threshold arrives ("恢复正常").

The first record that brings the run to ``consecutive_count`` generates
exactly one alert; later records of the same violating run do not. After
the run breaks it must grow back to ``consecutive_count`` from zero
before another alert can fire.

All methods run inside the caller's write transaction; the engine is
created per request like WindowEngine, so the rule set is read once and
reused across the batch and sweeper drains within that transaction.
"""
from __future__ import annotations

import json
import math
import numbers
import time
from typing import Any, Optional


class AlertEngine:
    def __init__(self, conn):
        self.conn = conn
        # Per-engine (per-transaction) rule cache: device_id -> list of rule rows.
        self._rules_cache: dict[str, Optional[list]] = {}

    # ------------------------------------------------------------------ #
    # rule management
    # ------------------------------------------------------------------ #
    def create_rule(
        self,
        device_id: str,
        field: str,
        op: str,
        threshold: float,
        consecutive_count: int,
        now: Optional[float] = None,
    ) -> dict:
        now = now if now is not None else time.time()
        cur = self.conn.execute(
            "INSERT INTO alert_rules (device_id, field, op, threshold, "
            "consecutive_count, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (device_id, field, op, float(threshold), consecutive_count, now),
        )
        self._rules_cache.pop(device_id, None)
        return self._rule_dict(
            self.conn.execute(
                "SELECT * FROM alert_rules WHERE id = ?", (cur.lastrowid,)
            ).fetchone()
        )

    def list_rules(self, device_id: Optional[str] = None) -> list[dict]:
        if device_id is not None:
            rows = self.conn.execute(
                "SELECT * FROM alert_rules WHERE device_id = ? ORDER BY id",
                (device_id,),
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM alert_rules ORDER BY id").fetchall()
        return [self._rule_dict(r) for r in rows]

    @staticmethod
    def _rule_dict(row) -> dict:
        return {
            "rule_id": row["id"],
            "device_id": row["device_id"],
            "field": row["field"],
            "op": row["op"],
            "threshold": row["threshold"],
            "consecutive_count": row["consecutive_count"],
            "created_at": row["created_at"],
        }

    # ------------------------------------------------------------------ #
    # alerts
    # ------------------------------------------------------------------ #
    def list_alerts(
        self,
        device_id: Optional[str] = None,
        rule_id: Optional[int] = None,
        limit: int = 100,
        after_id: Optional[int] = None,
    ) -> list[dict]:
        clauses = []
        params: list[Any] = []
        if device_id is not None:
            clauses.append("device_id = ?")
            params.append(device_id)
        if rule_id is not None:
            clauses.append("rule_id = ?")
            params.append(rule_id)
        if after_id is not None:
            clauses.append("id > ?")
            params.append(after_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.conn.execute(
            f"SELECT * FROM alerts {where} ORDER BY id ASC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [
            {
                "alert_id": r["id"],
                "device_id": r["device_id"],
                "rule_id": r["rule_id"],
                "field": r["field"],
                "op": r["op"],
                "threshold": r["threshold"],
                "consecutive_count": r["consecutive_count"],
                "trigger_seq": r["trigger_seq"],
                "run_start_seq": r["run_start_seq"],
                "value": r["value"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------ #
    # rule evaluation, driven by the window engine
    # ------------------------------------------------------------------ #
    def advance_device(self, device_id: str, jump_to: Optional[int] = None) -> None:
        """Advance every rule of a device over newly committed records.

        Walks the contiguous ``committed`` prefix past the per-device
        frontier in strictly ascending seq order, so records are
        evaluated exactly once regardless of which transaction committed
        them. Called once after every batch (and every background sweep
        that declares a gap).

        ``jump_to`` is set when a 断档 has just been declared: all runs
        break, the frontier jumps to ``jump_to - 1`` (the run beyond the
        hole drains afterwards and extends the frontier), and neither the
        hole's missing seqs nor later gap_filled backfills are evaluated.
        """
        dev = self.conn.execute(
            "SELECT alert_seq FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        if dev is None:
            return

        if jump_to is not None:
            self._break_runs(device_id)
            self.conn.execute(
                "UPDATE devices SET alert_seq = ? WHERE device_id = ?",
                (jump_to - 1, device_id),
            )
            dev = self.conn.execute(
                "SELECT alert_seq FROM devices WHERE device_id = ?", (device_id,)
            ).fetchone()

        frontier = dev["alert_seq"]
        if frontier is None:
            start = self._lowest_committed(device_id)
            if start is None:
                return
        else:
            start = frontier + 1

        rules = self._rules_for(device_id)
        states: dict[int, Optional[dict]] = {r["id"]: None for r in rules}
        states.update(self._load_states(rules))
        now = time.time()

        seq = start
        while True:
            row = self.conn.execute(
                "SELECT payload FROM records WHERE device_id = ? "
                "AND status = 'committed' AND seq = ?",
                (device_id, seq),
            ).fetchone()
            if row is None:
                break
            payload = self._parse(row["payload"])
            for rule in rules:
                self._advance_rule(rule, seq, payload, states, now)
            seq += 1

        last = seq - 1
        if last >= start:
            self._persist_states(states)
            self.conn.execute(
                "UPDATE devices SET alert_seq = ? WHERE device_id = ?",
                (last, device_id),
            )

    def _lowest_committed(self, device_id: str) -> Optional[int]:
        row = self.conn.execute(
            "SELECT MIN(seq) AS m FROM records WHERE device_id = ? "
            "AND status = 'committed'",
            (device_id,),
        ).fetchone()
        return None if row is None else row["m"]

    def _break_runs(self, device_id: str) -> None:
        """Interrupt every active run of the device (a 断档 was declared)."""
        self.conn.execute(
            "DELETE FROM alert_rule_state WHERE rule_id IN "
            "(SELECT id FROM alert_rules WHERE device_id = ?)",
            (device_id,),
        )

    # ------------------------------------------------------------------ #
    # internals
    # ------------------------------------------------------------------ #
    def _rules_for(self, device_id: str) -> list:
        rules = self._rules_cache.get(device_id)
        if rules is None:
            rules = self.conn.execute(
                "SELECT * FROM alert_rules WHERE device_id = ?", (device_id,)
            ).fetchall()
            self._rules_cache[device_id] = rules
        return rules

    @staticmethod
    def _parse(payload_json: str) -> Any:
        try:
            return json.loads(payload_json)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _numeric(value: Any) -> Optional[float]:
        """Top-level numeric field: real JSON numbers only.

        Booleans are rejected even though ``bool`` is a Python ``int``;
        NaN/Infinity are rejected because no finite threshold ordering
        makes sense for them.
        """
        if isinstance(value, bool) or not isinstance(value, numbers.Real):
            return None
        value = float(value)
        if not math.isfinite(value):
            return None
        return value

    def _load_states(self, rules: list) -> dict[int, dict]:
        ids = [r["id"] for r in rules]
        if not ids:
            return {}
        placeholders = ",".join("?" for _ in ids)
        return {
            row["rule_id"]: {
                "run_length": row["run_length"],
                "run_start_seq": row["run_start_seq"],
            }
            for row in self.conn.execute(
                f"SELECT rule_id, run_length, run_start_seq FROM alert_rule_state "
                f"WHERE rule_id IN ({placeholders})",
                ids,
            )
        }

    def _advance_rule(self, rule, seq: int, payload: Any, states: dict, now: float) -> None:
        rid = rule["id"]
        state = states.get(rid)
        if not isinstance(payload, dict) or rule["field"] not in payload:
            states[rid] = None  # missing field breaks the run
            return

        number = self._numeric(payload[rule["field"]])
        if number is None:
            states[rid] = None  # non-numeric field breaks the run
            return

        violates = (
            number > rule["threshold"]
            if rule["op"] == "gt"
            else number < rule["threshold"]
        )
        if not violates:
            states[rid] = None  # healthy again: run ends, rule rearms
            return

        if state is None:
            state = {"run_length": 0, "run_start_seq": seq}
        state["run_length"] += 1
        states[rid] = state

        # Fire only on the record that first brings the run to the count;
        # subsequent violating records keep the run but never refire.
        if state["run_length"] == rule["consecutive_count"]:
            self.conn.execute(
                "INSERT INTO alerts (device_id, rule_id, field, op, threshold, "
                "consecutive_count, trigger_seq, run_start_seq, value, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    rule["device_id"], rid, rule["field"], rule["op"],
                    rule["threshold"], rule["consecutive_count"], seq,
                    state["run_start_seq"], number, now,
                ),
            )

    def _persist_states(self, states: dict) -> None:
        broken_ids = [rid for rid, state in states.items() if state is None]
        if broken_ids:
            placeholders = ",".join("?" for _ in broken_ids)
            self.conn.execute(
                f"DELETE FROM alert_rule_state WHERE rule_id IN ({placeholders})",
                broken_ids,
            )
        for rid, state in states.items():
            if state is None:
                continue
            self.conn.execute(
                "INSERT INTO alert_rule_state (rule_id, run_length, run_start_seq) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(rule_id) DO UPDATE SET run_length = excluded.run_length, "
                "run_start_seq = excluded.run_start_seq",
                (rid, state["run_length"], state["run_start_seq"]),
            )
