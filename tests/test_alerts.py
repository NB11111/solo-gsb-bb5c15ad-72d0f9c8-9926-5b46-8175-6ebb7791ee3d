"""Consecutive-threshold alerting tests.

Covers: firing only on first reach of the count, one alert per violating
run, healthy/missing/non-numeric run breaks, buffered-flush evaluation in
seq order, duplicate-batch non-recounting, 断档 reset without backfill
recalculation, rule/list and alert query APIs.
"""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "test.db"),
        window_size=100,
        gap_timeout_seconds=0.25,
        sweep_enabled=False,  # deterministic: gaps are declared lazily
    )
    with TestClient(create_app(settings)) as c:
        yield c


def submit(client, device, items):
    """items: list of (seq, payload)."""
    resp = client.post(
        "/api/v1/batches",
        json={
            "device_id": device,
            "records": [{"seq": s, "payload": p} for s, p in items],
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def statuses(body):
    return {r["seq"]: r["status"] for r in body["results"]}


def make_rule(client, device="dev-a", field="temp", op="gt", threshold=90, count=3):
    resp = client.post(
        f"/api/v1/devices/{device}/alert-rules",
        json={"field": field, "op": op, "threshold": threshold,
              "consecutive_count": count},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def alerts(client, **params):
    resp = client.get("/api/v1/alerts", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()["alerts"]


def device_alerts(client, device, **params):
    resp = client.get(f"/api/v1/devices/{device}/alerts", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()["alerts"]


def vals(seqs, value=95, field="temp"):
    return [(s, {field: value}) for s in seqs]


# --------------------------------------------------------------------- #
# basic firing / once-per-run / rearm
# --------------------------------------------------------------------- #

def test_fires_on_first_reach_of_consecutive_count(client):
    rule = make_rule(client, count=3)
    body = submit(client, "dev-a", vals([1, 2, 3]))
    assert all(st == "committed" for st in statuses(body).values())

    got = alerts(client, device_id="dev-a")
    assert len(got) == 1
    a = got[0]
    assert a["rule_id"] == rule["rule_id"]
    assert a["device_id"] == "dev-a"
    assert a["field"] == "temp" and a["op"] == "gt"
    assert a["threshold"] == 90 and a["consecutive_count"] == 3
    assert a["trigger_seq"] == 3
    assert a["run_start_seq"] == 1
    assert a["value"] == 95.0


def test_no_alert_before_count_reached(client):
    make_rule(client, count=3)
    submit(client, "dev-a", vals([1, 2]))
    assert alerts(client) == []


def test_same_violating_run_generates_only_one_alert(client):
    make_rule(client, count=3)
    submit(client, "dev-a", vals(range(1, 8)))  # 7 violating records in one run
    got = alerts(client)
    assert [a["trigger_seq"] for a in got] == [3]


def test_rearms_only_after_returning_to_normal(client):
    make_rule(client, count=2)
    submit(client, "dev-a", vals([1, 2]))  # fire on 2
    # Still violating: no second alert even though another pair passes.
    submit(client, "dev-a", vals([3, 4, 5]))
    assert len(alerts(client)) == 1

    submit(client, "dev-a", vals([6], value=80))   # healthy -> run ends
    submit(client, "dev-a", vals([7, 8]))          # new violating run
    got = alerts(client)
    assert [a["trigger_seq"] for a in got] == [2, 8]
    assert got[1]["run_start_seq"] == 7


def test_count_one_fires_per_violating_run(client):
    make_rule(client, count=1)
    submit(client, "dev-a", vals([1, 2]))          # one run, one alert
    submit(client, "dev-a", vals([3], value=80))   # healthy
    submit(client, "dev-a", vals([4]))             # new run -> second alert
    assert [a["trigger_seq"] for a in alerts(client)] == [1, 4]


def test_lt_operator(client):
    make_rule(client, op="lt", threshold=10, count=2)
    submit(client, "dev-a", vals([1, 2], value=5))
    got = alerts(client)
    assert len(got) == 1 and got[0]["op"] == "lt" and got[0]["value"] == 5.0

    submit(client, "dev-a", vals([3], value=50))  # healthy
    submit(client, "dev-a", vals([4], value=99))  # above threshold, healthy for lt
    assert len(alerts(client)) == 1


# --------------------------------------------------------------------- #
# run breaks: missing / non-numeric fields
# --------------------------------------------------------------------- #

def test_missing_field_breaks_run(client):
    make_rule(client, count=3)
    submit(client, "dev-a", vals([1, 2]))
    submit(client, "dev-a", [(3, {"other": 1})])  # field missing -> break
    submit(client, "dev-a", vals([4]))             # run restarts at 1
    assert alerts(client) == []
    submit(client, "dev-a", vals([5, 6]))          # reaches 3 in the new run
    got = alerts(client)
    assert len(got) == 1
    assert got[0]["run_start_seq"] == 4 and got[0]["trigger_seq"] == 6


@pytest.mark.parametrize("bad", ["hot", True, False, None, ["x"], {"nested": 99}])
def test_non_numeric_field_breaks_run(client, bad):
    make_rule(client, count=2)
    submit(client, "dev-a", vals([1]))
    submit(client, "dev-a", [(2, {"temp": bad})])  # non-numeric -> break
    submit(client, "dev-a", vals([3]))
    assert alerts(client) == []
    submit(client, "dev-a", vals([4]))
    assert [a["trigger_seq"] for a in alerts(client)] == [4]


def test_integer_payload_values_are_numeric(client):
    make_rule(client, count=2)
    submit(client, "dev-a", [(1, {"temp": 91}), (2, {"temp": 100})])
    assert len(alerts(client)) == 1


# --------------------------------------------------------------------- #
# buffered flush: evaluate one by one in seq order, exactly once
# --------------------------------------------------------------------- #

def test_buffered_flush_evaluated_record_by_record_in_seq_order(client):
    make_rule(client, count=3)
    body = submit(client, "dev-a", vals([1, 3, 4]))  # 3,4 buffered behind gap at 2
    assert statuses(body) == {
        1: "committed", 3: "buffered", 4: "buffered",
    }
    assert alerts(client) == []  # run length only 1 so far

    body = submit(client, "dev-a", vals([2]))  # commits 2 and drains 3,4
    assert statuses(body)[2] == "committed"

    got = alerts(client)
    assert len(got) == 1
    # The flush processed 2, then 3, then 4 in seq order: the run
    # continues across the batch boundary and completes at seq 3.
    assert got[0]["run_start_seq"] == 1 and got[0]["trigger_seq"] == 3


def test_single_batch_flushing_many_buffered_counts_each_once(client):
    make_rule(client, count=4)
    submit(client, "dev-a", vals([1, 2, 3, 5, 6]))  # run=3; 5,6 buffered over gap 4
    assert alerts(client) == []
    submit(client, "dev-a", vals([4]))               # reaches 4, then drains 5,6
    got = alerts(client)
    assert len(got) == 1 and got[0]["trigger_seq"] == 4
    # 5 and 6 belong to the same run and must not produce more alerts.
    submit(client, "dev-a", vals([7]))
    assert len(alerts(client)) == 1


def test_duplicate_batch_does_not_recount(client):
    make_rule(client, count=3)
    submit(client, "dev-a", vals([1, 2, 3]))
    assert [a["trigger_seq"] for a in alerts(client)] == [3]

    # Retrying the same batch is fully idempotent and never re-evaluates.
    body = submit(client, "dev-a", vals([1, 2, 3]))
    assert all(st == "duplicate" for st in statuses(body).values())
    assert len(alerts(client)) == 1

    # And the already-committed run cannot be extended by mixing a
    # duplicate with fresh records beyond a healthy break either.
    submit(client, "dev-a", vals([4], value=80))
    body = submit(client, "dev-a", vals([2] + [5]))  # seq2 duplicate, seq5 violating
    assert statuses(body)[2] == "duplicate"
    assert alerts(client)  # still just the original one until a new run of 3 exists
    submit(client, "dev-a", vals([6, 7]))
    assert [a["trigger_seq"] for a in alerts(client)] == [3, 7]


# --------------------------------------------------------------------- #
# declared gap (断档) interrupts the run; backfill does not recompute
# --------------------------------------------------------------------- #

def test_declared_gap_resets_run_and_drains_start_fresh(client):
    make_rule(client, count=2)
    submit(client, "dev-a", vals([1, 3, 4]))  # run=1 at seq1; 3,4 buffered over gap 2
    time.sleep(0.3)

    # Lazy sweep on read declares 断档 [2,2]: the run resets, then 3 and 4
    # drain as new commits and form a fresh length-2 run -> one alert whose
    # run starts at 3, not 1.
    win = client.get("/api/v1/devices/dev-a/window").json()
    assert win["next_expected"] == 5
    got = alerts(client)
    assert len(got) == 1
    assert got[0]["run_start_seq"] == 3 and got[0]["trigger_seq"] == 4

    # Backfilling the gap later is never evaluated: no resume, no recount.
    body = submit(client, "dev-a", vals([2]))
    assert statuses(body)[2] == "gap_filled"
    assert len(alerts(client)) == 1


def test_declared_gap_run_cannot_reach_count_without_new_records(client):
    make_rule(client, count=3)
    submit(client, "dev-a", vals([1, 2, 4, 5]))  # run=2; 4,5 buffered over gap 3
    time.sleep(0.3)
    client.get("/api/v1/devices/dev-a/gaps")  # declares gap, drains 4,5: run only 2

    assert alerts(client) == []
    submit(client, "dev-a", vals([3]))  # gap backfill -> not evaluated
    assert alerts(client) == []
    submit(client, "dev-a", vals([6]))  # run 4,5,6 -> 3, fires
    got = alerts(client)
    assert len(got) == 1 and got[0]["run_start_seq"] == 4 and got[0]["trigger_seq"] == 6


def test_gap_declared_via_batch_path_also_resets(client):
    make_rule(client, count=3)
    submit(client, "dev-a", vals([1, 2, 4]))  # run=2; 4 buffered over gap 3
    time.sleep(0.3)
    # A new batch sweeps first (gap declared, 4 drains: run=1), then commits 5.
    submit(client, "dev-a", vals([5]))
    assert alerts(client) == []  # still only 2 after reset
    submit(client, "dev-a", vals([6]))
    assert [a["trigger_seq"] for a in alerts(client)] == [6]


# --------------------------------------------------------------------- #
# rule timing & independence
# --------------------------------------------------------------------- #

def test_late_rebase_below_pinned_frontier_keeps_run(client):
    # Concurrent first-batch anchor race (deterministic two-phase shape):
    # a high-anchored run pins the alert frontier; lower seqs arriving
    # later via the window-rebase correction are NOT a 断档. They are not
    # re-evaluated (the run ahead of them already started), so the
    # existing run is neither broken nor restarted -- exactly one alert.
    make_rule(client, count=3)
    submit(client, "dev-a", vals([7, 8, 9]))  # pins origin at 7, fires at 9
    assert [a["trigger_seq"] for a in alerts(client)] == [9]
    body = submit(client, "dev-a", vals([1, 2, 3, 4, 5, 6]))
    assert all(st == "committed" for st in statuses(body).values())
    got = alerts(client)
    assert len(got) == 1 and got[0]["run_start_seq"] == 7 and got[0]["trigger_seq"] == 9
    # Healthy later records still rearm normally.
    submit(client, "dev-a", vals([10], value=80))
    submit(client, "dev-a", vals([11, 12, 13]))
    assert [a["trigger_seq"] for a in alerts(client)] == [9, 13]


def test_rule_created_after_history_does_not_back_evaluate(client):
    submit(client, "dev-a", vals([1, 2, 3]))  # violating data, no rule yet
    make_rule(client, count=3)
    assert alerts(client) == []
    submit(client, "dev-a", vals([4, 5]))  # run restarts after rule creation
    assert alerts(client) == []
    submit(client, "dev-a", vals([6]))
    assert [a["trigger_seq"] for a in alerts(client)] == [6]


def test_rules_are_per_device_and_independent(client):
    make_rule(client, device="dev-a", count=2)
    make_rule(client, device="dev-b", count=2)
    submit(client, "dev-a", vals([1, 2]))
    got_a = alerts(client, device_id="dev-a")
    got_b = alerts(client, device_id="dev-b")
    assert len(got_a) == 1 and got_a[0]["device_id"] == "dev-a"
    assert got_b == []
    assert len(device_alerts(client, "dev-b")) == 0


def test_multiple_rules_advance_independently(client):
    make_rule(client, field="temp", op="gt", threshold=90, count=2)
    make_rule(client, field="hum", op="lt", threshold=20, count=2)
    submit(client, "dev-a", [(1, {"temp": 95, "hum": 15}),
                             (2, {"temp": 96, "hum": 10})])
    got = alerts(client)
    assert {(a["field"], a["trigger_seq"]) for a in got} == {
        ("temp", 2), ("hum", 2),
    }

    # A healthy record for one field breaks only that rule's run.
    submit(client, "dev-a", [(3, {"temp": 80, "hum": 50}),  # both healthy -> both rearm
                             (4, {"temp": 80, "hum": 5}),
                             (5, {"temp": 80, "hum": 4})])
    got = alerts(client)
    # hum's new run fires at 5; temp stays healthy and silent.
    assert sorted((a["field"], a["trigger_seq"]) for a in got) == [
        ("hum", 2), ("hum", 5), ("temp", 2),
    ]


# --------------------------------------------------------------------- #
# rule & alert query APIs
# --------------------------------------------------------------------- #

def test_rule_list_endpoints(client):
    make_rule(client, device="dev-a", count=2)
    make_rule(client, device="dev-b", count=5)

    rows = client.get("/api/v1/devices/dev-a/alert-rules").json()
    assert rows["device_id"] == "dev-a" and len(rows["rules"]) == 1
    assert rows["rules"][0]["consecutive_count"] == 2

    all_rules = client.get("/api/v1/alert-rules").json()["rules"]
    assert [r["device_id"] for r in all_rules] == ["dev-a", "dev-b"]


def test_alerts_filtering_and_keyset(client):
    r1 = make_rule(client, device="dev-a", count=1)
    r2 = make_rule(client, device="dev-b", count=1)
    submit(client, "dev-a", vals([1, 2]))  # one alert (single run)
    submit(client, "dev-b", vals([1]))

    assert len(alerts(client)) == 2
    by_rule = alerts(client, rule_id=r1["rule_id"])
    assert len(by_rule) == 1 and by_rule[0]["rule_id"] == r1["rule_id"]
    assert len(alerts(client, device_id="dev-b", rule_id=r2["rule_id"])) == 1

    page = client.get("/api/v1/alerts", params={"limit": 1}).json()["alerts"]
    assert len(page) == 1
    rest = alerts(client, after_id=page[0]["alert_id"])
    assert [a["alert_id"] for a in rest] == [page[0]["alert_id"] + 1]

    assert client.get(
        "/api/v1/devices/dev-a/alerts", params={"rule_id": r2["rule_id"]}
    ).json()["alerts"] == []


def test_rule_validation_rejects_bad_inputs(client):
    def post(body):
        return client.post(
            "/api/v1/devices/dev-a/alert-rules", json=body
        )

    assert post({"field": "temp", "op": "eq", "threshold": 1,
                 "consecutive_count": 1}).status_code == 422
    assert post({"field": "a.b", "op": "gt", "threshold": 1,
                 "consecutive_count": 1}).status_code == 422
    assert post({"field": "temp", "op": "gt", "threshold": 1,
                 "consecutive_count": 0}).status_code == 422
    assert post({"field": "", "op": "gt", "threshold": 1,
                 "consecutive_count": 1}).status_code == 422
    assert post({"field": "temp", "op": "gt", "threshold": "NaN",
                 "consecutive_count": 1}).status_code == 422
