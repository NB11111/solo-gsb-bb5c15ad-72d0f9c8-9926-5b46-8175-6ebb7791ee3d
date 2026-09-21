"""SQLite access layer.

One connection per request (created by the FastAPI dependency in main.py).
WAL mode + busy_timeout allow concurrent readers and serialized writers;
all mutations happen inside BEGIN IMMEDIATE transactions (see write_txn),
which keeps per-device window state consistent under concurrent requests.
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id        TEXT PRIMARY KEY,
    next_expected    INTEGER NOT NULL,          -- next contiguous sequence expected
    gap_since        REAL,                      -- when the current pending gap was first seen
    duplicates_count INTEGER NOT NULL DEFAULT 0,
    alert_seq        INTEGER,                   -- alert frontier: last seq evaluated
                                                -- (NULL = no origin pinned yet)
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS records (
    device_id   TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    payload     TEXT NOT NULL,                  -- JSON payload as received
    status      TEXT NOT NULL CHECK (status IN ('committed', 'buffered', 'gap_filled')),
    received_at REAL NOT NULL,
    PRIMARY KEY (device_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_records_device_status ON records(device_id, status);

CREATE TABLE IF NOT EXISTS gaps (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL,
    start_seq    INTEGER NOT NULL,
    end_seq      INTEGER NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('open', 'replaying', 'recovered')),
    replay_count INTEGER NOT NULL DEFAULT 0,
    detected_at  REAL NOT NULL,                 -- when the gap was first observed
    updated_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gaps_device ON gaps(device_id, status);

-- Consecutive-threshold alert rules, scoped per device. Each rule watches a
-- single top-level numeric payload field and fires once per violating run.
CREATE TABLE IF NOT EXISTS alert_rules (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id         TEXT NOT NULL,
    field             TEXT NOT NULL,            -- top-level payload field name
    op                TEXT NOT NULL CHECK (op IN ('gt', 'lt')),
    threshold         REAL NOT NULL,
    consecutive_count INTEGER NOT NULL CHECK (consecutive_count >= 1),
    created_at        REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alert_rules_device ON alert_rules(device_id);

-- Per-rule streak state. A row exists only while a violating run is active
-- (or has fired); it is deleted whenever a 断档 is declared and rewritten on
-- every evaluated record.
CREATE TABLE IF NOT EXISTS alert_rule_state (
    rule_id       INTEGER PRIMARY KEY REFERENCES alert_rules(id),
    run_length    INTEGER NOT NULL DEFAULT 0,   -- consecutive violating committed records
    run_start_seq INTEGER                       -- seq where the current run began
);

-- One row per fired alert (at most one per consecutive violating run).
CREATE TABLE IF NOT EXISTS alerts (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id         TEXT NOT NULL,
    rule_id           INTEGER NOT NULL,
    field             TEXT NOT NULL,            -- rule snapshot for self-contained queries
    op                TEXT NOT NULL,
    threshold         REAL NOT NULL,
    consecutive_count INTEGER NOT NULL,
    trigger_seq       INTEGER NOT NULL,         -- record on which the run first reached the count
    run_start_seq     INTEGER NOT NULL,         -- first seq of the violating run
    value             REAL NOT NULL,
    created_at        REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alerts_device ON alerts(device_id, id);
"""


def connect(db_path: str) -> sqlite3.Connection:
    # check_same_thread=False: FastAPI may run a request's dependency setup,
    # endpoint and teardown on different threadpool threads. Each connection
    # is still confined to a single request, and BEGIN IMMEDIATE serializes
    # writers, so this stays safe.
    conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str) -> None:
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        _migrate(conn)
    finally:
        conn.close()


def _migrate(conn: sqlite3.Connection) -> None:
    """Additive migrations for databases created by older releases."""
    dev_cols = {r["name"] for r in conn.execute("PRAGMA table_info(devices)")}
    if "alert_seq" not in dev_cols:
        conn.execute("ALTER TABLE devices ADD COLUMN alert_seq INTEGER")


@contextmanager
def write_txn(conn: sqlite3.Connection):
    """Exclusive write transaction; serializes writers at the SQLite level."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
