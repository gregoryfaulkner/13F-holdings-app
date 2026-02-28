"""
SQLite Persistence Layer for 13F Holdings App
==============================================
Stores fetch run snapshots so historical data survives restarts and
enables instant QoQ comparison without re-fetching from EDGAR.

Tables:
    runs      — one row per fetch execution (metadata + config snapshot)
    holdings  — one row per manager-stock position (all 32 enrichment fields)
"""

import json
import os
import sqlite3
import threading
from datetime import datetime

# ── Module state ──────────────────────────────────────────────────────────────

_db_path = None
_local = threading.local()


def init(db_path=None):
    """Initialise the module with a database path and create tables if needed."""
    global _db_path
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "holdings_history.db")
    _db_path = db_path
    _create_tables()


def _conn():
    """Return a thread-local connection (SQLite is not thread-safe by default)."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(_db_path)
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA foreign_keys=ON")
        _local.conn.row_factory = sqlite3.Row
    return _local.conn


# ── Schema ────────────────────────────────────────────────────────────────────

_HOLDINGS_COLUMNS = [
    "manager", "period_of_report", "filed_at", "rank", "name", "ticker",
    "value_usd", "pct_of_portfolio",
    # Prior quarter
    "prior_price_qtr_end", "prior_quarter_return_pct", "prior_trailing_pe",
    "prior_reported_eps", "prior_consensus_eps", "prior_eps_beat_dollars",
    "prior_eps_beat_pct",
    # Filing quarter
    "filing_price_qtr_end", "filing_quarter_return_pct", "filing_trailing_pe",
    "filing_reported_eps", "filing_consensus_eps", "filing_eps_beat_dollars",
    "filing_eps_beat_pct",
    # Forward / live
    "forward_pe", "forward_eps_growth", "dividend_yield",
    "trailing_eps", "forward_eps",
    # QTD
    "qtd_return_pct", "qtd_price_start",
    # Static
    "sector", "industry", "country",
    # ESG
    "esg_score", "esg_environmental", "esg_social", "esg_governance",
]


def _create_tables():
    conn = _conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date        TEXT    NOT NULL,
            created_at      TEXT    NOT NULL,
            max_date        TEXT,
            top_n           INTEGER,
            managers_json   TEXT,
            files_json      TEXT,
            errors_json     TEXT,
            manager_results_json TEXT,
            label           TEXT
        );

        CREATE TABLE IF NOT EXISTS holdings (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id                  INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
            manager                 TEXT NOT NULL,
            period_of_report        TEXT,
            filed_at                TEXT,
            rank                    INTEGER,
            name                    TEXT,
            ticker                  TEXT,
            value_usd               INTEGER,
            pct_of_portfolio        REAL,
            prior_price_qtr_end     REAL,
            prior_quarter_return_pct REAL,
            prior_trailing_pe       REAL,
            prior_reported_eps      REAL,
            prior_consensus_eps     REAL,
            prior_eps_beat_dollars  REAL,
            prior_eps_beat_pct      REAL,
            filing_price_qtr_end    REAL,
            filing_quarter_return_pct REAL,
            filing_trailing_pe      REAL,
            filing_reported_eps     REAL,
            filing_consensus_eps    REAL,
            filing_eps_beat_dollars REAL,
            filing_eps_beat_pct     REAL,
            forward_pe              REAL,
            forward_eps_growth      REAL,
            dividend_yield          REAL,
            trailing_eps            REAL,
            forward_eps             REAL,
            sector                  TEXT,
            industry                TEXT,
            country                 TEXT,
            esg_score               REAL,
            esg_environmental       REAL,
            qtd_return_pct          REAL,
            qtd_price_start         REAL,
            esg_social              REAL,
            esg_governance          REAL
        );

        CREATE INDEX IF NOT EXISTS idx_holdings_run    ON holdings(run_id);
        CREATE INDEX IF NOT EXISTS idx_holdings_ticker ON holdings(ticker);
        CREATE INDEX IF NOT EXISTS idx_holdings_mgr    ON holdings(manager);
    """)
    conn.commit()

    # Migrate: add new columns if missing (for existing databases)
    try:
        cursor = conn.execute("PRAGMA table_info(holdings)")
        existing_cols = {row[1] for row in cursor.fetchall()}
        for col_name, col_type in [("trailing_eps", "REAL"), ("forward_eps", "REAL"),
                                    ("qtd_return_pct", "REAL"), ("qtd_price_start", "REAL")]:
            if col_name not in existing_cols:
                conn.execute(f"ALTER TABLE holdings ADD COLUMN {col_name} {col_type}")
                print(f"[DB] Added column {col_name} to holdings table")
        conn.commit()
    except Exception:
        pass


# ── Save a run ────────────────────────────────────────────────────────────────

def save_run(results, cfg, label=None):
    """
    Persist a completed fetch run.

    Args:
        results: the dict returned by run_fetch() — must contain 'all_rows', 'run_date',
                 'managers' (status list), 'files', 'errors'.
        cfg:     the config dict at time of run.
        label:   optional human-readable label (e.g. "Q4 2025 Major Managers").

    Returns:
        int — the new run id.
    """
    conn = _conn()
    all_rows = results.get("all_rows", [])
    if not all_rows:
        return None

    # Combine 13F + NPORT managers for the snapshot
    managers_snapshot = {}
    managers_snapshot.update(cfg.get("managers_13f", {}))
    for name, info in cfg.get("managers_nport", {}).items():
        managers_snapshot[name] = info

    cur = conn.execute(
        """INSERT INTO runs (run_date, created_at, max_date, top_n,
                             managers_json, files_json, errors_json,
                             manager_results_json, label)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            results.get("run_date", datetime.today().strftime("%Y%m%d")),
            datetime.now().isoformat(),
            cfg.get("max_date", ""),
            cfg.get("top_n", 20),
            json.dumps(managers_snapshot),
            json.dumps(results.get("files", [])),
            json.dumps(results.get("errors", [])),
            json.dumps([m for m in results.get("managers", [])]),
            label,
        ),
    )
    run_id = cur.lastrowid

    # Batch-insert holdings
    placeholders = ", ".join(["?"] * (len(_HOLDINGS_COLUMNS) + 1))  # +1 for run_id
    col_names = "run_id, " + ", ".join(_HOLDINGS_COLUMNS)
    sql = f"INSERT INTO holdings ({col_names}) VALUES ({placeholders})"

    rows_to_insert = []
    for row in all_rows:
        values = [run_id]
        for col in _HOLDINGS_COLUMNS:
            values.append(row.get(col))
        rows_to_insert.append(values)

    conn.executemany(sql, rows_to_insert)
    conn.commit()
    return run_id


# ── List runs ─────────────────────────────────────────────────────────────────

def list_runs(limit=50):
    """
    Return recent runs as a list of dicts, newest first.

    Each dict: {id, run_date, created_at, max_date, top_n, label,
                manager_count, holding_count, managers_json, errors_json}
    """
    conn = _conn()
    rows = conn.execute(
        """SELECT r.id, r.run_date, r.created_at, r.max_date, r.top_n,
                  r.label, r.managers_json, r.errors_json, r.files_json,
                  r.manager_results_json,
                  COUNT(h.id) AS holding_count
           FROM runs r
           LEFT JOIN holdings h ON h.run_id = r.id
           GROUP BY r.id
           ORDER BY r.id DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()

    result = []
    for r in rows:
        managers = json.loads(r["managers_json"] or "{}") if r["managers_json"] else {}
        result.append({
            "id": r["id"],
            "run_date": r["run_date"],
            "created_at": r["created_at"],
            "max_date": r["max_date"],
            "top_n": r["top_n"],
            "label": r["label"],
            "manager_count": len(managers),
            "holding_count": r["holding_count"],
            "files": json.loads(r["files_json"] or "[]"),
            "errors": json.loads(r["errors_json"] or "[]"),
        })
    return result


# ── Load a run ────────────────────────────────────────────────────────────────

def load_run(run_id):
    """
    Load a stored run, returning the same structure as run_fetch() would.

    Returns:
        dict with keys: all_rows, run_date, managers, files, errors
        — or None if run_id not found.
    """
    conn = _conn()
    run_row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if not run_row:
        return None

    holding_rows = conn.execute(
        "SELECT * FROM holdings WHERE run_id = ? ORDER BY manager, rank",
        (run_id,),
    ).fetchall()

    all_rows = []
    for h in holding_rows:
        row = {}
        for col in _HOLDINGS_COLUMNS:
            row[col] = h[col]
        all_rows.append(row)

    return {
        "all_rows": all_rows,
        "run_date": run_row["run_date"],
        "managers": json.loads(run_row["manager_results_json"] or "[]"),
        "files": json.loads(run_row["files_json"] or "[]"),
        "errors": json.loads(run_row["errors_json"] or "[]"),
    }


# ── Load just the holdings rows for a run (for diff comparisons) ──────────────

def load_run_rows(run_id):
    """Load only the all_rows list for a given run (lightweight for diffs)."""
    conn = _conn()
    holding_rows = conn.execute(
        "SELECT * FROM holdings WHERE run_id = ? ORDER BY manager, rank",
        (run_id,),
    ).fetchall()
    rows = []
    for h in holding_rows:
        row = {}
        for col in _HOLDINGS_COLUMNS:
            row[col] = h[col]
        rows.append(row)
    return rows


# ── Delete a run ──────────────────────────────────────────────────────────────

def delete_run(run_id):
    """Delete a run and its holdings (CASCADE)."""
    conn = _conn()
    conn.execute("DELETE FROM runs WHERE id = ?", (run_id,))
    conn.commit()


# ── Rename / label a run ─────────────────────────────────────────────────────

def label_run(run_id, label):
    """Set or update a run's label."""
    conn = _conn()
    conn.execute("UPDATE runs SET label = ? WHERE id = ?", (label, run_id))
    conn.commit()


# ── Ticker history across runs ────────────────────────────────────────────────

def ticker_history(ticker, limit=20):
    """
    Get a ticker's presence across recent runs.

    Returns list of dicts sorted by run date (oldest first):
        [{run_id, run_date, max_date, managers: [{manager, pct, value, rank}]}]
    """
    conn = _conn()
    rows = conn.execute(
        """SELECT h.run_id, r.run_date, r.max_date,
                  h.manager, h.pct_of_portfolio, h.value_usd, h.rank
           FROM holdings h
           JOIN runs r ON r.id = h.run_id
           WHERE h.ticker = ?
           ORDER BY r.id DESC
           LIMIT ?""",
        (ticker, limit * 20),  # generous limit since multiple managers per run
    ).fetchall()

    # Group by run
    from collections import OrderedDict
    by_run = OrderedDict()
    for r in rows:
        rid = r["run_id"]
        if rid not in by_run:
            by_run[rid] = {
                "run_id": rid,
                "run_date": r["run_date"],
                "max_date": r["max_date"],
                "managers": [],
            }
        by_run[rid]["managers"].append({
            "manager": r["manager"],
            "pct": r["pct_of_portfolio"],
            "value": r["value_usd"],
            "rank": r["rank"],
        })

    result = list(by_run.values())
    result.reverse()  # oldest first for charting
    return result[:limit]
