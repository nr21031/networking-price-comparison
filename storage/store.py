"""
SQLite-backed price store with full version history and change detection.

Schema
──────
price_snapshots   — every fetched price point, keyed by (provider, sku_id, fetched_at)
price_changes     — detected diffs between consecutive fetch runs

A "fetch run" is identified by a unique run_id (ISO timestamp of when the
overall fetch started). All records from a single run share the same run_id.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from fetchers.base import PricePoint

DB_DEFAULT = Path(__file__).parent.parent / "data" / "prices.db"

DDL = """
CREATE TABLE IF NOT EXISTS price_snapshots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id              TEXT    NOT NULL,
    provider            TEXT    NOT NULL,
    service             TEXT    NOT NULL,
    sku_id              TEXT    NOT NULL,
    sku_name            TEXT    NOT NULL,
    description         TEXT,
    port_speed_gbps     REAL,
    price_monthly_usd   REAL,
    price_per_gb_usd    REAL,
    unit_original       TEXT,
    price_original_usd  REAL,
    region_canonical    TEXT,
    region_label        TEXT,
    region_raw          TEXT,
    plan_type           TEXT,
    currency            TEXT    DEFAULT 'USD',
    effective_date      TEXT,
    fetched_at          TEXT    NOT NULL,
    source_url          TEXT,
    raw_data            TEXT,
    UNIQUE(run_id, provider, sku_id, region_raw)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_provider_sku
    ON price_snapshots(provider, sku_id);

CREATE INDEX IF NOT EXISTS idx_snapshots_run
    ON price_snapshots(run_id);

CREATE TABLE IF NOT EXISTS price_changes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    detected_at     TEXT    NOT NULL,
    run_id_old      TEXT,
    run_id_new      TEXT    NOT NULL,
    provider        TEXT    NOT NULL,
    service         TEXT,
    sku_id          TEXT    NOT NULL,
    sku_name        TEXT,
    region_canonical TEXT,
    region_raw      TEXT,
    change_type     TEXT    NOT NULL,   -- 'price_change' | 'new_sku' | 'removed_sku' | 'new_region'
    old_price_monthly REAL,
    new_price_monthly REAL,
    old_price_per_gb  REAL,
    new_price_per_gb  REAL,
    pct_change_monthly REAL,
    pct_change_per_gb  REAL,
    notified        INTEGER DEFAULT 0   -- 0 = pending notification, 1 = sent
);

CREATE INDEX IF NOT EXISTS idx_changes_run ON price_changes(run_id_new);
CREATE INDEX IF NOT EXISTS idx_changes_notified ON price_changes(notified);

CREATE TABLE IF NOT EXISTS fetch_runs (
    run_id      TEXT PRIMARY KEY,
    started_at  TEXT NOT NULL,
    completed_at TEXT,
    providers   TEXT,   -- JSON list of providers fetched
    record_count INTEGER DEFAULT 0,
    status      TEXT DEFAULT 'running'  -- 'running' | 'completed' | 'failed'
);
"""


@contextmanager
def _conn(db_path: Path):
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


class PriceStore:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path) if db_path else DB_DEFAULT
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ── Init ─────────────────────────────────────────────────────────────────

    def _init_db(self):
        with _conn(self.db_path) as con:
            con.executescript(DDL)

    # ── Runs ──────────────────────────────────────────────────────────────────

    def start_run(self, providers: List[str]) -> str:
        run_id = datetime.now(timezone.utc).isoformat()
        with _conn(self.db_path) as con:
            con.execute(
                "INSERT INTO fetch_runs(run_id, started_at, providers, status) VALUES(?,?,?,?)",
                (run_id, run_id, json.dumps(providers), "running"),
            )
        return run_id

    def complete_run(self, run_id: str, record_count: int):
        completed_at = datetime.now(timezone.utc).isoformat()
        with _conn(self.db_path) as con:
            con.execute(
                "UPDATE fetch_runs SET completed_at=?, record_count=?, status='completed' WHERE run_id=?",
                (completed_at, record_count, run_id),
            )

    def fail_run(self, run_id: str, error: str = ""):
        completed_at = datetime.now(timezone.utc).isoformat()
        with _conn(self.db_path) as con:
            con.execute(
                "UPDATE fetch_runs SET completed_at=?, status='failed' WHERE run_id=?",
                (completed_at, run_id),
            )

    # ── Snapshots ─────────────────────────────────────────────────────────────

    def save_prices(self, run_id: str, points: List[PricePoint]) -> int:
        """Insert all price points for a run. Returns number saved."""
        rows = []
        for p in points:
            rows.append((
                run_id, p.provider, p.service, p.sku_id, p.sku_name, p.description,
                p.port_speed_gbps, p.price_monthly_usd, p.price_per_gb_usd,
                p.unit_original, p.price_original_usd,
                p.region_canonical, p.region_label, p.region_raw,
                p.plan_type, p.currency, p.effective_date, p.fetched_at,
                p.source_url, p.raw_data,
            ))
        with _conn(self.db_path) as con:
            con.executemany(
                """INSERT OR IGNORE INTO price_snapshots(
                    run_id, provider, service, sku_id, sku_name, description,
                    port_speed_gbps, price_monthly_usd, price_per_gb_usd,
                    unit_original, price_original_usd,
                    region_canonical, region_label, region_raw,
                    plan_type, currency, effective_date, fetched_at,
                    source_url, raw_data
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                rows,
            )
        return len(rows)

    def get_latest_run(self, provider: Optional[str] = None) -> Optional[str]:
        """Return the most recent completed run_id, optionally filtered by provider."""
        with _conn(self.db_path) as con:
            if provider:
                row = con.execute(
                    "SELECT run_id FROM fetch_runs WHERE status='completed' "
                    "AND providers LIKE ? ORDER BY started_at DESC LIMIT 1",
                    (f"%{provider}%",),
                ).fetchone()
            else:
                row = con.execute(
                    "SELECT run_id FROM fetch_runs WHERE status='completed' "
                    "ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
        return row["run_id"] if row else None

    def get_previous_run(self, current_run_id: str, provider: Optional[str] = None) -> Optional[str]:
        """Return the run_id immediately before current_run_id."""
        with _conn(self.db_path) as con:
            if provider:
                row = con.execute(
                    "SELECT run_id FROM fetch_runs WHERE status='completed' "
                    "AND run_id < ? AND providers LIKE ? ORDER BY run_id DESC LIMIT 1",
                    (current_run_id, f"%{provider}%"),
                ).fetchone()
            else:
                row = con.execute(
                    "SELECT run_id FROM fetch_runs WHERE status='completed' "
                    "AND run_id < ? ORDER BY run_id DESC LIMIT 1",
                    (current_run_id,),
                ).fetchone()
        return row["run_id"] if row else None

    def get_prices_for_run(self, run_id: str, provider: Optional[str] = None) -> List[dict]:
        """Return all price records for a given run."""
        with _conn(self.db_path) as con:
            if provider:
                rows = con.execute(
                    "SELECT * FROM price_snapshots WHERE run_id=? AND provider=?",
                    (run_id, provider),
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT * FROM price_snapshots WHERE run_id=?", (run_id,)
                ).fetchall()
        return [dict(r) for r in rows]

    def get_price_history(self, provider: str, sku_id: str, region_raw: str = "") -> List[dict]:
        """Return price history for a specific SKU across all runs."""
        with _conn(self.db_path) as con:
            if region_raw:
                rows = con.execute(
                    "SELECT ps.*, fr.started_at FROM price_snapshots ps "
                    "JOIN fetch_runs fr ON ps.run_id = fr.run_id "
                    "WHERE ps.provider=? AND ps.sku_id=? AND ps.region_raw=? "
                    "AND fr.status='completed' ORDER BY fr.started_at",
                    (provider, sku_id, region_raw),
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT ps.*, fr.started_at FROM price_snapshots ps "
                    "JOIN fetch_runs fr ON ps.run_id = fr.run_id "
                    "WHERE ps.provider=? AND ps.sku_id=? "
                    "AND fr.status='completed' ORDER BY fr.started_at",
                    (provider, sku_id),
                ).fetchall()
        return [dict(r) for r in rows]

    # ── Change detection ──────────────────────────────────────────────────────

    def detect_and_save_changes(self, run_id_new: str, run_id_old: Optional[str]) -> List[dict]:
        """
        Compare new run against old run. Persist detected changes.
        Returns list of change dicts.
        """
        if not run_id_old:
            return []  # First run — nothing to compare

        new_prices = {
            (r["provider"], r["sku_id"], r["region_raw"]): r
            for r in self.get_prices_for_run(run_id_new)
        }
        old_prices = {
            (r["provider"], r["sku_id"], r["region_raw"]): r
            for r in self.get_prices_for_run(run_id_old)
        }

        changes = []
        detected_at = datetime.now(timezone.utc).isoformat()

        # New or changed
        for key, new_rec in new_prices.items():
            old_rec = old_prices.get(key)
            if old_rec is None:
                # New SKU / region
                changes.append({
                    "detected_at": detected_at,
                    "run_id_old": run_id_old,
                    "run_id_new": run_id_new,
                    "provider": key[0],
                    "service": new_rec.get("service"),
                    "sku_id": key[1],
                    "sku_name": new_rec.get("sku_name"),
                    "region_canonical": new_rec.get("region_canonical"),
                    "region_raw": key[2],
                    "change_type": "new_sku",
                    "old_price_monthly": None,
                    "new_price_monthly": new_rec.get("price_monthly_usd"),
                    "old_price_per_gb": None,
                    "new_price_per_gb": new_rec.get("price_per_gb_usd"),
                    "pct_change_monthly": None,
                    "pct_change_per_gb": None,
                })
            else:
                # Check for price change
                old_m = old_rec.get("price_monthly_usd", 0) or 0
                new_m = new_rec.get("price_monthly_usd", 0) or 0
                old_g = old_rec.get("price_per_gb_usd", 0) or 0
                new_g = new_rec.get("price_per_gb_usd", 0) or 0

                pct_m = ((new_m - old_m) / old_m * 100) if old_m else None
                pct_g = ((new_g - old_g) / old_g * 100) if old_g else None

                changed = (
                    (pct_m is not None and abs(pct_m) >= 0.01) or
                    (pct_g is not None and abs(pct_g) >= 0.01)
                )
                if changed:
                    changes.append({
                        "detected_at": detected_at,
                        "run_id_old": run_id_old,
                        "run_id_new": run_id_new,
                        "provider": key[0],
                        "service": new_rec.get("service"),
                        "sku_id": key[1],
                        "sku_name": new_rec.get("sku_name"),
                        "region_canonical": new_rec.get("region_canonical"),
                        "region_raw": key[2],
                        "change_type": "price_change",
                        "old_price_monthly": old_m,
                        "new_price_monthly": new_m,
                        "old_price_per_gb": old_g,
                        "new_price_per_gb": new_g,
                        "pct_change_monthly": pct_m,
                        "pct_change_per_gb": pct_g,
                    })

        # Removed SKUs
        for key, old_rec in old_prices.items():
            if key not in new_prices:
                changes.append({
                    "detected_at": detected_at,
                    "run_id_old": run_id_old,
                    "run_id_new": run_id_new,
                    "provider": key[0],
                    "service": old_rec.get("service"),
                    "sku_id": key[1],
                    "sku_name": old_rec.get("sku_name"),
                    "region_canonical": old_rec.get("region_canonical"),
                    "region_raw": key[2],
                    "change_type": "removed_sku",
                    "old_price_monthly": old_rec.get("price_monthly_usd"),
                    "new_price_monthly": None,
                    "old_price_per_gb": old_rec.get("price_per_gb_usd"),
                    "new_price_per_gb": None,
                    "pct_change_monthly": None,
                    "pct_change_per_gb": None,
                })

        if changes:
            self._save_changes(changes)

        return changes

    def _save_changes(self, changes: List[dict]):
        with _conn(self.db_path) as con:
            con.executemany(
                """INSERT INTO price_changes(
                    detected_at, run_id_old, run_id_new, provider, service, sku_id,
                    sku_name, region_canonical, region_raw, change_type,
                    old_price_monthly, new_price_monthly, old_price_per_gb, new_price_per_gb,
                    pct_change_monthly, pct_change_per_gb, notified
                ) VALUES(:detected_at,:run_id_old,:run_id_new,:provider,:service,:sku_id,
                    :sku_name,:region_canonical,:region_raw,:change_type,
                    :old_price_monthly,:new_price_monthly,:old_price_per_gb,:new_price_per_gb,
                    :pct_change_monthly,:pct_change_per_gb,0)""",
                changes,
            )

    def get_unnotified_changes(self) -> List[dict]:
        with _conn(self.db_path) as con:
            rows = con.execute(
                "SELECT * FROM price_changes WHERE notified=0 ORDER BY detected_at"
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_changes_notified(self, change_ids: List[int]):
        with _conn(self.db_path) as con:
            con.executemany(
                "UPDATE price_changes SET notified=1 WHERE id=?",
                [(i,) for i in change_ids],
            )

    def get_recent_changes(self, limit: int = 50) -> List[dict]:
        with _conn(self.db_path) as con:
            rows = con.execute(
                "SELECT * FROM price_changes ORDER BY detected_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_runs(self, limit: int = 20) -> List[dict]:
        with _conn(self.db_path) as con:
            rows = con.execute(
                "SELECT * FROM fetch_runs ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]
