#!/usr/bin/env python3
"""
MarketX Supabase -> local SQLite synchronizer.

Direction:
    public.market_snapshots -> local SQLite

The sync is incremental. It remembers the highest Supabase row id already
stored for each symbol, then fetches only newer rows. It is safe to restart
and does not require the Supabase secret/service_role key.

Environment:
    SUPABASE_URL
    SUPABASE_PUBLISHABLE_KEY

Optional:
    MARKETX_SYMBOL=CRUDEOIL26SEPFUT
    MARKETX_SYNC_DB=marketx_sync.db
    MARKETX_PAGE_SIZE=1000
    MARKETX_POLL_SECONDS=1
"""

import os
import sqlite3
import time
from datetime import datetime, timezone

import requests

SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    "https://ynluiynxwnuubxcmxude.supabase.co",
).rstrip("/")
SUPABASE_PUBLISHABLE_KEY = os.environ.get("SUPABASE_PUBLISHABLE_KEY", "").strip()
SYMBOL = os.environ.get("MARKETX_SYMBOL", "CRUDEOIL26SEPFUT").strip()
DB_PATH = os.environ.get("MARKETX_SYNC_DB", "marketx_sync.db")
PAGE_SIZE = int(os.environ.get("MARKETX_PAGE_SIZE", "1000"))
POLL_SECONDS = float(os.environ.get("MARKETX_POLL_SECONDS", "1"))

if not SUPABASE_PUBLISHABLE_KEY:
    raise SystemExit("SUPABASE_PUBLISHABLE_KEY is required")

REST_URL = f"{SUPABASE_URL}/rest/v1/market_snapshots"

session = requests.Session()
session.headers.update({
    "apikey": SUPABASE_PUBLISHABLE_KEY,
    "Accept": "application/json",
})

COLUMNS = [
    "id", "received_at", "symbol", "bid", "ask", "ltp", "oi",
    "bid_qty", "ask_qty", "volume", "ba_ratio", "created_at",
    "exchange_time", "open", "high", "low", "atp", "prev_close",
    "ltq", "active_clients", "tick_fingerprint",
]


def open_db():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")

    db.execute("""
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            received_at TEXT,
            bid REAL,
            ask REAL,
            ltp REAL,
            oi INTEGER,
            bid_qty INTEGER,
            ask_qty INTEGER,
            volume INTEGER,
            ba_ratio REAL,
            created_at TEXT,
            exchange_time TEXT,
            open REAL,
            high REAL,
            low REAL,
            atp REAL,
            prev_close REAL,
            ltq REAL,
            active_clients INTEGER,
            tick_fingerprint TEXT,
            synced_at TEXT NOT NULL,
            PRIMARY KEY (symbol, id)
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
            symbol TEXT PRIMARY KEY,
            last_id INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    """)

    db.commit()
    return db


def get_last_id(db):
    row = db.execute(
        "SELECT last_id FROM sync_state WHERE symbol = ?",
        (SYMBOL,),
    ).fetchone()
    return int(row[0]) if row else 0


def set_last_id(db, last_id):
    db.execute(
        """
        INSERT INTO sync_state(symbol, last_id, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            last_id = excluded.last_id,
            updated_at = excluded.updated_at
        """,
        (SYMBOL, last_id, datetime.now(timezone.utc).isoformat()),
    )


def fetch_page(last_id):
    params = {
        "select": ",".join(COLUMNS),
        "symbol": f"eq.{SYMBOL}",
        "id": f"gt.{last_id}",
        "order": "id.asc",
        "limit": str(PAGE_SIZE),
    }
    response = session.get(REST_URL, params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def store_page(db, rows):
    if not rows:
        return 0, None

    now = datetime.now(timezone.utc).isoformat()
    placeholders = ",".join("?" for _ in range(len(COLUMNS) + 1))
    sql = f"""
        INSERT OR IGNORE INTO market_snapshots
        ({",".join(COLUMNS)}, synced_at)
        VALUES ({placeholders})
    """

    values = []
    for row in rows:
        values.append(tuple(row.get(col) for col in COLUMNS) + (now,))

    db.executemany(sql, values)
    newest_id = max(int(row["id"]) for row in rows)
    set_last_id(db, newest_id)
    db.commit()

    stored = db.total_changes
    return stored, newest_id


def sync_once(db):
    total = 0

    while True:
        last_id = get_last_id(db)
        rows = fetch_page(last_id)

        if not rows:
            return total

        before = db.total_changes
        _, newest_id = store_page(db, rows)
        total += db.total_changes - before

        print(
            f"MARKETX SYNC | {SYMBOL} | "
            f"received={len(rows)} stored={db.total_changes - before} "
            f"last_id={newest_id}",
            flush=True,
        )

        if len(rows) < PAGE_SIZE:
            return total


def main():
    db = open_db()

    print(
        f"MARKETX SYNC START | symbol={SYMBOL} | "
        f"db={DB_PATH} | last_id={get_last_id(db)}",
        flush=True,
    )

    while True:
        try:
            sync_once(db)
        except KeyboardInterrupt:
            print("MARKETX SYNC STOPPED", flush=True)
            break
        except Exception as exc:
            print(f"MARKETX SYNC ERROR: {exc}", flush=True)
            time.sleep(3)
            continue

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
