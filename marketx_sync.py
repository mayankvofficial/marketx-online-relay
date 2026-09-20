#!/usr/bin/env python3
"""
MarketX Supabase -> local SQLite synchronizer.

Direction:
    public.market_snapshots -> local SQLite

The sync is incremental and verified. It remembers the highest Supabase row
id already stored for each symbol, fetches only newer rows, verifies tick
fingerprints, and checks checkpoint counts.

Environment:
    SUPABASE_URL
    SUPABASE_PUBLISHABLE_KEY

Optional:
    MARKETX_SYMBOL=CRUDEOIL26SEPFUT
    MARKETX_SYNC_DB=marketx_sync.db
    MARKETX_PAGE_SIZE=1000
    MARKETX_POLL_SECONDS=1
    MARKETX_VERIFY_SECONDS=30
    MARKETX_HEALTH_STALE_SECONDS=30
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
VERIFY_SECONDS = float(os.environ.get("MARKETX_VERIFY_SECONDS", "30"))
HEALTH_STALE_SECONDS = float(os.environ.get("MARKETX_HEALTH_STALE_SECONDS", "30"))

if not SUPABASE_PUBLISHABLE_KEY:
    raise SystemExit("SUPABASE_PUBLISHABLE_KEY is required")

REST_URL = f"{SUPABASE_URL}/rest/v1/market_snapshots"
HEALTH_URL = f"{SUPABASE_URL}/rest/v1/marketx_bridge_health"
GAPS_URL = f"{SUPABASE_URL}/rest/v1/marketx_capture_gaps"

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


def verify_rows(db, rows):
    if not rows:
        return 0
    ids = [int(row["id"]) for row in rows]
    placeholders = ",".join("?" for _ in ids)
    local_rows = db.execute(
        f"SELECT id, tick_fingerprint FROM market_snapshots "
        f"WHERE symbol = ? AND id IN ({placeholders})",
        (SYMBOL, *ids),
    ).fetchall()
    local = {int(row[0]): row[1] for row in local_rows}
    mismatches = []
    missing = []
    for row in rows:
        row_id = int(row["id"])
        if row_id not in local:
            missing.append(row_id)
        elif local[row_id] != row.get("tick_fingerprint"):
            mismatches.append(row_id)
    if missing or mismatches:
        raise RuntimeError(
            f"INTEGRITY FAILURE: missing={len(missing)} "
            f"fingerprint_mismatches={len(mismatches)} "
            f"first_missing={missing[:5]} first_mismatch={mismatches[:5]}"
        )
    return len(rows)


def remote_count_through(last_id):
    response = session.head(
        REST_URL,
        params={"select": "id", "symbol": f"eq.{SYMBOL}", "id": f"lte.{last_id}"},
        headers={"Prefer": "count=exact"},
        timeout=20,
    )
    response.raise_for_status()
    content_range = response.headers.get("Content-Range", "")
    if "/" not in content_range:
        raise RuntimeError("INTEGRITY FAILURE: Supabase did not return Content-Range")
    return int(content_range.rsplit("/", 1)[1])


def verify_checkpoint(db):
    last_id = get_last_id(db)
    if last_id <= 0:
        return {"status": "WAITING", "last_id": 0}
    remote_count = remote_count_through(last_id)
    local_count = db.execute(
        "SELECT COUNT(*) FROM market_snapshots WHERE symbol = ? AND id <= ?",
        (SYMBOL, last_id),
    ).fetchone()[0]
    if remote_count != local_count:
        raise RuntimeError(
            f"INTEGRITY FAILURE: checkpoint count remote={remote_count} "
            f"local={local_count} through_id={last_id}"
        )
    return {
        "status": "SYNC OK",
        "last_id": last_id,
        "remote_count": remote_count,
        "local_count": local_count,
    }


def sync_lag(db):
    response = session.get(
        REST_URL,
        params={
            "select": "id,received_at,tick_fingerprint",
            "symbol": f"eq.{SYMBOL}",
            "order": "id.desc",
            "limit": "1",
        },
        timeout=20,
    )
    response.raise_for_status()
    rows = response.json()
    if not rows:
        return {"status": "NO_REMOTE_DATA"}
    remote = rows[0]
    local = db.execute(
        "SELECT id, received_at, tick_fingerprint "
        "FROM market_snapshots WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (SYMBOL,),
    ).fetchone()
    if not local:
        return {"status": "LAGGING", "remote_id": int(remote["id"]), "local_id": 0}
    return {
        "status": "SYNC OK" if int(local[0]) == int(remote["id"]) and local[2] == remote.get("tick_fingerprint") else "LAGGING",
        "remote_id": int(remote["id"]),
        "local_id": int(local[0]),
        "id_gap": int(remote["id"]) - int(local[0]),
        "fingerprint_match": local[2] == remote.get("tick_fingerprint"),
    }


def bridge_health():
    response = session.get(
        HEALTH_URL,
        params={
            "select": "symbol,process_started_at,last_tick_received_at,last_upload_at,"
                      "last_error,pending_ticks,bridge_connected,updated_at",
            "symbol": f"eq.{SYMBOL}",
            "limit": "1",
        },
        timeout=10,
    )
    response.raise_for_status()
    rows = response.json()
    if not rows:
        return {"status": "NO_HEALTH_RECORD"}

    h = rows[0]
    now = datetime.now(timezone.utc)

    def age(value):
        if not value:
            return None
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return max(0.0, (now - dt).total_seconds())

    tick_age = age(h.get("last_tick_received_at"))
    update_age = age(h.get("updated_at"))

    if update_age is None or update_age > HEALTH_STALE_SECONDS:
        status = "BRIDGE DOWN / HEARTBEAT STALE"
    elif not h.get("bridge_connected"):
        status = "TRADE99 DISCONNECTED"
    elif tick_age is None:
        status = "WAITING FOR FIRST TICK"
    elif tick_age > HEALTH_STALE_SECONDS:
        status = "STALE / NO NEW TICKS"
    else:
        status = "LIVE"

    return {
        "status": status,
        "last_tick": h.get("last_tick_received_at"),
        "tick_age_sec": round(tick_age, 1) if tick_age is not None else None,
        "last_upload": h.get("last_upload_at"),
        "heartbeat_age_sec": round(update_age, 1) if update_age is not None else None,
        "pending": h.get("pending_ticks"),
        "error": h.get("last_error"),
    }


def latest_capture_gap():
    response = session.get(
        GAPS_URL,
        params={
            "select": "detected_at,gap_start,gap_end,gap_seconds,reason,resolved",
            "symbol": f"eq.{SYMBOL}",
            "order": "detected_at.desc",
            "limit": "1",
        },
        timeout=10,
    )
    response.raise_for_status()
    rows = response.json()
    return rows[0] if rows else None


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
    values = [
        tuple(row.get(col) for col in COLUMNS) + (now,)
        for row in rows
    ]
    before = db.total_changes
    db.executemany(sql, values)
    newest_id = max(int(row["id"]) for row in rows)
    set_last_id(db, newest_id)
    db.commit()
    return db.total_changes - before, newest_id


def sync_once(db):
    total = 0
    while True:
        last_id = get_last_id(db)
        rows = fetch_page(last_id)
        if not rows:
            return total

        stored, newest_id = store_page(db, rows)
        total += stored
        verify_rows(db, rows)

        print(
            f"MARKETX SYNC | {SYMBOL} | "
            f"received={len(rows)} stored={stored} last_id={newest_id} | EXACT",
            flush=True,
        )

        if len(rows) < PAGE_SIZE:
            return total


def main():
    db = open_db()
    last_verify = 0.0

    print(
        f"MARKETX SYNC START | symbol={SYMBOL} | "
        f"db={DB_PATH} | last_id={get_last_id(db)}",
        flush=True,
    )

    while True:
        try:
            sync_once(db)
            now = time.monotonic()
            if now - last_verify >= VERIFY_SECONDS:
                checkpoint = verify_checkpoint(db)
                lag = sync_lag(db)
                health = bridge_health()
                gap = latest_capture_gap()
                print(f"MARKETX INTEGRITY | {checkpoint} | SYNC={lag}", flush=True)
                print(
                    f"MARKETX CAPTURE GAP | {gap if gap else 'NONE DETECTED'}",
                    flush=True,
                )
                print(
                    f"MARKETX BRIDGE HEALTH | LAST TICK: {health.get('last_tick')} | "
                    f"TICK AGE: {health.get('tick_age_sec')} sec | "
                    f"STATUS: {health.get('status')} | "
                    f"LAST UPLOAD: {health.get('last_upload')} | "
                    f"HEARTBEAT AGE: {health.get('heartbeat_age_sec')} sec | "
                    f"PENDING: {health.get('pending')} | "
                    f"ERROR: {health.get('error')}",
                    flush=True,
                )
                last_verify = now
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
