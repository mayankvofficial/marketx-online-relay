import hashlib
import json
import tempfile
import os
import time
import threading
from datetime import datetime, timezone

import requests
import socketio

TRADE99_URL = os.environ.get("MARKETX_TRADE99_URL", "https://trade99.live:3000")
SYMBOL = os.environ.get("MARKETX_SYMBOL", "CRUDEOIL26SEPFUT").strip()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY", "").strip()
BATCH_SIZE = int(os.environ.get("MARKETX_BATCH_SIZE", "50"))
FLUSH_SECONDS = float(os.environ.get("MARKETX_FLUSH_SECONDS", "0.5"))
HEALTH_SECONDS = float(os.environ.get("MARKETX_HEALTH_SECONDS", "10"))
GAP_SECONDS = float(os.environ.get("MARKETX_GAP_SECONDS", "30"))

if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
    raise SystemExit("SUPABASE_URL and SUPABASE_SECRET_KEY are required")

TABLE_URL = f"{SUPABASE_URL}/rest/v1/market_snapshots"
HEALTH_URL = f"{SUPABASE_URL}/rest/v1/marketx_bridge_health"
CAPTURE_GAPS_URL = f"{SUPABASE_URL}/rest/v1/marketx_capture_gaps"

session = requests.Session()
headers = {
    "apikey": SUPABASE_SECRET_KEY,
    "Content-Type": "application/json",
    "Prefer": "return=minimal,resolution=ignore-duplicates",
}
# New sb_secret_* keys are API keys, not JWTs, so do not send them as Bearer tokens.
# Legacy service_role JWTs still receive the Authorization header.
if not SUPABASE_SECRET_KEY.startswith("sb_secret_"):
    headers["Authorization"] = f"Bearer {SUPABASE_SECRET_KEY}"
session.headers.update(headers)

sio = socketio.Client(
    reconnection=False,
)

PENDING_FILE = os.environ.get("MARKETX_PENDING_FILE", "marketx_pending_ticks.jsonl")
pending = []
lock = threading.Lock()
last_flush = time.monotonic()
health_lock = threading.Lock()
process_started_at = datetime.now(timezone.utc)
last_tick_received_at = None
received_tick_count = 0
stored_tick_count = 0
last_upload_at = None
last_error = None
bridge_connected = False
previous_health = None
previous_health_loaded = False
last_gap_checked_tick = None


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def save_pending():
    # Socket.IO can dispatch multiple scrip_data callbacks concurrently.
    # Use a unique temporary file so concurrent persistence calls cannot
    # replace/delete each other's temp file.
    with lock:
        rows = list(pending)
        directory = os.path.dirname(os.path.abspath(PENDING_FILE)) or "."
        fd, tmp = tempfile.mkstemp(
            prefix=".marketx_pending_",
            suffix=".tmp",
            dir=directory,
            text=True,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row, separators=(",", ":")) + "\n")
            os.replace(tmp, PENDING_FILE)
        finally:
            if os.path.exists(tmp):
                try:
                    os.unlink(tmp)
                except OSError:
                    pass


def load_pending():
    if not os.path.exists(PENDING_FILE):
        return
    loaded = []
    try:
        with open(PENDING_FILE, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    loaded.append(json.loads(line))
        with lock:
            pending.extend(loaded)
        print(f"MARKETX PENDING RECOVERED: {len(loaded)} ticks", flush=True)
    except Exception as exc:
        print(f"MARKETX PENDING RECOVERY ERROR: {exc}", flush=True)



def set_error(message):
    global last_error
    with health_lock:
        last_error = str(message)[-1000:]


def normalize_tick(d):
    values = [
        d.get("Exchange_Time"), d.get("Symbol"), d.get("Bid"), d.get("Ask"),
        d.get("LTP"), d.get("OI"), d.get("Bid_Qty"), d.get("Ask_Qty"),
        d.get("Volume"), d.get("LTQ"), d.get("Open"), d.get("High"),
        d.get("Low"), d.get("ATP"), d.get("Prev_Close"), d.get("active_clients"),
    ]
    fingerprint = hashlib.sha256("|".join(map(str, values)).encode()).hexdigest()

    return {
        "tick_fingerprint": fingerprint,
        "received_at": now_iso(),
        "exchange_time": d.get("Exchange_Time"),
        "symbol": d.get("Symbol"),
        "bid": d.get("Bid"),
        "ask": d.get("Ask"),
        "ltp": d.get("LTP"),
        "open": d.get("Open"),
        "high": d.get("High"),
        "low": d.get("Low"),
        "atp": d.get("ATP"),
        "prev_close": d.get("Prev_Close"),
        "oi": d.get("OI"),
        "bid_qty": d.get("Bid_Qty"),
        "ask_qty": d.get("Ask_Qty"),
        "volume": d.get("Volume"),
        "ltq": d.get("LTQ"),
        "active_clients": d.get("active_clients"),
    }


def flush(force=False):
    global last_flush, last_upload_at, stored_tick_count

    with lock:
        if not pending:
            return
        if not force and len(pending) < BATCH_SIZE and time.monotonic() - last_flush < FLUSH_SECONDS:
            return
        batch = pending[:]

    try:
        response = session.post(TABLE_URL, json=batch, timeout=15)
        response.raise_for_status()
    except Exception as exc:
        set_error(f"UPLOAD ERROR: {exc}")
        print(f"UPLOAD ERROR: {exc}; keeping {len(batch)} ticks queued", flush=True)
        return

    with lock:
        del pending[:len(batch)]
    save_pending()

    last_flush = time.monotonic()
    with health_lock:
        last_upload_at = datetime.now(timezone.utc)
        last_error = None

    stored_tick_count += len(batch)
    if len(batch) == 1:
        stored_at = batch[0].get("Exchange_Time") or batch[0].get("exchange_time") or batch[0].get("received_at") or now_iso()
        print(
            f"MARKETX STORED | tick_time={stored_at} | total_stored={stored_tick_count} | pending={len(pending)}",
            flush=True,
        )
    else:
        times = [
            row.get("Exchange_Time") or row.get("exchange_time") or row.get("received_at") or now_iso()
            for row in batch
        ]
        print(
            f"MARKETX STORED: {len(batch)} ticks | tick_times={times[0]} -> {times[-1]} "
            f"| total_stored={stored_tick_count} | pending={len(pending)}",
            flush=True,
        )


def health_snapshot():
    with lock:
        pending_count = len(pending)
    with health_lock:
        return {
            "symbol": SYMBOL,
            "process_started_at": process_started_at.isoformat(),
            "last_tick_received_at": last_tick_received_at.isoformat() if last_tick_received_at else None,
            "last_upload_at": last_upload_at.isoformat() if last_upload_at else None,
            "last_error": last_error,
            "pending_ticks": pending_count,
            "bridge_connected": bridge_connected,
            "updated_at": now_iso(),
        }


def load_previous_health():
    global previous_health, previous_health_loaded
    try:
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
        previous_health = rows[0] if rows else None
    except Exception as exc:
        print(f"HEALTH READ ERROR: {exc}", flush=True)
    previous_health_loaded = True


def record_capture_gap(first_tick_time):
    global last_gap_checked_tick
    if not previous_health or not previous_health.get("last_tick_received_at"):
        return
    if last_gap_checked_tick == previous_health.get("last_tick_received_at"):
        return

    previous_tick = datetime.fromisoformat(
        previous_health["last_tick_received_at"].replace("Z", "+00:00")
    )
    gap_seconds = (first_tick_time - previous_tick).total_seconds()

    if gap_seconds <= GAP_SECONDS:
        last_gap_checked_tick = previous_health["last_tick_received_at"]
        return

    payload = {
        "symbol": SYMBOL,
        "gap_start": previous_health["last_tick_received_at"],
        "gap_end": first_tick_time.isoformat(),
        "gap_seconds": gap_seconds,
        "reason": "PROCESS_RESTART_OR_CAPTURE_INTERRUPTION",
        "bridge_was_connected": previous_health.get("bridge_connected"),
        "process_started_at": previous_health.get("process_started_at"),
        "last_tick_received_at": previous_health.get("last_tick_received_at"),
        "first_tick_after_gap_at": first_tick_time.isoformat(),
        "resolved": True,
    }

    try:
        response = session.post(
            CAPTURE_GAPS_URL,
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        print(
            f"MARKETX CAPTURE GAP | {gap_seconds:.1f}s | "
            f"{previous_health['last_tick_received_at']} -> {first_tick_time.isoformat()}",
            flush=True,
        )
        last_gap_checked_tick = previous_health["last_tick_received_at"]
    except Exception as exc:
        print(f"CAPTURE GAP LOG ERROR: {exc}", flush=True)


def publish_health():
    payload = health_snapshot()
    try:
        response = session.post(
            HEALTH_URL,
            params={"on_conflict": "symbol"},
            json=payload,
            headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
            timeout=10,
        )
        response.raise_for_status()
    except Exception as exc:
        print(f"HEALTH UPDATE ERROR: {exc}", flush=True)


def health_loop():
    while True:
        publish_health()
        time.sleep(HEALTH_SECONDS)


@sio.event
def connect():
    global bridge_connected
    with health_lock:
        bridge_connected = True
        last_error = None
    print(f"MARKETX BRIDGE CONNECTED | {TRADE99_URL} | {SYMBOL}", flush=True)
    sio.emit("subscribe_symbol", {"room": SYMBOL})


@sio.on("symbol_subscribed")
def symbol_subscribed(data):
    print(f"MARKETX SUBSCRIBED: {data}", flush=True)


@sio.on("scrip_data")
def scrip_data(packet):
    global last_tick_received_at, received_tick_count
    global previous_health_loaded
    data = packet.get("data", {}) if isinstance(packet, dict) else {}
    if data.get("Symbol") != SYMBOL:
        return

    tick_time = datetime.now(timezone.utc)
    received_tick_count += 1
    if received_tick_count == 1 or received_tick_count % 25 == 0:
        print(f"MARKETX RECEIVED | total_received={received_tick_count}", flush=True)

    if not previous_health_loaded:
        load_previous_health()
    record_capture_gap(tick_time)

    with health_lock:
        last_tick_received_at = tick_time

    tick = normalize_tick(data)
    with lock:
        pending.append(tick)
        with open(PENDING_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(tick, separators=(",", ":")) + "\n")
    flush()


@sio.event
def disconnect():
    global bridge_connected
    with health_lock:
        bridge_connected = False
    set_error("TRADE99 DISCONNECTED")
    print("MARKETX TRADE99 DISCONNECTED; reconnecting", flush=True)
    flush(force=True)


def main():
    key_type = "sb_secret" if SUPABASE_SECRET_KEY.startswith("sb_secret_") else ("legacy_or_other" if SUPABASE_SECRET_KEY else "missing")
    print(f"MARKETX BRIDGE START | symbol={SYMBOL} | SUPABASE KEY TYPE={key_type} | KEY LENGTH={len(SUPABASE_SECRET_KEY)}", flush=True)
    load_pending()
    load_previous_health()
    threading.Thread(target=health_loop, daemon=True).start()

    while True:
        try:
            # Prevent a reconnect race: socketio.Client.connect() raises
            # "Already connected" if another connection attempt won the race.
            if sio.connected:
                while sio.connected:
                    time.sleep(0.1)
                    flush()
                continue

            sio.connect(TRADE99_URL, transports=["websocket"], wait_timeout=20)

            while sio.connected:
                time.sleep(0.1)
                flush()

        except KeyboardInterrupt:
            break
        except Exception as exc:
            # If the connection became active during a race, keep using it
            # instead of treating "Already connected" as a real failure.
            if sio.connected:
                continue
            set_error(f"CONNECTION ERROR: {exc}")
            print(f"MARKETX CONNECTION ERROR: {exc}", flush=True)

        flush(force=True)
        time.sleep(2)


if __name__ == "__main__":
    main()
