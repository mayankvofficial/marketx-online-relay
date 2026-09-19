import hashlib
import os
import time
import threading
from datetime import datetime, timezone

import requests
import socketio

TRADE99_URL = os.environ.get("MARKETX_TRADE99_URL", "https://trade99.live:3000")
SYMBOL = os.environ.get("MARKETX_SYMBOL", "CRUDEOIL26SEPFUT").strip()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY", "")
BATCH_SIZE = int(os.environ.get("MARKETX_BATCH_SIZE", "50"))
FLUSH_SECONDS = float(os.environ.get("MARKETX_FLUSH_SECONDS", "0.5"))

if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
    raise SystemExit("SUPABASE_URL and SUPABASE_SECRET_KEY are required")

TABLE_URL = f"{SUPABASE_URL}/rest/v1/market_snapshots"
session = requests.Session()
session.headers.update({
    "apikey": SUPABASE_SECRET_KEY,
    "Authorization": f"Bearer {SUPABASE_SECRET_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal,resolution=ignore-duplicates",
})

sio = socketio.Client(
    reconnection=True,
    reconnection_attempts=0,
    reconnection_delay=1,
    reconnection_delay_max=10,
)

pending = []
lock = threading.Lock()
last_flush = time.monotonic()


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
        "received_at": datetime.now(timezone.utc).isoformat(),
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
    global last_flush

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
        print(f"UPLOAD ERROR: {exc}; keeping {len(batch)} ticks queued", flush=True)
        return

    with lock:
        del pending[:len(batch)]

    last_flush = time.monotonic()
    print(f"MARKETX STORED: {len(batch)} ticks | pending={len(pending)}", flush=True)


@sio.event
def connect():
    print(f"MARKETX BRIDGE CONNECTED | {TRADE99_URL} | {SYMBOL}", flush=True)
    sio.emit("subscribe_symbol", {"room": SYMBOL})


@sio.on("symbol_subscribed")
def symbol_subscribed(data):
    print(f"MARKETX SUBSCRIBED: {data}", flush=True)


@sio.on("scrip_data")
def scrip_data(packet):
    data = packet.get("data", {}) if isinstance(packet, dict) else {}
    if data.get("Symbol") != SYMBOL:
        return

    with lock:
        pending.append(normalize_tick(data))
    flush()


@sio.event
def disconnect():
    print("MARKETX TRADE99 DISCONNECTED; reconnecting", flush=True)
    flush(force=True)


def main():
    print(f"MARKETX BRIDGE START | symbol={SYMBOL}", flush=True)

    while True:
        try:
            sio.connect(TRADE99_URL, transports=["websocket"], wait_timeout=20)

            while sio.connected:
                time.sleep(0.1)
                flush()

        except KeyboardInterrupt:
            break
        except Exception as exc:
            print(f"MARKETX CONNECTION ERROR: {exc}", flush=True)

        flush(force=True)
        time.sleep(2)


if __name__ == "__main__":
    main()
