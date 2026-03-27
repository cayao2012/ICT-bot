"""
Test: Do WS-built 1m bars match REST 1m bars?
Connects to WebSocket, builds bars the same way ptnut_bot does,
then after N minutes compares against REST bars.
"""
import time
import logging
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from tsxapipy import authenticate, APIClient, DataStream

CT = ZoneInfo("America/Chicago")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("bar_test")

NQ_CONTRACT = "CON.F.US.ENQ.M26"

# ── WS bar builder (exact copy of ptnut_bot.on_tick) ──
ws_bars = []
current_bar = None
bar_minute = -1

def on_tick(price, ct_now):
    global current_bar, bar_minute, ws_bars
    cur_min = ct_now.hour * 60 + ct_now.minute
    bar_closed = False

    if cur_min != bar_minute:
        if current_bar is not None:
            ws_bars.append(current_bar)
            bar_closed = True
        bar_minute = cur_min
        t_ns = int(ct_now.replace(second=0, microsecond=0).timestamp() * 1e9)
        current_bar = {
            "time_ns": t_ns,
            "open": price, "high": price,
            "low": price, "close": price,
            "hour": ct_now.hour, "minute": ct_now.minute,
        }
    else:
        if current_bar is not None:
            if price > current_bar["high"]:
                current_bar["high"] = price
            if price < current_bar["low"]:
                current_bar["low"] = price
            current_bar["close"] = price

    return bar_closed

# ── Quote handler ──
tick_count = 0
def on_quote(quote):
    global tick_count
    price = quote.get("lastPrice") or quote.get("LastPrice")
    if not price:
        return
    price = float(price)
    tick_count += 1

    # Use server timestamp (same as bot)
    now = None
    ts = quote.get("lastUpdated") or quote.get("LastUpdated")
    if ts:
        try:
            if isinstance(ts, str):
                now = datetime.fromisoformat(ts.rstrip("Z")).replace(tzinfo=ZoneInfo("UTC")).astimezone(CT)
            elif isinstance(ts, (int, float)):
                if ts > 1e12:
                    now = datetime.fromtimestamp(ts / 1000, tz=CT)
                else:
                    now = datetime.fromtimestamp(ts, tz=CT)
        except Exception:
            pass
    if now is None:
        now = datetime.now(CT)

    closed = on_tick(price, now)
    if closed and tick_count <= 5000:
        b = ws_bars[-1]
        log.info(f"  WS bar closed: {b['hour']:02d}:{b['minute']:02d} O={b['open']:.2f} H={b['high']:.2f} L={b['low']:.2f} C={b['close']:.2f}")

    # Also log raw timestamp for first few ticks to see format
    if tick_count <= 3:
        raw_ts = quote.get("lastUpdated") or quote.get("LastUpdated")
        log.info(f"  Raw quote tick #{tick_count}: price={price} lastUpdated={raw_ts} type={type(raw_ts).__name__} parsed_ct={now}")

def on_trade(trade):
    pass  # not used for bar building

# ── Main ──
log.info("Authenticating...")
token, token_time = authenticate()
if not token:
    log.error("Auth failed!")
    sys.exit(1)

api = APIClient(initial_token=token, token_acquired_at=token_time)
log.info(f"Connected. Contract: {NQ_CONTRACT}")

# Record start time
start_time = datetime.now(CT)
log.info(f"Start time (CT): {start_time.strftime('%H:%M:%S')}")
log.info(f"Collecting WS bars for 5 minutes, then comparing to REST...")

# Connect WebSocket
ds = DataStream(
    api_client=api,
    contract_id_to_subscribe=NQ_CONTRACT,
    on_quote_callback=on_quote,
    on_trade_callback=on_trade,
    auto_subscribe_quotes=True,
    auto_subscribe_trades=False,
)
ds.start()

# Wait 5 minutes to collect bars
WAIT_MINUTES = 5
log.info(f"Waiting {WAIT_MINUTES} minutes for bars to accumulate...")
time.sleep(WAIT_MINUTES * 60 + 10)  # extra 10s buffer for last bar to close

# Stop WebSocket
ds.stop()
log.info(f"WebSocket stopped. Got {len(ws_bars)} WS bars, {tick_count} ticks total.")

# Fetch REST bars for same period
end_time = datetime.now(CT)
UTC = ZoneInfo("UTC")
s_utc = start_time.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
e_utc = end_time.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")

log.info(f"Fetching REST 1m bars: {s_utc} → {e_utc}")
resp = api.get_historical_bars(
    contract_id=NQ_CONTRACT,
    start_time_iso=s_utc,
    end_time_iso=e_utc,
    unit=2, unit_number=1, limit=100, live=False,
)

rest_bars = []
if resp and resp.bars:
    for b in resp.bars:
        t = b.t if hasattr(b.t, 'astimezone') else datetime.fromisoformat(str(b.t))
        t = t.astimezone(CT)
        ns = int(t.timestamp() * 1e9)
        rest_bars.append({
            "time_ns": ns,
            "open": float(b.o), "high": float(b.h),
            "low": float(b.l), "close": float(b.c),
            "hour": t.hour, "minute": t.minute,
        })

log.info(f"REST returned {len(rest_bars)} bars")

# ── Compare ──
print("\n" + "="*90)
print(f"{'Time':>6} | {'':^40} | {'':^40}")
print(f"{'':>6} | {'WS Bar':^40} | {'REST Bar':^40}")
print(f"{'':>6} | {'O':>10} {'H':>10} {'L':>10} {'C':>10} | {'O':>10} {'H':>10} {'L':>10} {'C':>10}")
print("="*90)

# Index REST bars by time_ns
rest_by_ns = {b["time_ns"]: b for b in rest_bars}
ws_by_ns = {b["time_ns"]: b for b in ws_bars}

all_ns = sorted(set(list(rest_by_ns.keys()) + list(ws_by_ns.keys())))

mismatches = 0
for ns in all_ns:
    ws = ws_by_ns.get(ns)
    rest = rest_by_ns.get(ns)

    if ws and rest:
        t_str = f"{ws['hour']:02d}:{ws['minute']:02d}"
        match = (
            abs(ws["open"] - rest["open"]) < 0.01 and
            abs(ws["high"] - rest["high"]) < 0.01 and
            abs(ws["low"] - rest["low"]) < 0.01 and
            abs(ws["close"] - rest["close"]) < 0.01
        )
        flag = "  OK" if match else " *** MISMATCH ***"
        if not match:
            mismatches += 1
        print(f"{t_str:>6} | {ws['open']:10.2f} {ws['high']:10.2f} {ws['low']:10.2f} {ws['close']:10.2f} | {rest['open']:10.2f} {rest['high']:10.2f} {rest['low']:10.2f} {rest['close']:10.2f} {flag}")
        if not match:
            print(f"       | diff: O={ws['open']-rest['open']:+.2f} H={ws['high']-rest['high']:+.2f} L={ws['low']-rest['low']:+.2f} C={ws['close']-rest['close']:+.2f}")
    elif ws:
        t_str = f"{ws['hour']:02d}:{ws['minute']:02d}"
        print(f"{t_str:>6} | {ws['open']:10.2f} {ws['high']:10.2f} {ws['low']:10.2f} {ws['close']:10.2f} | {'--- NO REST BAR ---':>40}")
    elif rest:
        t_str = f"{rest['hour']:02d}:{rest['minute']:02d}"
        print(f"{t_str:>6} | {'--- NO WS BAR ---':>40} | {rest['open']:10.2f} {rest['high']:10.2f} {rest['low']:10.2f} {rest['close']:10.2f}")

print("="*90)
print(f"\nTotal: {len(ws_bars)} WS bars, {len(rest_bars)} REST bars")
print(f"Matched: {len(all_ns) - mismatches} | Mismatches: {mismatches}")
if mismatches == 0:
    print("PERFECT MATCH — WS bars identical to REST bars")
else:
    print(f"WARNING: {mismatches} bars differ between WS and REST!")
