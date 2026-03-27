"""Quick test: what if entries were at the zone boundary instead of 1m close?"""
import os, sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))

from tsxapipy import authenticate, APIClient
from backtest_topstep import fetch_with_rollover, build_dr

CT = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")
PV = 20
CONTRACTS = 3

# The losing trades from the 1m-beyond backtest on March 19
# For each one, test: if we entered at zone boundary + 1pt, would it win?
trades = [
    {"time": "08:28", "side": "bull", "ep_actual": 24424.75, "stop": 24411.75, "rr": 2.0,
     "zone_top": 24412.75, "zone_bot": 24407.50},
    {"time": "09:57", "side": "bear", "ep_actual": 24491.00, "stop": 24501.00, "rr": 2.0,
     "zone_top": 24524.50, "zone_bot": 24497.50},
    {"time": "11:26", "side": "bear", "ep_actual": 24448.50, "stop": 24461.50, "rr": 2.0,
     "zone_top": 24462.75, "zone_bot": 24458.25},
    {"time": "11:31", "side": "bear", "ep_actual": 24478.25, "stop": 24480.50, "rr": 2.0,
     "zone_top": 24485.25, "zone_bot": 24475.75},  # this was the WIN
    {"time": "12:05", "side": "bull", "ep_actual": 24422.75, "stop": 24420.75, "rr": 2.0,
     "zone_top": 24421.75, "zone_bot": 24404.25},
    {"time": "14:06", "side": "bear", "ep_actual": 24613.50, "stop": 24619.50, "rr": 1.3,
     "zone_top": 24624.00, "zone_bot": 24614.50},
]

# Fetch 1m bars for March 19
token, _ = authenticate()
api = APIClient(initial_token=token, token_acquired_at=_)
start = datetime(2026, 3, 19, 0, 0, 0, tzinfo=CT)
end = datetime(2026, 3, 20, 0, 0, 0, tzinfo=CT)
b1 = fetch_with_rollover(api, 1, start - timedelta(days=1), end)
print(f"1m bars: {len(b1)}")

FEES = 8.40

for t in trades:
    # Hypothetical entry at zone boundary + 1pt
    if t["side"] == "bull":
        ep_zone = t["zone_top"] + 1.0  # enter 1pt above zone top
    else:
        ep_zone = t["zone_bot"] - 1.0  # enter 1pt below zone bot

    # Original entry
    ep_orig = t["ep_actual"]
    sp = t["stop"]
    rr = t["rr"]

    for label, ep in [("ACTUAL (1m close)", ep_orig), ("ZONE +1pt", ep_zone)]:
        if t["side"] == "bull":
            risk = ep - sp
            target = ep + risk * rr
        else:
            risk = sp - ep
            target = ep - risk * rr

        if risk <= 0:
            print(f"  {t['time']} {label}: SKIP (risk <= 0)")
            continue

        # Parse entry time
        h, m = map(int, t["time"].split(":"))
        entry_dt = datetime(2026, 3, 19, h, m, 0, tzinfo=CT)
        entry_ns = int(entry_dt.timestamp() * 1e9)

        # Walk 1m bars forward
        result = "OPEN"
        for bar in b1:
            if bar["time_ns"] <= entry_ns:
                continue
            if t["side"] == "bull":
                if bar["low"] <= sp:
                    result = "LOSS"
                    pnl = (sp - ep) * PV * CONTRACTS - FEES
                    break
                if bar["high"] >= target:
                    result = "WIN"
                    pnl = (target - ep) * PV * CONTRACTS - FEES
                    break
            else:
                if bar["high"] >= sp:
                    result = "LOSS"
                    pnl = (ep - sp) * PV * CONTRACTS - FEES
                    break
                if bar["low"] <= target:
                    result = "WIN"
                    pnl = (ep - target) * PV * CONTRACTS - FEES
                    break

        if result == "OPEN":
            pnl = 0

        dist = abs(ep - (t["zone_top"] if t["side"] == "bull" else t["zone_bot"]))
        print(f"  {t['time']} {t['side'].upper():4s} | {label:20s} ep={ep:10.2f} sl={sp:.2f} tp={target:.2f} "
              f"risk={risk:.1f}pt | dist_from_zone={dist:.1f}pt | {result} ${pnl:+,.0f}")
    print()
