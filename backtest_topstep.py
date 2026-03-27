"""
Quick backtest using TopstepX historical data.
Fetches bars via REST API, runs gen_sweep_entries + scoring.
"""
import os, sys, time
from datetime import datetime, timedelta, date as dt_date
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(__file__))
os.environ.setdefault("PYTHONPATH", os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))

from tsxapipy import authenticate, APIClient
from tsxapipy.api.contract_utils import get_futures_contract_details
from v106_dynamic_rr_zone_entry import (
    gen_sweep_entries, get_liquidity_levels,
    detect_sweep_at, cisd_5m, structure_15m, sweep_15m,
)

CT = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")

NQ_CONTRACT = "CON.F.US.ENQ.M26"
NQ_CONTRACT_PREV = "CON.F.US.ENQ.H26"

SLIP = 0.5
PV = 20
CONTRACTS = 3
MAX_RISK = 1000
COOLDOWN_S = 120       # 2 min cooldown after exit
MCL = 3                # max consecutive losses per side
GMCL = 5               # global max consecutive losses
DLL = -2000            # daily loss limit
FEES_RT = 8.40         # round-trip fees ($4.20/side for 3ct)


def fetch_bars(api, cid, tf_minutes, start_ct, end_ct):
    """Fetch bars with chunking, return backtest dict format."""
    all_bars = []
    s = start_ct
    chunk_days = 2 if tf_minutes <= 5 else 5

    while s < end_ct:
        chunk_end = min(s + timedelta(days=chunk_days), end_ct)
        for attempt in range(3):
            try:
                s_utc = s.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
                e_utc = chunk_end.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
                resp = api.get_historical_bars(
                    contract_id=cid, start_time_iso=s_utc, end_time_iso=e_utc,
                    unit=2, unit_number=tf_minutes, limit=10000, live=False,
                )
                if resp and resp.bars:
                    all_bars.extend(resp.bars)
                break
            except Exception as ex:
                if attempt == 2:
                    print(f"  WARN: chunk fetch failed: {ex}")
                else:
                    time.sleep(2)
        s = chunk_end
        if s < end_ct:
            time.sleep(0.5)

    seen = set()
    result = []
    for b in all_bars:
        try:
            t = b.t if hasattr(b.t, 'astimezone') else datetime.fromisoformat(str(b.t))
            t = t.astimezone(CT)
            ns = int(t.timestamp() * 1e9)
            if ns in seen:
                continue
            seen.add(ns)
            result.append({
                "time_ns": ns, "open": float(b.o), "high": float(b.h),
                "low": float(b.l), "close": float(b.c),
                "hour": t.hour, "minute": t.minute,
            })
        except Exception:
            continue
    result.sort(key=lambda x: x["time_ns"])
    return result


def fetch_with_rollover(api, tf_minutes, start_ct, end_ct):
    """Fetch from current + prior contract, merge at rollover boundary."""
    bars = fetch_bars(api, NQ_CONTRACT, tf_minutes, start_ct, end_ct)
    if NQ_CONTRACT_PREV:
        # Always fetch prior contract — current contract may not cover full date range
        prev_bars = fetch_bars(api, NQ_CONTRACT_PREV, tf_minutes, start_ct, end_ct)
        if prev_bars:
            if bars:
                first_new_ns = bars[0]["time_ns"]
                older = [b for b in prev_bars if b["time_ns"] < first_new_ns]
            else:
                older = prev_bars
            if older:
                bars = older + (bars or [])
                print(f"  Rollover backfill: {len(older)} {tf_minutes}m bars from prior contract")
    return bars


def build_dr(bars):
    """Build date range dict {date: (start_idx, end_idx)}."""
    dr = {}
    for i, b in enumerate(bars):
        t = datetime.fromtimestamp(b["time_ns"] / 1e9, tz=CT)
        d = t.date()
        if d not in dr:
            dr[d] = (i, i + 1)
        else:
            dr[d] = (dr[d][0], i + 1)
    return dr


def build_dr_htf(bars):
    """Build date range for 15m bars."""
    dr = {}
    for i, b in enumerate(bars):
        t = datetime.fromtimestamp(b["time_ns"] / 1e9, tz=CT)
        d = t.date()
        if d not in dr:
            dr[d] = (i, i + 1)
        else:
            dr[d] = (dr[d][0], i + 1)
    return dr


def main():
    print("=" * 60)
    print("BACKTEST — TopstepX Historical Data")
    print("=" * 60)

    # Authenticate
    token, token_time = authenticate()
    if not token:
        print("Auth failed!")
        sys.exit(1)
    api = APIClient(initial_token=token, token_acquired_at=token_time)
    print("Authenticated.\n")

    now = datetime.now(CT)
    # Single day debug — March 19 with 1m-beyond + zone entries
    # Start at 09:00 to simulate bot starting at 08:53 (misses pre-KZ signals)
    start = datetime(2026, 3, 19, 0, 0, 0, tzinfo=CT)
    end = datetime(2026, 3, 20, 0, 0, 0, tzinfo=CT)
    # Filter: only trade signals from 09:00+ (bot wasn't running before)
    SIM_START_HOUR = 9
    SIM_START_MIN = 0

    # Fetch bars — need 5+ days lookback for 5m/15m (bot loads multi-day cache)
    # Bot log shows: "Data coverage: last 5 dates in cache"
    lookback = start - timedelta(days=7)  # 7 calendar days ≈ 5 trading days

    print("Fetching 5m bars (with lookback)...")
    b5 = fetch_with_rollover(api, 5, lookback, end)
    print(f"  5m: {len(b5)} bars")

    print("Fetching 15m bars (with lookback)...")
    b15 = fetch_with_rollover(api, 15, lookback, end)
    print(f"  15m: {len(b15)} bars")

    print("Fetching 1m bars...")
    b1 = fetch_with_rollover(api, 1, lookback, end)
    print(f"  1m: {len(b1)} bars")

    # Build date ranges
    dr5 = build_dr(b5)
    dr15 = build_dr_htf(b15)
    all_dates = sorted(dr5.keys())
    # Only generate signals for target dates, not lookback period
    target_start = start.date()
    trade_dates = [d for d in all_dates if d >= target_start]

    print(f"\nDates in data: {[str(d) for d in all_dates]}")
    print(f"Trading dates: {[str(d) for d in trade_dates]}")
    print()

    # Run signal generation for each date — simulate live bar-by-bar
    # Process one 5m bar at a time, only using data available at each boundary.
    # This matches the bot: you can't see a 5m bar until it closes.
    all_sigs = []

    for d in trade_dates:
        if d not in dr5:
            continue
        ds5, de5 = dr5[d]
        liq = get_liquidity_levels(b5, dr5, d, all_dates)

        # Simulate live: step through each 5m bar boundary
        seen_ns = set()        # dedup by signal timestamp across all cursor steps
        # No zone dedup — see comment in signal loop

        b1_cutoff = 0
        for cursor in range(ds5 + 1, de5 + 1):
            # 1m-beyond ENABLED: extend b1 to NEXT 5m bar close so gen_sweep_entries
            # can find 1m touches beyond the current cursor (= the live bot behavior).
            # Bot scans on every 1m close — first closed 1m touching zone = entry.
            if cursor < de5:
                next_bar_ns = b5[cursor]["time_ns"] + 5 * 60_000_000_000
            else:
                next_bar_ns = b5[cursor - 1]["time_ns"] + 10 * 60_000_000_000
            while b1_cutoff < len(b1) and b1[b1_cutoff]["time_ns"] < next_bar_ns:
                b1_cutoff += 1
            ents = gen_sweep_entries(b5[:cursor + 1], b1[:b1_cutoff], ds5, cursor, d, liq)

            seen_bars = set()  # per-cursor bar dedup (same as bot's per-scan dedup)
            for e in sorted(ents, key=lambda x: (x["ns"], -{"ifvg": 2, "disp_fvg": 1}.get(x["zt"], 0))):
                if e["ns"] in seen_ns:
                    continue
                if e["bar_idx"] in seen_bars:
                    continue
                # Skip signals before bot start time
                _et = datetime.fromtimestamp(e["ns"] / 1e9, tz=CT)
                if _et.hour < SIM_START_HOUR or (_et.hour == SIM_START_HOUR and _et.minute < SIM_START_MIN):
                    continue
                # No zone dedup in backtest — bot only marks zones when it actually
                # executes a trade, which depends on position state we can't simulate here.
                seen_bars.add(e["bar_idx"])

                ep = e["ep"]
                sp = e["sp"]
                side = e["side"]
                if side == "bull":
                    ep += SLIP
                else:
                    ep -= SLIP
                risk_pts = abs(ep - sp)
                if risk_pts <= 0:
                    continue
                risk_dollars = risk_pts * PV * CONTRACTS

                # Score
                score = 1
                if e.get("rej"):
                    score += 1
                cisd = cisd_5m(b5, e["bar_idx"], ds5)
                if cisd == side:
                    score += 1
                sw_d, _, _ = detect_sweep_at(b5, e["bar_idx"], liq, lookback=8)
                if sw_d == side:
                    score += 2
                struct = structure_15m(b15, dr15, d, e["ns"])
                if struct == side:
                    score += 2
                sw15 = sweep_15m(b15, dr15, d, e["ns"], liq, side)
                if sw15:
                    score += 1

                # 4T-v3 confluence RR — matches bot (ptnut_bot.py lines 732-740)
                has_sweep = sw_d == side
                has_struct = struct == side
                has_cisd = cisd == side
                if has_sweep:
                    rr = 2.0
                elif has_struct:
                    rr = 1.7
                elif has_cisd:
                    rr = 1.5
                else:
                    rr = 1.3
                passes_risk = risk_dollars <= MAX_RISK

                # Entry time: when would the bot actually see this?
                cursor_bar = b5[cursor - 1] if cursor <= len(b5) else b5[-1]
                cursor_close_ns = cursor_bar["time_ns"] + 5 * 60_000_000_000
                # If signal ns is from a 1m bar beyond the previous 5m close,
                # bot finds it at the 1m scan (ns is already 1m bar close time).
                # Otherwise it's a 5m first-touch, found at cursor bar close.
                prev_close_ns = b5[cursor - 2]["time_ns"] + 5 * 60_000_000_000 if cursor > ds5 + 1 else cursor_bar["time_ns"]
                if e["ns"] >= prev_close_ns:
                    entry_time = datetime.fromtimestamp(e["ns"] / 1e9, tz=CT)
                else:
                    entry_time = datetime.fromtimestamp(cursor_close_ns / 1e9, tz=CT)

                seen_ns.add(e["ns"])
                all_sigs.append({
                    "date": d, "side": side, "entry": ep, "stop": sp,
                    "risk_pts": risk_pts, "risk_$": risk_dollars,
                    "score": score, "rr": rr, "zone": e["zt"],
                    "time": entry_time, "passes": passes_risk,
                    "hour": entry_time.hour, "minute": entry_time.minute,
                    "signal_time": datetime.fromtimestamp(e["ns"] / 1e9, tz=CT),
                    "zone_top": round(e.get("zone_top", 0), 2),
                    "zone_bot": round(e.get("zone_bot", 0), 2),
                })

                # (zone dedup removed — can't simulate bot's position state)

    # ── DUMP ALL RAW SIGNALS ──
    print("=" * 60)
    print(f"ALL RAW SIGNALS: {len(all_sigs)}")
    print("=" * 60)
    for s in sorted(all_sigs, key=lambda x: x["time"]):
        rf = "PASS" if s["passes"] else f"RISK ${s['risk_$']:.0f}"
        print(f"  {s['time'].strftime('%H:%M')} | {s['side'].upper():4s} @ {s['entry']:.2f} "
              f"| SL {s['stop']:.2f} | risk {s['risk_pts']:.1f}pt ${s['risk_$']:.0f} "
              f"| sc={s['score']} rr={s['rr']} | {s['zone']} | {rf} "
              f"| zone [{s['zone_top']:.2f}-{s['zone_bot']:.2f}]")

    # ── TRADE SIMULATION ──
    # Matches bot: risk filter, cooldown, zone dedup, MCL, GMCL, DLL, one trade at a time
    print("=" * 60)
    print(f"RAW SIGNALS: {len(all_sigs)}")
    print("=" * 60)

    # Sort all signals by time
    all_sigs.sort(key=lambda x: x["time"])

    trades = []
    in_position = False
    pos_exit_time = None   # datetime when current position exits
    cooldown_until = None  # datetime — no new trades until this time
    current_day = None
    cl_bull = 0            # consecutive losses — bull side
    cl_bear = 0            # consecutive losses — bear side
    gcl = 0                # global consecutive losses
    used_zones = set()     # {(side, zt, zone_top, zone_bot)}
    day_pnl = 0.0

    for s in all_sigs:
        if not s["passes"]:
            continue

        # Reset state at start of each day
        if s["date"] != current_day:
            current_day = s["date"]
            in_position = False
            pos_exit_time = None
            cooldown_until = None
            cl_bull = 0
            cl_bear = 0
            gcl = 0
            used_zones = set()
            day_pnl = 0.0

        entry_time = s["time"]

        # Can't enter if already in a position
        if in_position:
            if pos_exit_time and entry_time >= pos_exit_time:
                in_position = False
            else:
                continue

        # Cooldown — 120s after last exit (bot: COOLDOWN = 120)
        if cooldown_until and entry_time < cooldown_until:
            continue

        # Zone dedup — same zone bounds only traded once per day (bot lines 704-708)
        zone_key = (s["side"], s["zone"], s["zone_top"], s["zone_bot"])
        if zone_key in used_zones:
            continue

        # MCL — 3 consecutive losses per side (bot line 1858)
        if s["side"] == "bull" and cl_bull >= MCL:
            continue
        if s["side"] == "bear" and cl_bear >= MCL:
            continue

        # GMCL and DLL omitted from backtest — too path-dependent on
        # early trade ordering which differs between REST and live data

        # Calculate target
        risk_pts = s["risk_pts"]
        if s["side"] == "bull":
            target = s["entry"] + risk_pts * s["rr"]
        else:
            target = s["entry"] - risk_pts * s["rr"]

        # Walk 1m bars forward from entry to find SL or TP hit
        entry_ns = int(entry_time.timestamp() * 1e9)
        result = "OPEN"
        exit_time = None
        exit_price = None
        pnl = 0.0

        for bar in b1:
            if bar["time_ns"] < entry_ns:
                continue
            # Skip bar that OPENS at entry — entry happens at bar open,
            # so intra-bar action is post-entry. But for the entry bar itself,
            # the open IS our entry price, so only check the NEXT bar onward.
            # (entry bar's H/L may include pre-entry action from prior ticks)
            if bar["time_ns"] == entry_ns:
                continue
            # Check SL/TP hit on this 1m bar
            if s["side"] == "bull":
                if bar["low"] <= s["stop"]:
                    result = "LOSS"
                    exit_price = s["stop"]
                    exit_time = datetime.fromtimestamp(bar["time_ns"] / 1e9, tz=CT)
                    pnl = (exit_price - s["entry"]) * PV * CONTRACTS
                    break
                if bar["high"] >= target:
                    result = "WIN"
                    exit_price = target
                    exit_time = datetime.fromtimestamp(bar["time_ns"] / 1e9, tz=CT)
                    pnl = (exit_price - s["entry"]) * PV * CONTRACTS
                    break
            else:  # bear
                if bar["high"] >= s["stop"]:
                    result = "LOSS"
                    exit_price = s["stop"]
                    exit_time = datetime.fromtimestamp(bar["time_ns"] / 1e9, tz=CT)
                    pnl = (s["entry"] - exit_price) * PV * CONTRACTS
                    break
                if bar["low"] <= target:
                    result = "WIN"
                    exit_price = target
                    exit_time = datetime.fromtimestamp(bar["time_ns"] / 1e9, tz=CT)
                    pnl = (s["entry"] - exit_price) * PV * CONTRACTS
                    break

        # Deduct round-trip fees
        pnl -= FEES_RT

        # Mark zone as used
        used_zones.add(zone_key)

        # Track position state
        if result == "OPEN":
            pos_exit_time = datetime.now(CT)
            in_position = True
        else:
            pos_exit_time = exit_time
            in_position = True
            # Set cooldown (bot line 1861: COOLDOWN = 120s)
            cooldown_until = exit_time + timedelta(seconds=COOLDOWN_S)

        # Update MCL/GMCL counters (bot tracks consecutive losses per side)
        if result == "LOSS":
            if s["side"] == "bull":
                cl_bull += 1
            else:
                cl_bear += 1
            gcl += 1
        elif result == "WIN":
            if s["side"] == "bull":
                cl_bull = 0
            else:
                cl_bear = 0
            gcl = 0

        day_pnl += pnl

        trades.append({
            **s,
            "target": target,
            "result": result,
            "exit_time": exit_time,
            "exit_price": exit_price,
            "pnl": pnl,
        })

    # Print trade results
    print(f"\n{'=' * 60}")
    print(f"SIMULATED TRADES: {len(trades)}")
    print("=" * 60)

    total_pnl = 0.0
    wins = 0
    losses = 0
    daily_pnl = {}

    for t in trades:
        d = t["date"]
        if d not in daily_pnl:
            daily_pnl[d] = 0.0

        tag = t["result"]
        if tag == "WIN":
            wins += 1
        elif tag == "LOSS":
            losses += 1
        total_pnl += t["pnl"]
        daily_pnl[d] += t["pnl"]

        exit_str = f"exit {t['exit_time'].strftime('%H:%M')}" if t["exit_time"] else "OPEN"
        sig_str = t['signal_time'].strftime('%H:%M')
        print(f"  {t['date']} sig={sig_str} entry={t['time'].strftime('%H:%M')} | {t['side'].upper():4s} @ {t['entry']:.2f} "
              f"| SL {t['stop']:.2f} TP {t['target']:.2f} | risk {t['risk_pts']:.1f}pt ${t['risk_$']:.0f} "
              f"| sc={t['score']} rr={t['rr']} | {t['zone']} "
              f"| {tag} {exit_str} ${t['pnl']:+,.0f}")

    # Daily P&L summary
    print(f"\n{'=' * 60}")
    print("DAILY P&L")
    print("=" * 60)
    running = 0.0
    for d in sorted(daily_pnl.keys()):
        day_trades = [t for t in trades if t["date"] == d]
        day_wins = sum(1 for t in day_trades if t["result"] == "WIN")
        day_losses = sum(1 for t in day_trades if t["result"] == "LOSS")
        running += daily_pnl[d]
        print(f"  {d} | {len(day_trades)} trades ({day_wins}W {day_losses}L) "
              f"| ${daily_pnl[d]:+,.0f} | running: ${running:+,.0f}")

    # Overall summary
    print(f"\n{'=' * 60}")
    print(f"MTD SUMMARY")
    print(f"  Trades: {len(trades)} | Wins: {wins} | Losses: {losses} | "
          f"Win rate: {wins/(wins+losses)*100:.0f}%" if wins + losses > 0 else "")
    print(f"  Total P&L: ${total_pnl:+,.0f}")
    print(f"  Avg win: ${sum(t['pnl'] for t in trades if t['result']=='WIN')/max(wins,1):+,.0f} "
          f"| Avg loss: ${sum(t['pnl'] for t in trades if t['result']=='LOSS')/max(losses,1):+,.0f}")
    print("=" * 60)


if __name__ == "__main__":
    main()
