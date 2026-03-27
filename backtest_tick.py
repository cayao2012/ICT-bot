#!/usr/bin/env python3
"""
Tick-level backtest — V106 ICT Strategy
=========================================
Signal generation:  TopstepX API (5m/1m/15m bars)
Trade simulation:   Databento NQ tick data (nanosecond resolution)

Compares:
  Mode 2 (1m Close entry) vs Mode 3 (Zone+Confirm entry)
  1m bar simulation vs tick-by-tick simulation

Key question answered:
  "With tick-level precision, does Mode 3 still beat Mode 2?"
  "Are there trades where the 1m bar gave the WRONG result?"
"""
import os, sys, time, json
import glob as globmod
import numpy as np
from datetime import datetime, timedelta, date as dt_date
from zoneinfo import ZoneInfo
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))

CT = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")

TICK_DIR = "/Users/tradingbot/Library/Mobile Documents/com~apple~CloudDocs/GLBX-20260320-7WNLNAUVDQ"
PV = 20
CONTRACTS = 3
FEES_RT = 8.40
MAX_RISK = 1000
COOLDOWN_S = 120
MCL = 3
SLIP = 0.5
SIM_START_HOUR = 7
SIM_START_MIN = 30
BAR_CACHE_DIR = os.path.join(os.path.dirname(__file__), ".bar_cache")


# ═══════════════════════════════════════════════════════════════
# BAR CACHE — fetch once, reuse forever
# ═══════════════════════════════════════════════════════════════
def _cache_path(tf, start_date, end_date):
    os.makedirs(BAR_CACHE_DIR, exist_ok=True)
    return os.path.join(BAR_CACHE_DIR,
                        f"bars_{tf}m_{start_date}_{end_date}.json")


def save_bar_cache(bars, tf, start_date, end_date):
    path = _cache_path(tf, start_date, end_date)
    with open(path, "w") as f:
        json.dump(bars, f)
    print(f"  Cached {len(bars)} {tf}m bars → {os.path.basename(path)}")


def load_bar_cache(tf, start_date, end_date):
    path = _cache_path(tf, start_date, end_date)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        bars = json.load(f)
    print(f"  Cache hit: {len(bars)} {tf}m bars ← {os.path.basename(path)}")
    return bars


def fetch_or_cache(api, tf, lookback, end, start_date, end_date):
    from backtest_topstep import fetch_with_rollover
    cached = load_bar_cache(tf, start_date, end_date)
    if cached:
        return cached
    print(f"  Fetching {tf}m bars from API...")
    bars = fetch_with_rollover(api, tf, lookback, end)
    save_bar_cache(bars, tf, start_date, end_date)
    return bars


# ═══════════════════════════════════════════════════════════════
# TICK DATA LOADING
# ═══════════════════════════════════════════════════════════════
def load_ticks(dates):
    """Load Databento tick data for given dates.
    Returns (prices_array, timestamps_ns_array, loaded_dates)."""
    import databento as db

    all_prices = []
    all_ts = []
    loaded = []

    for d in sorted(dates):
        fname = f"glbx-mdp3-{d.strftime('%Y%m%d')}.trades.dbn.zst"
        fpath = os.path.join(TICK_DIR, fname)
        if not os.path.exists(fpath):
            print(f"    {d}: NO FILE")
            continue

        store = db.DBNStore.from_file(fpath)
        df = store.to_df()

        # Filter to actual NQ contracts (not spreads like NQH6-NQM6)
        nq_syms = [s for s in df['symbol'].unique()
                    if s.startswith('NQ') and '-' not in s and len(s) == 4]
        if not nq_syms:
            continue

        # Use highest-volume contract
        best = max(nq_syms, key=lambda s: len(df[df['symbol'] == s]))
        sub = df[df['symbol'] == best]

        prices = sub['price'].values.astype(np.float64)
        ts_ns = sub.index.view(np.int64)

        all_prices.append(prices)
        all_ts.append(ts_ns)
        loaded.append(d)
        print(f"    {d} {best}: {len(prices):>8,} ticks  "
              f"[{prices.min():.2f} – {prices.max():.2f}]")

    if not all_prices:
        return None, None, []

    return np.concatenate(all_prices), np.concatenate(all_ts), loaded


# ═══════════════════════════════════════════════════════════════
# SINGLE-TRADE OUTCOME — 1m bars vs ticks
# ═══════════════════════════════════════════════════════════════
def outcome_1m(sig, b1):
    """Check WIN/LOSS for one signal using 1m bars. No state."""
    entry_ns = int(sig["time"].timestamp() * 1e9)
    side = sig["side"]
    ep, sp = sig["entry"], sig["stop"]
    risk = sig["risk_pts"]
    tp = ep + risk * sig["rr"] if side == "bull" else ep - risk * sig["rr"]

    for bar in b1:
        if bar["time_ns"] <= entry_ns:
            continue
        if side == "bull":
            if bar["low"] <= sp:
                return "LOSS", (sp - ep) * PV * CONTRACTS - FEES_RT, tp
            if bar["high"] >= tp:
                return "WIN", (tp - ep) * PV * CONTRACTS - FEES_RT, tp
        else:
            if bar["high"] >= sp:
                return "LOSS", (ep - sp) * PV * CONTRACTS - FEES_RT, tp
            if bar["low"] <= tp:
                return "WIN", (ep - tp) * PV * CONTRACTS - FEES_RT, tp
    return "OPEN", -FEES_RT, tp


def outcome_tick(sig, tick_p, tick_t):
    """Check WIN/LOSS for one signal using tick data. No state."""
    entry_ns = int(sig["time"].timestamp() * 1e9)
    side = sig["side"]
    ep, sp = sig["entry"], sig["stop"]
    risk = sig["risk_pts"]
    tp = ep + risk * sig["rr"] if side == "bull" else ep - risk * sig["rr"]

    idx0 = np.searchsorted(tick_t, entry_ns, side='right')
    post = tick_p[idx0:]

    if len(post) == 0:
        return "OPEN", -FEES_RT, tp, 0

    if side == "bull":
        sh = np.where(post <= sp)[0]
        th = np.where(post >= tp)[0]
    else:
        sh = np.where(post >= sp)[0]
        th = np.where(post <= tp)[0]

    fs = sh[0] if len(sh) else len(post)
    ft = th[0] if len(th) else len(post)

    if fs < ft:
        exit_ns = tick_t[idx0 + fs]
        if side == "bull":
            return "LOSS", (sp - ep) * PV * CONTRACTS - FEES_RT, tp, exit_ns
        else:
            return "LOSS", (ep - sp) * PV * CONTRACTS - FEES_RT, tp, exit_ns
    elif ft < fs:
        exit_ns = tick_t[idx0 + ft]
        if side == "bull":
            return "WIN", (tp - ep) * PV * CONTRACTS - FEES_RT, tp, exit_ns
        else:
            return "WIN", (ep - tp) * PV * CONTRACTS - FEES_RT, tp, exit_ns

    return "OPEN", -FEES_RT, tp, 0


# ═══════════════════════════════════════════════════════════════
# FULL SIMULATION WITH STATE (cooldown, MCL, zone dedup)
# ═══════════════════════════════════════════════════════════════
def simulate_ticks_full(sigs, tick_p, tick_t):
    """Simulate with cooldown/MCL/zone dedup using tick data."""
    sigs = sorted(sigs, key=lambda x: x["time"])
    trades = []
    current_day = None
    in_pos = False
    pos_exit_ns = 0
    cool_ns = 0
    cl_b = cl_r = 0
    used = set()

    for s in sigs:
        if s["date"] != current_day:
            current_day = s["date"]
            in_pos = False; pos_exit_ns = 0; cool_ns = 0
            cl_b = cl_r = 0; used = set()

        entry_ns = int(s["time"].timestamp() * 1e9)

        if in_pos:
            if pos_exit_ns and entry_ns >= pos_exit_ns:
                in_pos = False
            else:
                continue
        if cool_ns and entry_ns < cool_ns:
            continue
        zk = (s["side"], s["zone"], s["zone_top"], s["zone_bot"])
        if zk in used:
            continue
        if s["side"] == "bull" and cl_b >= MCL:
            continue
        if s["side"] == "bear" and cl_r >= MCL:
            continue

        result, pnl, tp, exit_ns = outcome_tick(s, tick_p, tick_t)

        used.add(zk)
        exit_time = None
        if exit_ns > 0:
            exit_time = datetime.fromtimestamp(exit_ns / 1e9, tz=CT)
            in_pos = True
            pos_exit_ns = exit_ns
            cool_ns = exit_ns + COOLDOWN_S * 1_000_000_000
        else:
            in_pos = True
            pos_exit_ns = int((datetime.now(CT) + timedelta(hours=24)).timestamp() * 1e9)

        if result == "LOSS":
            if s["side"] == "bull": cl_b += 1
            else: cl_r += 1
        elif result == "WIN":
            if s["side"] == "bull": cl_b = 0
            else: cl_r = 0

        trades.append({
            **s, "target": tp, "result": result,
            "exit_time": exit_time, "pnl": pnl,
        })

    return trades


# ═══════════════════════════════════════════════════════════════
# STATS HELPER
# ═══════════════════════════════════════════════════════════════
def calc_stats(trades):
    if not trades:
        return {"n": 0, "w": 0, "l": 0, "wr": 0, "pnl": 0, "pf": 0,
                "avg_w": 0, "avg_l": 0}
    wins = [t for t in trades if t["result"] == "WIN"]
    losses = [t for t in trades if t["result"] == "LOSS"]
    pnl = sum(t["pnl"] for t in trades)
    gw = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    return {
        "n": len(trades), "w": len(wins), "l": len(losses),
        "wr": 100 * len(wins) / len(trades) if trades else 0,
        "pnl": pnl, "pf": gw / gl if gl > 0 else float('inf'),
        "avg_w": gw / len(wins) if wins else 0,
        "avg_l": -gl / len(losses) if losses else 0,
    }


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    from tsxapipy import authenticate, APIClient
    from backtest_topstep import fetch_with_rollover, build_dr, build_dr_htf
    from v106_dynamic_rr_zone_entry import (
        get_liquidity_levels, in_kz, KZ, NS_MIN,
    )
    from backtest_entry_modes import (
        gen_sweep_entries_enriched, apply_entry_mode, simulate_trades,
        MODE_CLOSE_ENTRY, MODE_ZONE_CONF, MODE_NAMES,
    )

    t0 = time.time()
    print("=" * 80)
    print("TICK-LEVEL BACKTEST — V106 ICT Strategy")
    print("  Signals:    TopstepX API bars (5m/1m/15m)")
    print("  Simulation: Databento NQ tick data (nanosecond)")
    print("=" * 80)

    # ── Find ALL trading days with tick data ──
    all_tick_files = sorted(globmod.glob(
        os.path.join(TICK_DIR, "glbx-mdp3-*.trades.dbn.zst")))
    target_dates = []
    for f in all_tick_files:
        ds = os.path.basename(f).split('-')[2].split('.')[0]
        d = dt_date(int(ds[:4]), int(ds[4:6]), int(ds[6:8]))
        if d.weekday() < 5 and os.path.getsize(f) > 500_000:
            target_dates.append(d)
    print(f"\nTick data: {len(target_dates)} trading days "
          f"({target_dates[0]} → {target_dates[-1]})")

    # ── Load tick data ──
    print("\nLoading tick data...")
    tick_p, tick_t, loaded_dates = load_ticks(target_dates)
    if tick_p is None:
        print("No tick data!"); return
    print(f"  Total: {len(tick_p):,} ticks loaded")

    # ── Fetch API bars (cached after first run) ──
    print("\nLoading API bars for signal generation...")
    first_tick = datetime(target_dates[0].year, target_dates[0].month,
                          target_dates[0].day, 0, 0, 0, tzinfo=CT)
    lookback = first_tick - timedelta(days=7)
    end = datetime(target_dates[-1].year, target_dates[-1].month,
                   target_dates[-1].day, 23, 59, 59, tzinfo=CT) + timedelta(days=1)
    sd = str(target_dates[0])
    ed = str(target_dates[-1])

    # Try cache first, only authenticate if needed
    if (load_bar_cache(5, sd, ed) and load_bar_cache(15, sd, ed)
            and load_bar_cache(1, sd, ed)):
        api = None  # don't need API
        b5 = load_bar_cache(5, sd, ed)
        b15 = load_bar_cache(15, sd, ed)
        b1 = load_bar_cache(1, sd, ed)
    else:
        token, token_time = authenticate()
        api = APIClient(initial_token=token, token_acquired_at=token_time)
        b5 = fetch_or_cache(api, 5, lookback, end, sd, ed)
        b15 = fetch_or_cache(api, 15, lookback, end, sd, ed)
        b1 = fetch_or_cache(api, 1, lookback, end, sd, ed)
    print(f"  5m: {len(b5)} | 15m: {len(b15)} | 1m: {len(b1)}")

    dr5 = build_dr(b5)
    dr15 = build_dr_htf(b15)
    all_dates = sorted(dr5.keys())
    trade_dates = [d for d in loaded_dates if d in dr5]
    print(f"  Trading dates: {len(trade_dates)} days "
          f"({trade_dates[0] if trade_dates else '?'} → "
          f"{trade_dates[-1] if trade_dates else '?'})")

    # ── Generate enriched signals ──
    print("\nGenerating enriched signals...")
    all_raw = []
    for d in trade_dates:
        ds5, de5 = dr5[d]
        liq = get_liquidity_levels(b5, dr5, d, all_dates)

        seen_ns = set()
        b1_cutoff = 0
        for cursor in range(ds5 + 1, de5 + 1):
            if cursor < de5:
                next_ns = b5[cursor]["time_ns"] + 5 * NS_MIN
            else:
                next_ns = b5[cursor - 1]["time_ns"] + 10 * NS_MIN
            while b1_cutoff < len(b1) and b1[b1_cutoff]["time_ns"] < next_ns:
                b1_cutoff += 1

            ents = gen_sweep_entries_enriched(
                b5[:cursor + 1], b1[:b1_cutoff], ds5, cursor, d, liq)

            for e in sorted(ents, key=lambda x: (x["ns"],
                            -{"ifvg": 2, "disp_fvg": 1}.get(x["zt"], 0))):
                if e["ns"] in seen_ns:
                    continue
                _et = datetime.fromtimestamp(e["ns"] / 1e9, tz=CT)
                if (_et.hour < SIM_START_HOUR or
                        (_et.hour == SIM_START_HOUR and _et.minute < SIM_START_MIN)):
                    continue
                seen_ns.add(e["ns"])
                e["_date"] = d
                e["_ds5"] = ds5
                e["_liq"] = liq
                all_raw.append(e)

    print(f"  Raw entries: {len(all_raw)}")

    # ── Build Mode 2 and Mode 3 signals ──
    modes = {
        "Mode 2 (1m Close)": MODE_CLOSE_ENTRY,
        "Mode 3 (Zone+Confirm)": MODE_ZONE_CONF,
    }
    mode_sigs = {}
    for label, mode in modes.items():
        sigs = []
        for raw in all_raw:
            sig = apply_entry_mode(
                raw, mode, b1, b5, raw["_ds5"],
                raw["_liq"], dr15, b15, raw["_date"],
            )
            if sig:
                sigs.append(sig)
        mode_sigs[label] = sigs
        print(f"  {label}: {len(sigs)} signals")

    # ═══════════════════════════════════════════════════════════════
    # PART 1: SIGNAL-BY-SIGNAL COMPARISON (no cooldown/MCL)
    # Each signal simulated independently with 1m bars AND ticks
    # ═══════════════════════════════════════════════════════════════
    for label, sigs in mode_sigs.items():
        print(f"\n{'=' * 80}")
        print(f"SIGNAL-BY-SIGNAL COMPARISON — {label}")
        print(f"  (Each signal checked independently — no cooldown/MCL)")
        print(f"{'=' * 80}")

        mismatches = 0
        mismatch_details = []
        results_1m = []
        results_tk = []

        for s in sorted(sigs, key=lambda x: x["time"]):
            r1, pnl1, tp1 = outcome_1m(s, b1)
            rt, pnlt, tpt, exit_ns = outcome_tick(s, tick_p, tick_t)

            if r1 != rt:
                mismatches += 1
                mismatch_details.append((s, r1, pnl1, rt, pnlt, tp1, exit_ns))

            results_1m.append({"result": r1, "pnl": pnl1})
            results_tk.append({"result": rt, "pnl": pnlt})

        # Signal-level summary
        n = len(sigs)
        w1 = sum(1 for r in results_1m if r["result"] == "WIN")
        wt = sum(1 for r in results_tk if r["result"] == "WIN")
        pnl1 = sum(r["pnl"] for r in results_1m)
        pnlt = sum(r["pnl"] for r in results_tk)

        print(f"\n  {'Metric':20} {'1m Bars':>10} {'Ticks':>10} {'Diff':>10}")
        print(f"  {'─' * 52}")
        print(f"  {'Signals':20} {n:>10} {n:>10}")
        print(f"  {'Wins':20} {w1:>10} {wt:>10} {wt-w1:>+10}")
        print(f"  {'Win Rate':20} {100*w1/n if n else 0:>9.1f}% "
              f"{100*wt/n if n else 0:>9.1f}%")
        print(f"  {'Raw P&L':20} ${pnl1:>9,.0f} ${pnlt:>9,.0f} "
              f"${pnlt-pnl1:>+9,.0f}")
        print(f"  {'Mismatches':20} {mismatches:>10}")

        if mismatch_details:
            print(f"\n  Mismatched signals:")
            for s, r1, p1, rt, pt, tp1, ens in mismatch_details:
                print(f"    {s['date']} {s['time'].strftime('%H:%M')} "
                      f"{s['side'].upper():4s} @ {s['entry']:.2f} │ "
                      f"1m={r1} ${p1:+,.0f}  tick={rt} ${pt:+,.0f}  "
                      f"Δ${pt-p1:+,.0f}")

    # ═══════════════════════════════════════════════════════════════
    # PART 2: FULL TRADE SIMULATION (with cooldown/MCL/zone dedup)
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print("FULL SIMULATION COMPARISON (with cooldown, MCL, zone dedup)")
    print(f"{'=' * 80}")

    print(f"\n  {'Mode':<25} {'Sim':>6} {'Tr':>4} {'W':>3} {'L':>3} "
          f"{'WR%':>6} {'P&L':>10} {'AvgW':>7} {'AvgL':>7} {'PF':>6}")
    print(f"  {'─' * 80}")

    for label, sigs in mode_sigs.items():
        # 1m bar simulation
        trades_1m = simulate_trades(sigs, b1)
        s1 = calc_stats(trades_1m)
        print(f"  {label:<25} {'1m':>6} {s1['n']:>4} {s1['w']:>3} {s1['l']:>3} "
              f"{s1['wr']:>5.1f}% ${s1['pnl']:>9,.0f} ${s1['avg_w']:>6,.0f} "
              f"${s1['avg_l']:>6,.0f} {s1['pf']:>6.2f}")

        # Tick simulation
        trades_tk = simulate_ticks_full(sigs, tick_p, tick_t)
        st = calc_stats(trades_tk)
        print(f"  {'':25} {'tick':>6} {st['n']:>4} {st['w']:>3} {st['l']:>3} "
              f"{st['wr']:>5.1f}% ${st['pnl']:>9,.0f} ${st['avg_w']:>6,.0f} "
              f"${st['avg_l']:>6,.0f} {st['pf']:>6.2f}")
        print()

    # ═══════════════════════════════════════════════════════════════
    # PART 3: DETAILED TICK TRADES — Mode 3
    # ═══════════════════════════════════════════════════════════════
    print(f"{'=' * 80}")
    print("DETAILED TRADES — Mode 3 (Zone+Confirm) with TICK simulation")
    print(f"{'=' * 80}")

    trades_m3_tick = simulate_ticks_full(mode_sigs["Mode 3 (Zone+Confirm)"],
                                          tick_p, tick_t)
    daily_pnl = defaultdict(float)
    daily_trades = defaultdict(list)
    for t in trades_m3_tick:
        daily_pnl[t["date"]] += t["pnl"]
        daily_trades[t["date"]].append(t)

    # Show trades for each day (compact format)
    for d in sorted(daily_trades):
        trades_d = daily_trades[d]
        wins = sum(1 for t in trades_d if t["result"] == "WIN")
        losses = sum(1 for t in trades_d if t["result"] == "LOSS")
        pnl_d = daily_pnl[d]
        print(f"\n  {d}  ({wins}W/{losses}L = ${pnl_d:+,.0f})")
        for t in trades_d:
            exit_str = (f"exit {t['exit_time'].strftime('%H:%M')}"
                        if t.get("exit_time") else "OPEN")
            print(f"    {t['time'].strftime('%H:%M')} {t['side'].upper():4s} "
                  f"@ {t['entry']:>9.2f}  SL {t['stop']:>9.2f}  "
                  f"TP {t['target']:>9.2f}  risk ${t['risk_$']:>5.0f} "
                  f"sc={t['score']} rr={t['rr']}  "
                  f"{t['result']:4s} {exit_str} ${t['pnl']:>+7,.0f}")

    # Weekly summary
    from collections import OrderedDict
    weekly_pnl = OrderedDict()
    weekly_trades = OrderedDict()
    for d in sorted(daily_pnl):
        # ISO week
        yr, wk, _ = d.isocalendar()
        wkey = f"{yr}-W{wk:02d}"
        weekly_pnl[wkey] = weekly_pnl.get(wkey, 0) + daily_pnl[d]
        weekly_trades[wkey] = weekly_trades.get(wkey, 0) + len(daily_trades[d])

    print(f"\n  {'─' * 50}")
    print(f"  WEEKLY SUMMARY")
    print(f"  {'Week':10} {'Trades':>6} {'P&L':>10} {'Cumul':>10}")
    print(f"  {'─' * 40}")
    cumul = 0
    for wk in weekly_pnl:
        cumul += weekly_pnl[wk]
        print(f"  {wk:10} {weekly_trades[wk]:>6} "
              f"${weekly_pnl[wk]:>+9,.0f} ${cumul:>+9,.0f}")
    print(f"  {'─' * 40}")
    n_days = len(daily_pnl)
    total = sum(daily_pnl.values())
    print(f"  TOTAL ({n_days} days)  ${total:>+9,.0f}  "
          f"(${total/n_days:>+,.0f}/day)")

    # ═══════════════════════════════════════════════════════════════
    # PART 4: MISMATCH ANALYSIS
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print("MISMATCH ANALYSIS — Where 1m bars got it WRONG")
    print(f"  (Signals where 1m bar result ≠ tick result)")
    print(f"{'=' * 80}")

    found_any = False
    for label, sigs in mode_sigs.items():
        for s in sorted(sigs, key=lambda x: x["time"]):
            r1, pnl1, tp1 = outcome_1m(s, b1)
            rt, pnlt, _, exit_ns = outcome_tick(s, tick_p, tick_t)
            if r1 != rt:
                found_any = True
                print(f"\n  {label}")
                print(f"  {s['date']} {s['time'].strftime('%H:%M')} "
                      f"{s['side'].upper()} @ {s['entry']:.2f}")
                print(f"    SL: {s['stop']:.2f}  TP: {tp1:.2f}  "
                      f"risk: {s['risk_pts']:.1f}pt (${s['risk_$']:.0f})")
                print(f"    1m bar: {r1:4s} ${pnl1:+,.0f}")
                print(f"    Tick:   {rt:4s} ${pnlt:+,.0f}")
                print(f"    P&L difference: ${pnlt - pnl1:+,.0f}")
                if exit_ns > 0:
                    exit_dt = datetime.fromtimestamp(exit_ns / 1e9, tz=CT)
                    print(f"    Tick exit at: {exit_dt.strftime('%H:%M:%S.%f')}")

    if not found_any:
        print("\n  No mismatches found — 1m bar results match tick results!")

    # ═══════════════════════════════════════════════════════════════
    # PART 5: DRAWDOWN ANALYSIS & EQUITY CURVE
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print("DRAWDOWN ANALYSIS — Tick Simulation")
    print(f"{'=' * 80}")

    # Run tick simulation for Mode 2 (Mode 3 already done above as trades_m3_tick)
    trades_m2_tick = simulate_ticks_full(mode_sigs["Mode 2 (1m Close)"],
                                          tick_p, tick_t)

    for label, trades_tk in [("Mode 2 (1m Close)", trades_m2_tick),
                              ("Mode 3 (Zone+Confirm)", trades_m3_tick)]:
        print(f"\n  ── {label} {'─' * (55 - len(label))}")

        if not trades_tk:
            print("    No trades.")
            continue

        st = calc_stats(trades_tk)

        # ── Daily P&L for this mode ──
        dpnl = defaultdict(float)
        for t in trades_tk:
            dpnl[t["date"]] += t["pnl"]
        sorted_days = sorted(dpnl.keys())

        # ── Equity curve (cumulative P&L) ──
        equity = []
        running = 0.0
        for d in sorted_days:
            running += dpnl[d]
            equity.append(running)

        # ── Max drawdown (peak to trough) ──
        peak = 0.0
        max_dd = 0.0
        dd_peak_day = dd_trough_day = sorted_days[0]
        cur_peak_day = sorted_days[0]
        for i, d in enumerate(sorted_days):
            if equity[i] > peak:
                peak = equity[i]
                cur_peak_day = d
            dd = peak - equity[i]
            if dd > max_dd:
                max_dd = dd
                dd_peak_day = cur_peak_day
                dd_trough_day = d

        # ── Max consecutive losses ──
        max_consec = 0
        cur_consec = 0
        for t in sorted(trades_tk, key=lambda x: x["time"]):
            if t["result"] == "LOSS":
                cur_consec += 1
                max_consec = max(max_consec, cur_consec)
            else:
                cur_consec = 0

        # ── Longest losing streak in days ──
        losing_days = [d for d in sorted_days if dpnl[d] < 0]
        max_streak_days = 0
        cur_streak = 0
        for i, d in enumerate(sorted_days):
            if dpnl[d] < 0:
                cur_streak += 1
                max_streak_days = max(max_streak_days, cur_streak)
            else:
                cur_streak = 0

        # ── Best / worst day ──
        best_day = max(sorted_days, key=lambda d: dpnl[d])
        worst_day = min(sorted_days, key=lambda d: dpnl[d])

        # ── Expectancy per trade ──
        expectancy = st["pnl"] / st["n"] if st["n"] else 0

        print(f"    {'Trades':24s} {st['n']:>6}")
        print(f"    {'Win Rate':24s} {st['wr']:>5.1f}%")
        print(f"    {'Total P&L':24s} ${st['pnl']:>+10,.0f}")
        print(f"    {'Avg Win':24s} ${st['avg_w']:>+10,.0f}")
        print(f"    {'Avg Loss':24s} ${st['avg_l']:>+10,.0f}")
        print(f"    {'Expectancy / Trade':24s} ${expectancy:>+10,.0f}")
        print(f"    {'Profit Factor':24s} {st['pf']:>10.2f}")
        print(f"    {'Max Drawdown':24s} ${max_dd:>10,.0f}"
              f"  ({dd_peak_day} → {dd_trough_day})")
        print(f"    {'Max Consecutive Losses':24s} {max_consec:>10}")
        print(f"    {'Longest Losing Streak':24s} {max_streak_days:>8} days")
        print(f"    {'Best Day':24s} ${dpnl[best_day]:>+10,.0f}  ({best_day})")
        print(f"    {'Worst Day':24s} ${dpnl[worst_day]:>+10,.0f}  ({worst_day})")
        print(f"    {'Losing Days':24s} {len(losing_days):>4} / {len(sorted_days)}"
              f"  ({100*len(losing_days)/len(sorted_days):.0f}%)")

    # ── Compact equity curve — Mode 2 tick sim ──
    dpnl_m2 = defaultdict(float)
    for t in trades_m2_tick:
        dpnl_m2[t["date"]] += t["pnl"]
    sorted_days_m2 = sorted(dpnl_m2.keys())

    if sorted_days_m2:
        print(f"\n  {'─' * 60}")
        print(f"  EQUITY CURVE — Mode 2 (1m Close) Tick Sim — Daily P&L")
        print(f"  {'Date':12s} {'Day P&L':>9s} {'Cumul':>9s}  Chart")
        print(f"  {'─' * 60}")

        eq_m2 = []
        run_m2 = 0.0
        for d in sorted_days_m2:
            run_m2 += dpnl_m2[d]
            eq_m2.append(run_m2)

        # Scale bars: find max absolute cumulative for chart width
        max_abs = max(abs(v) for v in eq_m2) if eq_m2 else 1
        bar_width = 30

        for i, d in enumerate(sorted_days_m2):
            day_pnl = dpnl_m2[d]
            cum = eq_m2[i]
            bar_len = int(abs(cum) / max_abs * bar_width) if max_abs else 0
            if cum >= 0:
                bar = ' ' * bar_width + '│' + '█' * bar_len
            else:
                pad = bar_width - bar_len
                bar = ' ' * pad + '░' * bar_len + '│'
            print(f"  {d}  ${day_pnl:>+8,.0f} ${cum:>+8,.0f}  {bar}")

        print(f"  {'─' * 60}")

    # ═══════════════════════════════════════════════════════════════
    # PART 6: WHAT MAKES A VALID ZONE? — Confluence breakdown
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print("WHAT MAKES A VALID ZONE? — Win rate by every factor")
    print("  Mode 2 (1m Close) tick simulation — 156 trades")
    print(f"{'=' * 80}")

    trades_anal = trades_m2_tick  # analyze production mode

    def wr_breakdown(trades, key_fn, label):
        """Print win rate breakdown by a grouping function."""
        groups = defaultdict(list)
        for t in trades:
            k = key_fn(t)
            groups[k].append(t)
        print(f"\n  ── {label} {'─' * (60 - len(label))}")
        print(f"  {'Value':<20} {'Tr':>4} {'W':>3} {'L':>3} {'WR%':>6} "
              f"{'P&L':>9} {'AvgPnL':>8}")
        print(f"  {'─' * 60}")
        for k in sorted(groups.keys(), key=lambda x: (str(x))):
            tr = groups[k]
            w = sum(1 for t in tr if t["result"] == "WIN")
            l = sum(1 for t in tr if t["result"] == "LOSS")
            pnl = sum(t["pnl"] for t in tr)
            wr = 100 * w / len(tr) if tr else 0
            avg = pnl / len(tr) if tr else 0
            print(f"  {str(k):<20} {len(tr):>4} {w:>3} {l:>3} {wr:>5.1f}% "
                  f"${pnl:>+8,.0f} ${avg:>+7,.0f}")

    # Score
    wr_breakdown(trades_anal, lambda t: t["score"], "BY SCORE")

    # RR tier
    wr_breakdown(trades_anal, lambda t: t["rr"], "BY RR TIER")

    # Zone type
    wr_breakdown(trades_anal, lambda t: t["zone"], "BY ZONE TYPE")

    # Side
    wr_breakdown(trades_anal, lambda t: t["side"], "BY SIDE (bull/bear)")

    # Hour of entry
    wr_breakdown(trades_anal, lambda t: f"{t['time'].hour:02d}:00",
                 "BY HOUR")

    # Risk bucket
    def risk_bucket(t):
        r = t["risk_$"]
        if r <= 300: return "≤$300"
        if r <= 500: return "$301-500"
        if r <= 700: return "$501-700"
        return "$701-1000"
    wr_breakdown(trades_anal, risk_bucket, "BY RISK SIZE")

    # Risk in points
    def risk_pt_bucket(t):
        r = t["risk_pts"]
        if r <= 3: return "≤3pt"
        if r <= 6: return "3-6pt"
        if r <= 10: return "6-10pt"
        return ">10pt"
    wr_breakdown(trades_anal, risk_pt_bucket, "BY RISK (points)")

    # Score >= threshold analysis
    print(f"\n  ── CUMULATIVE SCORE FILTER {'─' * 33}")
    print(f"  {'Min Score':<20} {'Tr':>4} {'W':>3} {'L':>3} {'WR%':>6} "
          f"{'P&L':>9} {'PF':>6} {'$/trade':>8}")
    print(f"  {'─' * 60}")
    for min_sc in range(1, 9):
        filt = [t for t in trades_anal if t["score"] >= min_sc]
        if not filt:
            continue
        w = sum(1 for t in filt if t["result"] == "WIN")
        l = sum(1 for t in filt if t["result"] == "LOSS")
        pnl = sum(t["pnl"] for t in filt)
        gw = sum(t["pnl"] for t in filt if t["result"] == "WIN")
        gl = abs(sum(t["pnl"] for t in filt if t["result"] == "LOSS"))
        wr = 100 * w / len(filt) if filt else 0
        pf = gw / gl if gl > 0 else float('inf')
        avg = pnl / len(filt) if filt else 0
        print(f"  score >= {min_sc:<10} {len(filt):>4} {w:>3} {l:>3} {wr:>5.1f}% "
              f"${pnl:>+8,.0f} {pf:>6.2f} ${avg:>+7,.0f}")

    # Score + RR combo
    print(f"\n  ── SCORE + RR COMBO {'─' * 40}")
    print(f"  {'Combo':<20} {'Tr':>4} {'W':>3} {'L':>3} {'WR%':>6} "
          f"{'P&L':>9} {'PF':>6}")
    print(f"  {'─' * 55}")
    combos = defaultdict(list)
    for t in trades_anal:
        combos[(t["score"], t["rr"])].append(t)
    for (sc, rr) in sorted(combos.keys()):
        tr = combos[(sc, rr)]
        w = sum(1 for t in tr if t["result"] == "WIN")
        l = sum(1 for t in tr if t["result"] == "LOSS")
        pnl = sum(t["pnl"] for t in tr)
        gw = sum(t["pnl"] for t in tr if t["result"] == "WIN")
        gl = abs(sum(t["pnl"] for t in tr if t["result"] == "LOSS"))
        wr = 100 * w / len(tr) if tr else 0
        pf = gw / gl if gl > 0 else float('inf')
        print(f"  sc={sc} rr={rr:<8} {len(tr):>4} {w:>3} {l:>3} {wr:>5.1f}% "
              f"${pnl:>+8,.0f} {pf:>6.2f}")

    # Side + Hour combo
    print(f"\n  ── SIDE + HOUR {'─' * 44}")
    print(f"  {'Combo':<20} {'Tr':>4} {'W':>3} {'L':>3} {'WR%':>6} "
          f"{'P&L':>9}")
    print(f"  {'─' * 50}")
    sh_combos = defaultdict(list)
    for t in trades_anal:
        sh_combos[(t["side"], t["time"].hour)].append(t)
    for (side, hr) in sorted(sh_combos.keys()):
        tr = sh_combos[(side, hr)]
        w = sum(1 for t in tr if t["result"] == "WIN")
        l = sum(1 for t in tr if t["result"] == "LOSS")
        pnl = sum(t["pnl"] for t in tr)
        wr = 100 * w / len(tr) if tr else 0
        print(f"  {side:4s} {hr:02d}:00       {len(tr):>4} {w:>3} {l:>3} {wr:>5.1f}% "
              f"${pnl:>+8,.0f}")

    # ═══════════════════════════════════════════════════════════════
    # PART 7: DISP_FVG DEEP DIVE — what makes the good zones great?
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print("DISP_FVG DEEP DIVE — 115 trades, 64.3% WR — what pushes it higher?")
    print(f"{'=' * 80}")

    dfvg = [t for t in trades_m2_tick if t["zone"] == "disp_fvg"]

    wr_breakdown(dfvg, lambda t: t["score"], "DISP_FVG BY SCORE")
    wr_breakdown(dfvg, lambda t: t["rr"], "DISP_FVG BY RR TIER")
    wr_breakdown(dfvg, lambda t: t["side"], "DISP_FVG BY SIDE")
    wr_breakdown(dfvg, lambda t: f"{t['time'].hour:02d}:00", "DISP_FVG BY HOUR")
    wr_breakdown(dfvg, risk_bucket, "DISP_FVG BY RISK SIZE")
    wr_breakdown(dfvg, risk_pt_bucket, "DISP_FVG BY RISK (points)")

    # Zone width
    def zone_width(t):
        w = abs(t["zone_top"] - t["zone_bot"])
        if w <= 2: return "≤2pt"
        if w <= 5: return "2-5pt"
        if w <= 10: return "5-10pt"
        return ">10pt"
    wr_breakdown(dfvg, zone_width, "DISP_FVG BY ZONE WIDTH")

    # Distance from zone at entry
    def dist_bucket(t):
        d = t.get("dist_from_zone", 0)
        if d <= 2: return "≤2pt"
        if d <= 5: return "2-5pt"
        if d <= 10: return "5-10pt"
        return ">10pt"
    wr_breakdown(dfvg, dist_bucket, "DISP_FVG BY ENTRY DISTANCE FROM ZONE")

    # Score + Side combo
    print(f"\n  ── DISP_FVG: SCORE + SIDE {'─' * 33}")
    print(f"  {'Combo':<20} {'Tr':>4} {'W':>3} {'L':>3} {'WR%':>6} "
          f"{'P&L':>9} {'PF':>6}")
    print(f"  {'─' * 55}")
    ss_combos = defaultdict(list)
    for t in dfvg:
        ss_combos[(t["score"], t["side"])].append(t)
    for (sc, side) in sorted(ss_combos.keys()):
        tr = ss_combos[(sc, side)]
        w = sum(1 for t in tr if t["result"] == "WIN")
        l = sum(1 for t in tr if t["result"] == "LOSS")
        pnl = sum(t["pnl"] for t in tr)
        gw = sum(t["pnl"] for t in tr if t["result"] == "WIN")
        gl = abs(sum(t["pnl"] for t in tr if t["result"] == "LOSS"))
        wr = 100 * w / len(tr) if tr else 0
        pf = gw / gl if gl > 0 else float('inf')
        print(f"  sc={sc} {side:<10} {len(tr):>4} {w:>3} {l:>3} {wr:>5.1f}% "
              f"${pnl:>+8,.0f} {pf:>6.2f}")

    # Cumulative score filter on disp_fvg only
    print(f"\n  ── DISP_FVG: CUMULATIVE SCORE FILTER {'─' * 22}")
    print(f"  {'Min Score':<20} {'Tr':>4} {'W':>3} {'L':>3} {'WR%':>6} "
          f"{'P&L':>9} {'PF':>6} {'$/trade':>8}")
    print(f"  {'─' * 60}")
    for min_sc in range(1, 9):
        filt = [t for t in dfvg if t["score"] >= min_sc]
        if not filt:
            continue
        w = sum(1 for t in filt if t["result"] == "WIN")
        l = sum(1 for t in filt if t["result"] == "LOSS")
        pnl = sum(t["pnl"] for t in filt)
        gw = sum(t["pnl"] for t in filt if t["result"] == "WIN")
        gl = abs(sum(t["pnl"] for t in filt if t["result"] == "LOSS"))
        wr = 100 * w / len(filt) if filt else 0
        pf = gw / gl if gl > 0 else float('inf')
        avg = pnl / len(filt) if filt else 0
        print(f"  score >= {min_sc:<10} {len(filt):>4} {w:>3} {l:>3} {wr:>5.1f}% "
              f"${pnl:>+8,.0f} {pf:>6.2f} ${avg:>+7,.0f}")

    # Combos that might be golden: disp_fvg + score>=4 + specific hours
    print(f"\n  ── DISP_FVG FILTERS: STACKING CONFLUENCES {'─' * 17}")
    filters = [
        ("disp_fvg only", lambda t: True),
        ("+ score >= 4", lambda t: t["score"] >= 4),
        ("+ score >= 4, no 10am", lambda t: t["score"] >= 4 and t["time"].hour != 10),
        ("+ score >= 4, bull only", lambda t: t["score"] >= 4 and t["side"] == "bull"),
        ("+ score >= 4, bear only", lambda t: t["score"] >= 4 and t["side"] == "bear"),
        ("+ score >= 4, risk>$500", lambda t: t["score"] >= 4 and t["risk_$"] > 500),
        ("+ score >= 4, risk≤$500", lambda t: t["score"] >= 4 and t["risk_$"] <= 500),
        ("+ score >= 4, zone≤5pt", lambda t: t["score"] >= 4 and abs(t["zone_top"]-t["zone_bot"]) <= 5),
        ("+ score >= 4, zone>5pt", lambda t: t["score"] >= 4 and abs(t["zone_top"]-t["zone_bot"]) > 5),
        ("+ sc>=4, no10am, risk>3pt", lambda t: t["score"] >= 4 and t["time"].hour != 10 and t["risk_pts"] > 3),
        ("+ sc>=4, 8-9am+11am+1-2pm", lambda t: t["score"] >= 4 and t["time"].hour in (8,9,11,13,14)),
        ("+ sc>=6", lambda t: t["score"] >= 6),
    ]
    print(f"  {'Filter':<30} {'Tr':>4} {'W':>3} {'L':>3} {'WR%':>6} "
          f"{'P&L':>9} {'PF':>6} {'$/day':>7}")
    print(f"  {'─' * 70}")
    for name, fn in filters:
        filt = [t for t in dfvg if fn(t)]
        if not filt:
            print(f"  {name:<30}    0")
            continue
        w = sum(1 for t in filt if t["result"] == "WIN")
        l = sum(1 for t in filt if t["result"] == "LOSS")
        pnl = sum(t["pnl"] for t in filt)
        gw = sum(t["pnl"] for t in filt if t["result"] == "WIN")
        gl = abs(sum(t["pnl"] for t in filt if t["result"] == "LOSS"))
        wr = 100 * w / len(filt) if filt else 0
        pf = gw / gl if gl > 0 else float('inf')
        days_with = len(set(t["date"] for t in filt))
        dpd = pnl / days_with if days_with else 0
        print(f"  {name:<30} {len(filt):>4} {w:>3} {l:>3} {wr:>5.1f}% "
              f"${pnl:>+8,.0f} {pf:>6.2f} ${dpd:>+6,.0f}")

    # ═══════════════════════════════════════════════════════════════
    # PART 8: WHY DO DISP_FVG ZONES FAIL? — Loss autopsy
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print("WHY DO DISP_FVG ZONES FAIL? — Loss autopsy with tick data")
    print(f"{'=' * 80}")

    dfvg_wins = [t for t in trades_m2_tick if t["zone"] == "disp_fvg" and t["result"] == "WIN"]
    dfvg_losses = [t for t in trades_m2_tick if t["zone"] == "disp_fvg" and t["result"] == "LOSS"]

    # For each trade, compute tick-level metrics:
    # - Time to exit (seconds)
    # - Max adverse excursion (MAE) — how far against before outcome
    # - Max favorable excursion (MFE) — how far in favor before outcome
    # - Stop distance in points
    def tick_metrics(t, tick_p, tick_t):
        entry_ns = int(t["time"].timestamp() * 1e9)
        idx0 = np.searchsorted(tick_t, entry_ns, side='right')
        exit_ns = int(t["exit_time"].timestamp() * 1e9) if t.get("exit_time") else 0
        if exit_ns == 0 or idx0 >= len(tick_p):
            return None
        idx1 = np.searchsorted(tick_t, exit_ns, side='right')
        segment = tick_p[idx0:idx1+1]
        if len(segment) == 0:
            return None

        ep = t["entry"]
        sp = t["stop"]
        time_s = (exit_ns - entry_ns) / 1e9

        if t["side"] == "bull":
            mae = ep - segment.min()  # max move against (below entry)
            mfe = segment.max() - ep  # max move in favor (above entry)
        else:
            mae = segment.max() - ep  # max move against (above entry)
            mfe = ep - segment.min()  # max move in favor (below entry)

        stop_dist = abs(ep - sp)
        # How much of the stop was used before reversal (for losses)
        mae_pct = (mae / stop_dist * 100) if stop_dist > 0 else 0

        return {
            "time_s": time_s, "mae": mae, "mfe": mfe,
            "stop_dist": stop_dist, "mae_pct": mae_pct,
        }

    win_metrics = [m for m in (tick_metrics(t, tick_p, tick_t) for t in dfvg_wins) if m]
    loss_metrics = [m for m in (tick_metrics(t, tick_p, tick_t) for t in dfvg_losses) if m]

    if win_metrics and loss_metrics:
        print(f"\n  {'Metric':<30} {'WINS':>10} {'LOSSES':>10}")
        print(f"  {'─' * 52}")

        avg_w_time = sum(m["time_s"] for m in win_metrics) / len(win_metrics)
        avg_l_time = sum(m["time_s"] for m in loss_metrics) / len(loss_metrics)
        print(f"  {'Avg time to exit (sec)':30} {avg_w_time:>9.0f}s {avg_l_time:>9.0f}s")

        med_w_time = sorted(m["time_s"] for m in win_metrics)[len(win_metrics)//2]
        med_l_time = sorted(m["time_s"] for m in loss_metrics)[len(loss_metrics)//2]
        print(f"  {'Median time to exit (sec)':30} {med_w_time:>9.0f}s {med_l_time:>9.0f}s")

        avg_w_mae = sum(m["mae"] for m in win_metrics) / len(win_metrics)
        avg_l_mae = sum(m["mae"] for m in loss_metrics) / len(loss_metrics)
        print(f"  {'Avg MAE (max against, pts)':30} {avg_w_mae:>9.1f}pt {avg_l_mae:>9.1f}pt")

        avg_w_mfe = sum(m["mfe"] for m in win_metrics) / len(win_metrics)
        avg_l_mfe = sum(m["mfe"] for m in loss_metrics) / len(loss_metrics)
        print(f"  {'Avg MFE (max in favor, pts)':30} {avg_w_mfe:>9.1f}pt {avg_l_mfe:>9.1f}pt")

        avg_w_stop = sum(m["stop_dist"] for m in win_metrics) / len(win_metrics)
        avg_l_stop = sum(m["stop_dist"] for m in loss_metrics) / len(loss_metrics)
        print(f"  {'Avg stop distance (pts)':30} {avg_w_stop:>9.1f}pt {avg_l_stop:>9.1f}pt")

        # How many losses got stopped out within first 60 seconds?
        fast_stops = sum(1 for m in loss_metrics if m["time_s"] <= 60)
        print(f"\n  Losses stopped out within 60s: {fast_stops} / {len(loss_metrics)} "
              f"({100*fast_stops/len(loss_metrics):.0f}%)")

        fast_120 = sum(1 for m in loss_metrics if m["time_s"] <= 120)
        print(f"  Losses stopped out within 2min: {fast_120} / {len(loss_metrics)} "
              f"({100*fast_120/len(loss_metrics):.0f}%)")

        # How many losses had MFE > 0 (price went in favor first)?
        went_right = sum(1 for m in loss_metrics if m["mfe"] > 2.0)
        print(f"  Losses where price went 2+pt in favor first: {went_right} / {len(loss_metrics)} "
              f"({100*went_right/len(loss_metrics):.0f}%)")

        went_right5 = sum(1 for m in loss_metrics if m["mfe"] > 5.0)
        print(f"  Losses where price went 5+pt in favor first: {went_right5} / {len(loss_metrics)} "
              f"({100*went_right5/len(loss_metrics):.0f}%)")

        # How many wins had MAE > 50% of stop (took heat)?
        heat_wins = sum(1 for m in win_metrics if m["mae_pct"] > 50)
        print(f"  Wins that took >50% stop heat: {heat_wins} / {len(win_metrics)} "
              f"({100*heat_wins/len(win_metrics):.0f}%)")

        heat_wins80 = sum(1 for m in win_metrics if m["mae_pct"] > 80)
        print(f"  Wins that took >80% stop heat: {heat_wins80} / {len(win_metrics)} "
              f"({100*heat_wins80/len(win_metrics):.0f}%)")

        # Stop distance distribution on losses
        print(f"\n  ── LOSS STOP DISTANCE DISTRIBUTION ──")
        print(f"  {'Stop Dist':<15} {'Count':>5} {'%':>6}")
        print(f"  {'─' * 30}")
        for lo, hi, label in [(0,3,"≤3pt"),(3,6,"3-6pt"),(6,10,"6-10pt"),(10,50,">10pt")]:
            cnt = sum(1 for m in loss_metrics if lo < m["stop_dist"] <= hi or (lo == 0 and m["stop_dist"] <= hi))
            print(f"  {label:<15} {cnt:>5} {100*cnt/len(loss_metrics):>5.0f}%")

        # MFE distribution on losses (did they almost win?)
        print(f"\n  ── LOSS MFE — How close did losers get to winning? ──")
        print(f"  {'MFE':<15} {'Count':>5} {'%':>6}")
        print(f"  {'─' * 30}")
        for lo, hi, label in [(0,1,"<1pt"),(1,3,"1-3pt"),(3,5,"3-5pt"),(5,10,"5-10pt"),(10,100,">10pt")]:
            cnt = sum(1 for m in loss_metrics if lo <= m["mfe"] < hi)
            print(f"  {label:<15} {cnt:>5} {100*cnt/len(loss_metrics):>5.0f}%")

        # Time to exit distribution on losses
        print(f"\n  ── LOSS TIME TO EXIT — How fast do they fail? ──")
        print(f"  {'Time':<15} {'Count':>5} {'%':>6}")
        print(f"  {'─' * 30}")
        for lo, hi, label in [(0,10,"<10s"),(10,60,"10-60s"),(60,300,"1-5min"),(300,900,"5-15min"),(900,99999,">15min")]:
            cnt = sum(1 for m in loss_metrics if lo <= m["time_s"] < hi)
            print(f"  {label:<15} {cnt:>5} {100*cnt/len(loss_metrics):>5.0f}%")

        # Each losing trade detail
        print(f"\n  ── EVERY LOSING DISP_FVG TRADE ──")
        print(f"  {'Date':>10} {'Time':>5} {'Side':>4} {'Entry':>9} {'Stop':>6} "
              f"{'StopDist':>8} {'MAE':>6} {'MFE':>6} {'ExitSec':>7} {'P&L':>8}")
        print(f"  {'─' * 80}")
        for t, m in zip(dfvg_losses, loss_metrics):
            print(f"  {t['date']} {t['time'].strftime('%H:%M'):>5} {t['side']:>4} "
                  f"{t['entry']:>9.2f} {t['stop']:>9.2f} "
                  f"{m['stop_dist']:>6.1f}pt {m['mae']:>5.1f}pt {m['mfe']:>5.1f}pt "
                  f"{m['time_s']:>6.0f}s ${t['pnl']:>+7,.0f}")

    # ═══════════════════════════════════════════════════════════════
    # PART 9: FLAT 1.3 RR — What if we just take the low-hanging fruit?
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print("FLAT RR COMPARISON — disp_fvg only, tick simulation")
    print("  What happens when we stop reaching for 2.0 RR?")
    print(f"{'=' * 80}")

    # Rebuild Mode 2 signals with flat RR overrides, disp_fvg only
    for test_rr in [1.0, 1.3, 1.5, 2.0, 2.5, 3.0]:
        # Take all Mode 2 signals, override RR, filter to disp_fvg
        test_sigs = []
        for raw in all_raw:
            sig = apply_entry_mode(
                raw, MODE_CLOSE_ENTRY, b1, b5, raw["_ds5"],
                raw["_liq"], dr15, b15, raw["_date"],
            )
            if sig and sig["zone"] == "disp_fvg":
                sig["rr"] = test_rr  # override RR
                test_sigs.append(sig)

        test_trades = simulate_ticks_full(test_sigs, tick_p, tick_t)
        if not test_trades:
            continue
        st = calc_stats(test_trades)

        # Max drawdown
        dpnl = defaultdict(float)
        for t in test_trades:
            dpnl[t["date"]] += t["pnl"]
        sorted_days = sorted(dpnl.keys())
        equity = []
        running = 0.0
        for d in sorted_days:
            running += dpnl[d]
            equity.append(running)
        peak = 0.0
        max_dd = 0.0
        for e in equity:
            if e > peak:
                peak = e
            dd = peak - e
            if dd > max_dd:
                max_dd = dd

        losing_days = sum(1 for d in sorted_days if dpnl[d] < 0)

        print(f"\n  RR={test_rr:.1f}  |  {st['n']} trades  |  "
              f"{st['w']}W/{st['l']}L  |  WR {st['wr']:.1f}%  |  "
              f"PF {st['pf']:.2f}  |  P&L ${st['pnl']:+,.0f}  |  "
              f"MaxDD ${max_dd:,.0f}  |  AvgW ${st['avg_w']:+,.0f}  |  "
              f"AvgL ${st['avg_l']:+,.0f}  |  "
              f"Losing days: {losing_days}/{len(sorted_days)}")

    # ── iFVG flat RR comparison ──
    print(f"\n{'=' * 80}")
    print("FLAT RR COMPARISON — iFVG only, tick simulation")
    print(f"{'=' * 80}")

    for test_rr in [1.0, 1.1, 1.2, 1.3, 1.5, 2.0]:
        test_sigs = []
        for raw in all_raw:
            sig = apply_entry_mode(
                raw, MODE_CLOSE_ENTRY, b1, b5, raw["_ds5"],
                raw["_liq"], dr15, b15, raw["_date"],
            )
            if sig and sig["zone"] == "ifvg":
                sig["rr"] = test_rr
                test_sigs.append(sig)

        test_trades = simulate_ticks_full(test_sigs, tick_p, tick_t)
        if not test_trades:
            continue
        st = calc_stats(test_trades)

        dpnl = defaultdict(float)
        for t in test_trades:
            dpnl[t["date"]] += t["pnl"]
        sorted_days = sorted(dpnl.keys())
        equity = []
        running = 0.0
        for d in sorted_days:
            running += dpnl[d]
            equity.append(running)
        peak = 0.0
        max_dd = 0.0
        for e in equity:
            if e > peak:
                peak = e
            dd = peak - e
            if dd > max_dd:
                max_dd = dd

        losing_days = sum(1 for d in sorted_days if dpnl[d] < 0)

        print(f"\n  RR={test_rr:.1f}  |  {st['n']} trades  |  "
              f"{st['w']}W/{st['l']}L  |  WR {st['wr']:.1f}%  |  "
              f"PF {st['pf']:.2f}  |  P&L ${st['pnl']:+,.0f}  |  "
              f"MaxDD ${max_dd:,.0f}  |  AvgW ${st['avg_w']:+,.0f}  |  "
              f"AvgL ${st['avg_l']:+,.0f}  |  "
              f"Losing days: {losing_days}/{len(sorted_days)}")

    # ── Combined (disp_fvg + ifvg) flat RR comparison ──
    print(f"\n{'=' * 80}")
    print("FLAT RR COMPARISON — disp_fvg + iFVG combined, tick simulation")
    print(f"{'=' * 80}")

    for test_rr in [1.0, 1.1, 1.2, 1.3, 1.5, 2.0]:
        test_sigs = []
        for raw in all_raw:
            sig = apply_entry_mode(
                raw, MODE_CLOSE_ENTRY, b1, b5, raw["_ds5"],
                raw["_liq"], dr15, b15, raw["_date"],
            )
            if sig and sig["zone"] in ("disp_fvg", "ifvg"):
                sig["rr"] = test_rr
                test_sigs.append(sig)

        test_trades = simulate_ticks_full(test_sigs, tick_p, tick_t)
        if not test_trades:
            continue
        st = calc_stats(test_trades)

        dpnl = defaultdict(float)
        for t in test_trades:
            dpnl[t["date"]] += t["pnl"]
        sorted_days = sorted(dpnl.keys())
        equity = []
        running = 0.0
        for d in sorted_days:
            running += dpnl[d]
            equity.append(running)
        peak = 0.0
        max_dd = 0.0
        for e in equity:
            if e > peak:
                peak = e
            dd = peak - e
            if dd > max_dd:
                max_dd = dd

        losing_days = sum(1 for d in sorted_days if dpnl[d] < 0)

        print(f"\n  RR={test_rr:.1f}  |  {st['n']} trades  |  "
              f"{st['w']}W/{st['l']}L  |  WR {st['wr']:.1f}%  |  "
              f"PF {st['pf']:.2f}  |  P&L ${st['pnl']:+,.0f}  |  "
              f"MaxDD ${max_dd:,.0f}  |  AvgW ${st['avg_w']:+,.0f}  |  "
              f"AvgL ${st['avg_l']:+,.0f}  |  "
              f"Losing days: {losing_days}/{len(sorted_days)}")

    # ── OPTIMIZATION TWEAKS ──
    print(f"\n{'=' * 80}")
    print("OPTIMIZATION TWEAKS — flat 1.1 RR, disp_fvg + iFVG, tick simulation")
    print(f"{'=' * 80}")

    import backtest_entry_modes as bem
    orig_cool = bem.COOLDOWN_S
    orig_mcl = bem.MCL
    orig_clear = bem.IFVG_INV_CLEAR

    # For IFVG_INV_CLEAR changes, we need to regenerate signals (full cursor loop)
    # For cooldown/MCL changes, we can reuse existing signals (applied in simulate)
    # So: first test IFVG_CLEAR variants with fresh signal gen, then cooldown/MCL with existing sigs

    def run_tweak(name, sigs_to_use, cool, mcl, min_sc):
        bem.COOLDOWN_S = cool
        bem.MCL = mcl
        test_sigs = []
        for raw in sigs_to_use:
            sig = apply_entry_mode(
                raw, MODE_CLOSE_ENTRY, b1, b5, raw["_ds5"],
                raw["_liq"], dr15, b15, raw["_date"],
            )
            if sig and sig["zone"] in ("disp_fvg", "ifvg") and sig.get("score", 0) >= min_sc:
                sig["rr"] = 1.1
                test_sigs.append(sig)
        test_trades = simulate_ticks_full(test_sigs, tick_p, tick_t)
        if not test_trades:
            print(f"  {name:<48s}  -- no trades --")
            return
        st = calc_stats(test_trades)
        dpnl = defaultdict(float)
        for t in test_trades:
            dpnl[t["date"]] += t["pnl"]
        sorted_days = sorted(dpnl.keys())
        eq, run = [], 0.0
        for d in sorted_days:
            run += dpnl[d]; eq.append(run)
        pk, mdd = 0.0, 0.0
        for e in eq:
            if e > pk: pk = e
            dd = pk - e
            if dd > mdd: mdd = dd
        ld = sum(1 for d in sorted_days if dpnl[d] < 0)
        pd = st['pnl'] / len(sorted_days) if sorted_days else 0
        print(f"  {name:<48s} {st['n']:>3d} {st['w']:>3d} {st['l']:>3d}  "
              f"{st['wr']:>5.1f}% ${st['pnl']:>+8,.0f}  {st['pf']:>5.2f} "
              f"${mdd:>6,.0f} ${pd:>+5,.0f}  {ld}/{len(sorted_days)}")

    print(f"\n  {'Config':<48s}  Tr   W   L    WR%       P&L     PF   MaxDD  $/day  LDays")
    print(f"  {'─'*110}")

    # Tests using existing signals (cooldown/MCL/score tweaks only)
    run_tweak("BASELINE (cool=120, MCL=3, all scores)",   all_raw, 120, 3, 1)
    run_tweak("BASELINE sc>=4",                           all_raw, 120, 3, 4)
    run_tweak("#3: cooldown=60s",                         all_raw, 60,  3, 1)
    run_tweak("#3: cooldown=30s",                         all_raw, 30,  3, 1)
    run_tweak("#4: MCL=4",                                all_raw, 120, 4, 1)
    run_tweak("#4: MCL=5",                                all_raw, 120, 5, 1)
    run_tweak("cool=60 + MCL=4",                          all_raw, 60,  4, 1)
    run_tweak("cool=60 + MCL=4 + sc>=4",                  all_raw, 60,  4, 4)
    run_tweak("cool=60 + MCL=5",                          all_raw, 60,  5, 1)

    # Tests requiring signal regeneration (IFVG_INV_CLEAR changes)
    for clear_val in [1.0, 0.5]:
        bem.IFVG_INV_CLEAR = clear_val
        regen_raw = []
        for d in trade_dates:
            ds5r, de5r = dr5[d]
            liq_r = get_liquidity_levels(b5, dr5, d, all_dates)
            seen_ns_r = set()
            b1c_r = 0
            for cursor in range(ds5r + 1, de5r + 1):
                if cursor < de5r:
                    nns = b5[cursor]["time_ns"] + 5 * NS_MIN
                else:
                    nns = b5[cursor - 1]["time_ns"] + 10 * NS_MIN
                while b1c_r < len(b1) and b1[b1c_r]["time_ns"] < nns:
                    b1c_r += 1
                ents_r = gen_sweep_entries_enriched(
                    b5[:cursor + 1], b1[:b1c_r], ds5r, cursor, d, liq_r)
                for e in sorted(ents_r, key=lambda x: (x["ns"],
                                -{"ifvg": 2, "disp_fvg": 1}.get(x["zt"], 0))):
                    if e["ns"] in seen_ns_r:
                        continue
                    _et = datetime.fromtimestamp(e["ns"] / 1e9, tz=CT)
                    if (_et.hour < SIM_START_HOUR or
                            (_et.hour == SIM_START_HOUR and _et.minute < SIM_START_MIN)):
                        continue
                    seen_ns_r.add(e["ns"])
                    e["_date"] = d
                    e["_ds5"] = ds5r
                    e["_liq"] = liq_r
                    regen_raw.append(e)

        n_ifvg = sum(1 for e in regen_raw if e["zt"] == "ifvg")
        run_tweak(f"#2: IFVG_CLEAR={clear_val} ({n_ifvg} ifvg sigs)",
                  regen_raw, 120, 3, 1)
        run_tweak(f"#2: IFVG_CLEAR={clear_val} + cool=60 + MCL=4",
                  regen_raw, 60, 4, 1)

    # Restore originals
    bem.COOLDOWN_S = orig_cool
    bem.MCL = orig_mcl
    bem.IFVG_INV_CLEAR = orig_clear

    # ── Combined flat RR + score filter ──
    print(f"\n{'=' * 80}")
    print("FLAT RR + SCORE FILTER — disp_fvg + iFVG combined, tick simulation")
    print(f"{'=' * 80}")

    for min_sc in [1, 2, 3, 4, 5, 6]:
        print(f"\n  ── Score >= {min_sc} ──")
        for test_rr in [1.0, 1.1, 1.2, 1.3]:
            test_sigs = []
            for raw in all_raw:
                sig = apply_entry_mode(
                    raw, MODE_CLOSE_ENTRY, b1, b5, raw["_ds5"],
                    raw["_liq"], dr15, b15, raw["_date"],
                )
                if sig and sig["zone"] in ("disp_fvg", "ifvg") and sig.get("score", 0) >= min_sc:
                    sig["rr"] = test_rr
                    test_sigs.append(sig)

            test_trades = simulate_ticks_full(test_sigs, tick_p, tick_t)
            if not test_trades:
                continue
            st = calc_stats(test_trades)

            dpnl = defaultdict(float)
            for t in test_trades:
                dpnl[t["date"]] += t["pnl"]
            sorted_days = sorted(dpnl.keys())
            equity = []
            running = 0.0
            for d in sorted_days:
                running += dpnl[d]
                equity.append(running)
            peak = 0.0
            max_dd = 0.0
            for e in equity:
                if e > peak:
                    peak = e
                dd = peak - e
                if dd > max_dd:
                    max_dd = dd

            losing_days = sum(1 for d in sorted_days if dpnl[d] < 0)

            print(f"    RR={test_rr:.1f}  |  {st['n']} trades  |  "
                  f"{st['w']}W/{st['l']}L  |  WR {st['wr']:.1f}%  |  "
                  f"PF {st['pf']:.2f}  |  P&L ${st['pnl']:+,.0f}  |  "
                  f"MaxDD ${max_dd:,.0f}  |  AvgW ${st['avg_w']:+,.0f}  |  "
                  f"AvgL ${st['avg_l']:+,.0f}  |  "
                  f"Losing days: {losing_days}/{len(sorted_days)}")

    # ── Dynamic RR by score (sc>=4 only) ──
    print(f"\n{'=' * 80}")
    print("DYNAMIC RR BY SCORE — sc>=4 only, disp_fvg + iFVG, tick simulation")
    print(f"{'=' * 80}")

    # Test different RR tier combos for score 4, 5, 6+
    rr_combos = [
        {"name": "flat 1.0",  4: 1.0, 5: 1.0, 6: 1.0},
        {"name": "flat 1.1",  4: 1.1, 5: 1.1, 6: 1.1},
        {"name": "flat 1.3",  4: 1.3, 5: 1.3, 6: 1.3},
        {"name": "4=1.0 5=1.1 6=1.3",  4: 1.0, 5: 1.1, 6: 1.3},
        {"name": "4=1.0 5=1.2 6=1.5",  4: 1.0, 5: 1.2, 6: 1.5},
        {"name": "4=1.0 5=1.3 6=1.5",  4: 1.0, 5: 1.3, 6: 1.5},
        {"name": "4=1.1 5=1.3 6=1.5",  4: 1.1, 5: 1.3, 6: 1.5},
        {"name": "4=1.1 5=1.3 6=2.0",  4: 1.1, 5: 1.3, 6: 2.0},
        {"name": "4=1.1 5=1.5 6=2.0",  4: 1.1, 5: 1.5, 6: 2.0},
        {"name": "4=1.2 5=1.5 6=2.0",  4: 1.2, 5: 1.5, 6: 2.0},
    ]

    print(f"\n  {'Config':<25s}  Tr   W   L    WR%       P&L     PF   MaxDD  $/day  LoseDays")
    print(f"  {'─'*100}")

    for combo in rr_combos:
        test_sigs = []
        for raw in all_raw:
            sig = apply_entry_mode(
                raw, MODE_CLOSE_ENTRY, b1, b5, raw["_ds5"],
                raw["_liq"], dr15, b15, raw["_date"],
            )
            if not sig or sig["zone"] not in ("disp_fvg", "ifvg"):
                continue
            sc = sig.get("score", 0)
            if sc < 4:
                continue
            # Assign RR based on score tier
            if sc >= 6:
                sig["rr"] = combo[6]
            elif sc >= 5:
                sig["rr"] = combo[5]
            else:
                sig["rr"] = combo[4]
            test_sigs.append(sig)

        test_trades = simulate_ticks_full(test_sigs, tick_p, tick_t)
        if not test_trades:
            continue
        st = calc_stats(test_trades)

        dpnl = defaultdict(float)
        for t in test_trades:
            dpnl[t["date"]] += t["pnl"]
        sorted_days = sorted(dpnl.keys())
        equity = []
        running = 0.0
        for d in sorted_days:
            running += dpnl[d]
            equity.append(running)
        peak = 0.0
        max_dd = 0.0
        for e in equity:
            if e > peak:
                peak = e
            dd = peak - e
            if dd > max_dd:
                max_dd = dd

        losing_days = sum(1 for d in sorted_days if dpnl[d] < 0)
        per_day = st['pnl'] / len(sorted_days) if sorted_days else 0

        print(f"  {combo['name']:<25s} {st['n']:>3d} {st['w']:>3d} {st['l']:>3d}  "
              f"{st['wr']:>5.1f}% ${st['pnl']:>+8,.0f}  {st['pf']:>5.2f} "
              f"${max_dd:>6,.0f} ${per_day:>+5,.0f}  {losing_days}/{len(sorted_days)}")

    # ═══════════════════════════════════════════════════════════════
    # PART 10: THE 4 IMPROVEMENTS — disp_fvg, flat 1.3 RR baseline
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print("THE 4 IMPROVEMENTS — disp_fvg + flat 1.3 RR baseline")
    print(f"{'=' * 80}")

    # Build baseline: disp_fvg, Mode 2, flat 1.3 RR
    base_sigs = []
    for raw in all_raw:
        sig = apply_entry_mode(
            raw, MODE_CLOSE_ENTRY, b1, b5, raw["_ds5"],
            raw["_liq"], dr15, b15, raw["_date"],
        )
        if sig and sig["zone"] == "disp_fvg":
            sig["rr"] = 1.3
            base_sigs.append(sig)

    base_trades = simulate_ticks_full(base_sigs, tick_p, tick_t)
    bs = calc_stats(base_trades)
    print(f"\n  BASELINE: {bs['n']} trades | {bs['w']}W/{bs['l']}L | "
          f"WR {bs['wr']:.1f}% | P&L ${bs['pnl']:+,.0f} | PF {bs['pf']:.2f}")

    # ── 1. ENTRY PRICE — How far from zone are we? ──
    print(f"\n{'─' * 70}")
    print("  1. ENTRY PRICE — distance from zone edge at entry")
    print(f"{'─' * 70}")

    base_wins = [t for t in base_trades if t["result"] == "WIN"]
    base_losses = [t for t in base_trades if t["result"] == "LOSS"]

    avg_dist_w = sum(t.get("dist_from_zone", 0) for t in base_wins) / len(base_wins) if base_wins else 0
    avg_dist_l = sum(t.get("dist_from_zone", 0) for t in base_losses) / len(base_losses) if base_losses else 0
    print(f"  Avg entry distance — Wins: {avg_dist_w:.1f}pt | Losses: {avg_dist_l:.1f}pt")

    # What if we capped max entry distance from zone?
    print(f"\n  Max entry distance cap (skip trades too far from zone):")
    print(f"  {'Max Dist':<12} {'Tr':>4} {'W':>3} {'L':>3} {'WR%':>6} "
          f"{'P&L':>9} {'PF':>6}")
    print(f"  {'─' * 50}")
    for max_dist in [3, 5, 8, 10, 15, 99]:
        filt_sigs = [s for s in base_sigs if s.get("dist_from_zone", 0) <= max_dist]
        filt_trades = simulate_ticks_full(filt_sigs, tick_p, tick_t)
        if not filt_trades:
            continue
        fs = calc_stats(filt_trades)
        label = f"≤{max_dist}pt" if max_dist < 99 else "all"
        print(f"  {label:<12} {fs['n']:>4} {fs['w']:>3} {fs['l']:>3} {fs['wr']:>5.1f}% "
              f"${fs['pnl']:>+8,.0f} {fs['pf']:>6.2f}")

    # Tick-level limit order simulation: what if we placed limit at zone edge?
    print(f"\n  LIMIT ORDER AT ZONE EDGE — tick simulation:")
    print(f"  Place limit at zone_top+1 (bull) / zone_bot-1 (bear) after signal.")
    print(f"  Check if tick data shows a fill within 5 minutes.")
    limit_results = []
    for s in base_sigs:
        entry_ns = int(s["time"].timestamp() * 1e9)
        window_ns = entry_ns + 5 * 60 * 1_000_000_000  # 5 min window
        idx0 = np.searchsorted(tick_t, entry_ns, side='right')
        idx1 = np.searchsorted(tick_t, window_ns, side='right')
        segment_p = tick_p[idx0:idx1]
        segment_t = tick_t[idx0:idx1]
        if len(segment_p) == 0:
            continue

        if s["side"] == "bull":
            limit_price = s["zone_top"] + 1.0 + SLIP
            fill_idx = np.where(segment_p <= s["zone_top"] + 1.0)[0]
        else:
            limit_price = s["zone_bot"] - 1.0 - SLIP
            fill_idx = np.where(segment_p >= s["zone_bot"] - 1.0)[0]

        if len(fill_idx) == 0:
            limit_results.append({"filled": False})
            continue

        # Filled! Now simulate from fill point
        fill_tick_idx = idx0 + fill_idx[0]
        fill_ns = tick_t[fill_tick_idx]
        ep = limit_price
        sp = s["stop"]
        risk = abs(ep - sp)
        if risk <= 0:
            continue
        tp = ep + risk * 1.3 if s["side"] == "bull" else ep - risk * 1.3

        post_p = tick_p[fill_tick_idx + 1:]
        post_t = tick_t[fill_tick_idx + 1:]
        if len(post_p) == 0:
            continue

        if s["side"] == "bull":
            sh = np.where(post_p <= sp)[0]
            th = np.where(post_p >= tp)[0]
        else:
            sh = np.where(post_p >= sp)[0]
            th = np.where(post_p <= tp)[0]

        fs_i = sh[0] if len(sh) else len(post_p)
        ft_i = th[0] if len(th) else len(post_p)

        if fs_i < ft_i:
            pnl = -risk * PV * CONTRACTS - FEES_RT
            result = "LOSS"
        elif ft_i < fs_i:
            pnl = risk * 1.3 * PV * CONTRACTS - FEES_RT
            result = "WIN"
        else:
            pnl = -FEES_RT
            result = "OPEN"

        improvement = ep - s["entry"] if s["side"] == "bull" else s["entry"] - ep
        limit_results.append({
            "filled": True, "result": result, "pnl": pnl,
            "improvement": improvement, "date": s["date"],
        })

    filled = [r for r in limit_results if r["filled"]]
    not_filled = [r for r in limit_results if not r["filled"]]
    if filled:
        lw = sum(1 for r in filled if r["result"] == "WIN")
        ll = sum(1 for r in filled if r["result"] == "LOSS")
        lpnl = sum(r["pnl"] for r in filled)
        gw = sum(r["pnl"] for r in filled if r["result"] == "WIN")
        gl = abs(sum(r["pnl"] for r in filled if r["result"] == "LOSS"))
        lpf = gw / gl if gl > 0 else float('inf')
        avg_imp = sum(r["improvement"] for r in filled) / len(filled)
        print(f"  Filled: {len(filled)} / {len(limit_results)} "
              f"({100*len(filled)/len(limit_results):.0f}%)")
        print(f"  Not filled (missed): {len(not_filled)}")
        print(f"  Limit WR: {lw}W/{ll}L = {100*lw/len(filled):.1f}%")
        print(f"  Limit P&L: ${lpnl:+,.0f} | PF {lpf:.2f}")
        print(f"  Avg entry improvement: {avg_imp:.1f}pt better than 1m close")

    # ── 2. STOP PLACEMENT — are stops too wide? ──
    print(f"\n{'─' * 70}")
    print("  2. STOP PLACEMENT — what's the right stop size?")
    print(f"{'─' * 70}")

    win_stops = [t["risk_pts"] for t in base_wins]
    loss_stops = [t["risk_pts"] for t in base_losses]
    print(f"  Avg stop — Wins: {sum(win_stops)/len(win_stops):.1f}pt | "
          f"Losses: {sum(loss_stops)/len(loss_stops):.1f}pt")
    print(f"  Median stop — Wins: {sorted(win_stops)[len(win_stops)//2]:.1f}pt | "
          f"Losses: {sorted(loss_stops)[len(loss_stops)//2]:.1f}pt")

    # Sim with max stop cap
    print(f"\n  Max stop cap (skip trades with stop > Xpt):")
    print(f"  {'Max Stop':<12} {'Tr':>4} {'W':>3} {'L':>3} {'WR%':>6} "
          f"{'P&L':>9} {'PF':>6} {'MaxDD':>8}")
    print(f"  {'─' * 60}")
    for max_stop in [5, 8, 10, 12, 15, 99]:
        filt_sigs = [s for s in base_sigs if s["risk_pts"] <= max_stop]
        filt_trades = simulate_ticks_full(filt_sigs, tick_p, tick_t)
        if not filt_trades:
            continue
        fs = calc_stats(filt_trades)
        # Quick DD
        dpnl = defaultdict(float)
        for t in filt_trades:
            dpnl[t["date"]] += t["pnl"]
        eq = []
        r = 0.0
        for d in sorted(dpnl.keys()):
            r += dpnl[d]
            eq.append(r)
        pk = 0.0
        mdd = 0.0
        for e in eq:
            if e > pk: pk = e
            if pk - e > mdd: mdd = pk - e
        label = f"≤{max_stop}pt" if max_stop < 99 else "all"
        print(f"  {label:<12} {fs['n']:>4} {fs['w']:>3} {fs['l']:>3} {fs['wr']:>5.1f}% "
              f"${fs['pnl']:>+8,.0f} {fs['pf']:>6.2f} ${mdd:>7,.0f}")

    # ── 3. ZONE FRESHNESS — first touch vs retest ──
    print(f"\n{'─' * 70}")
    print("  3. ZONE FRESHNESS — first touch vs retest in session")
    print(f"{'─' * 70}")

    # Track zone touches per day
    zone_touch_count = defaultdict(int)
    for t in sorted(base_trades, key=lambda x: x["time"]):
        zk = (t["date"], t["zone_top"], t["zone_bot"])
        zone_touch_count[id(t)] = zone_touch_count.get(zk, 0)
        # Increment AFTER so first touch = 0
        current = zone_touch_count.get(zk, 0)
        zone_touch_count[zk] = current + 1
        t["_touch_num"] = current  # 0 = first touch

    first_touch = [t for t in base_trades if t.get("_touch_num", 0) == 0]
    retests = [t for t in base_trades if t.get("_touch_num", 0) > 0]

    for label, group in [("First touch", first_touch), ("Retest", retests)]:
        if not group:
            print(f"  {label}: 0 trades")
            continue
        w = sum(1 for t in group if t["result"] == "WIN")
        l = sum(1 for t in group if t["result"] == "LOSS")
        pnl = sum(t["pnl"] for t in group)
        gw = sum(t["pnl"] for t in group if t["result"] == "WIN")
        gl = abs(sum(t["pnl"] for t in group if t["result"] == "LOSS"))
        wr = 100 * w / len(group) if group else 0
        pf = gw / gl if gl > 0 else float('inf')
        print(f"  {label:<15} {len(group):>4} trades | {w}W/{l}L | "
              f"WR {wr:.1f}% | P&L ${pnl:+,.0f} | PF {pf:.2f}")

    # ── 4. HIGHER TIMEFRAME CONTEXT ──
    print(f"\n{'─' * 70}")
    print("  4. HIGHER TIMEFRAME CONTEXT — score as HTF proxy")
    print(f"{'─' * 70}")
    print(f"  Score includes: rej(+1) cisd(+1) dbl_sweep(+2) 15m_struct(+2) 15m_sweep(+1)")

    # Re-derive HTF components for each trade from raw entries
    # Match trades back to raw entries by time
    raw_by_ns = {}
    for raw in all_raw:
        raw_by_ns[raw["ns"]] = raw

    htf_aligned = []
    htf_not = []
    cisd_aligned = []
    cisd_not = []

    for t in base_trades:
        entry_ns = int(t["time"].timestamp() * 1e9)
        # Score >= 4 means at least one major HTF component hit
        # Score >= 5 with rr override means had sweep or 15m struct
        # Since we overrode RR to 1.3, original score still reflects confluences
        sc = t["score"]
        if sc >= 4:
            htf_aligned.append(t)
        else:
            htf_not.append(t)

    for label, group in [("Score >= 4 (HTF aligned)", htf_aligned),
                          ("Score < 4 (no HTF)", htf_not)]:
        if not group:
            print(f"  {label}: 0 trades")
            continue
        w = sum(1 for t in group if t["result"] == "WIN")
        l = sum(1 for t in group if t["result"] == "LOSS")
        pnl = sum(t["pnl"] for t in group)
        gw = sum(t["pnl"] for t in group if t["result"] == "WIN")
        gl = abs(sum(t["pnl"] for t in group if t["result"] == "LOSS"))
        wr = 100 * w / len(group) if group else 0
        pf = gw / gl if gl > 0 else float('inf')
        print(f"  {label:<30} {len(group):>4} trades | {w}W/{l}L | "
              f"WR {wr:.1f}% | P&L ${pnl:+,.0f} | PF {pf:.2f}")

    # Score breakdown at 1.3 RR
    wr_breakdown(base_trades, lambda t: t["score"], "DISP_FVG 1.3RR BY SCORE")

    # ── COMBINED: What if we stack improvements? ──
    print(f"\n{'─' * 70}")
    print("  STACKING IMPROVEMENTS")
    print(f"{'─' * 70}")
    combos = [
        ("BASELINE (disp_fvg, 1.3RR)", lambda s: True),
        ("+ drop ≤3pt stops", lambda s: s["risk_pts"] > 3),
        ("+ drop >15pt stops", lambda s: s["risk_pts"] <= 15),
        ("+ stop 3-15pt", lambda s: 3 < s["risk_pts"] <= 15),
        ("+ stop 3-12pt", lambda s: 3 < s["risk_pts"] <= 12),
        ("+ dist ≤10pt from zone", lambda s: s.get("dist_from_zone", 0) <= 10),
        ("+ dist ≤8pt from zone", lambda s: s.get("dist_from_zone", 0) <= 8),
        ("stop 3-15 + dist≤10", lambda s: 3 < s["risk_pts"] <= 15 and s.get("dist_from_zone", 0) <= 10),
        ("stop 3-12 + dist≤10", lambda s: 3 < s["risk_pts"] <= 12 and s.get("dist_from_zone", 0) <= 10),
        ("stop 3-12 + dist≤8", lambda s: 3 < s["risk_pts"] <= 12 and s.get("dist_from_zone", 0) <= 8),
    ]
    print(f"  {'Filter':<30} {'Tr':>4} {'W':>3} {'L':>3} {'WR%':>6} "
          f"{'P&L':>9} {'PF':>6} {'MaxDD':>8} {'$/day':>7}")
    print(f"  {'─' * 80}")
    for name, fn in combos:
        filt_sigs = [s for s in base_sigs if fn(s)]
        filt_trades = simulate_ticks_full(filt_sigs, tick_p, tick_t)
        if not filt_trades:
            print(f"  {name:<30}    0")
            continue
        fs = calc_stats(filt_trades)
        dpnl = defaultdict(float)
        for t in filt_trades:
            dpnl[t["date"]] += t["pnl"]
        eq = []
        r = 0.0
        for d in sorted(dpnl.keys()):
            r += dpnl[d]
            eq.append(r)
        pk = 0.0
        mdd = 0.0
        for e in eq:
            if e > pk: pk = e
            if pk - e > mdd: mdd = pk - e
        days_active = len(set(t["date"] for t in filt_trades))
        dpd = fs['pnl'] / days_active if days_active else 0
        print(f"  {name:<30} {fs['n']:>4} {fs['w']:>3} {fs['l']:>3} {fs['wr']:>5.1f}% "
              f"${fs['pnl']:>+8,.0f} {fs['pf']:>6.2f} ${mdd:>7,.0f} ${dpd:>+6,.0f}")

    # ═══════════════════════════════════════════════════════════════
    # PART 11: iFVG AUTOPSY — why are they 29% WR?
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print("iFVG AUTOPSY — 29% WR, what's broken?")
    print(f"{'=' * 80}")

    # Get iFVG trades at 1.3 RR
    ifvg_sigs = []
    for raw in all_raw:
        sig = apply_entry_mode(
            raw, MODE_CLOSE_ENTRY, b1, b5, raw["_ds5"],
            raw["_liq"], dr15, b15, raw["_date"],
        )
        if sig and sig["zone"] == "ifvg":
            sig["rr"] = 1.3
            ifvg_sigs.append(sig)

    ifvg_trades = simulate_ticks_full(ifvg_sigs, tick_p, tick_t)

    # Also get raw entries for iFVGs to check zone properties
    ifvg_raws = [r for r in all_raw if r["zt"] == "ifvg"]

    print(f"\n  iFVG signals: {len(ifvg_sigs)} | iFVG trades: {len(ifvg_trades)}")
    ifvg_w = [t for t in ifvg_trades if t["result"] == "WIN"]
    ifvg_l = [t for t in ifvg_trades if t["result"] == "LOSS"]
    print(f"  {len(ifvg_w)}W / {len(ifvg_l)}L = {100*len(ifvg_w)/len(ifvg_trades):.1f}% WR" if ifvg_trades else "")

    # Zone width analysis
    print(f"\n  ── iFVG ZONE WIDTH ──")
    for lo, hi, label in [(0,2,"≤2pt"),(2,5,"2-5pt"),(5,10,"5-10pt"),(10,50,">10pt")]:
        group = [t for t in ifvg_trades if lo < abs(t["zone_top"]-t["zone_bot"]) <= hi or (lo==0 and abs(t["zone_top"]-t["zone_bot"])<=hi)]
        if not group: continue
        w = sum(1 for t in group if t["result"] == "WIN")
        print(f"  {label:<10} {len(group):>3} trades | {w}W/{len(group)-w}L | "
              f"WR {100*w/len(group):.0f}%")

    # Distance from zone
    print(f"\n  ── iFVG ENTRY DISTANCE FROM ZONE ──")
    for lo, hi, label in [(0,3,"≤3pt"),(3,8,"3-8pt"),(8,15,"8-15pt"),(15,99,">15pt")]:
        group = [t for t in ifvg_trades if lo < t.get("dist_from_zone",0) <= hi or (lo==0 and t.get("dist_from_zone",0)<=hi)]
        if not group: continue
        w = sum(1 for t in group if t["result"] == "WIN")
        print(f"  {label:<10} {len(group):>3} trades | {w}W/{len(group)-w}L | "
              f"WR {100*w/len(group):.0f}%")

    # Every iFVG trade with zone details
    print(f"\n  ── EVERY iFVG TRADE ──")
    print(f"  {'Date':>10} {'Time':>5} {'Side':>4} {'Entry':>9} {'ZoneT':>9} {'ZoneB':>9} "
          f"{'Width':>5} {'Dist':>5} {'Res':>4} {'P&L':>8}")
    print(f"  {'─' * 80}")
    for t in sorted(ifvg_trades, key=lambda x: x["time"]):
        zw = abs(t["zone_top"] - t["zone_bot"])
        dist = t.get("dist_from_zone", 0)
        print(f"  {t['date']} {t['time'].strftime('%H:%M'):>5} {t['side']:>4} "
              f"{t['entry']:>9.2f} {t['zone_top']:>9.2f} {t['zone_bot']:>9.2f} "
              f"{zw:>4.1f}pt {dist:>4.1f}pt {t['result']:>4} ${t['pnl']:>+7,.0f}")

    print(f"\n{'=' * 80}")
    print(f"Completed in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
