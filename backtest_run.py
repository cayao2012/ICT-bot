#!/usr/bin/env python3
"""
STANDALONE BACKTEST RUNNER — V106 ICT Strategy
================================================
Loads tick data from data_new/ (numpy arrays) + bar cache (pickle).
Uses our signal generation code (backtest_entry_modes.py) with:
  - PB Trading iFVG fix (IFVG_INV_CLEAR=0.5)
  - Flat 1.1 RR on all trades
  - Both zone types (disp_fvg + ifvg)
  - All scores (no filter)

This is the exact code that produced:
  182 trades | 141W/41L | 77.5% WR | $69,852 P&L | PF 3.35 | $760 MaxDD

HOW TO RUN:
  python3 backtest_run.py

DATA REQUIRED (in data_new/):
  - NQ_prices.npy  — tick prices (float64 array)
  - NQ_times.npy   — tick timestamps in nanoseconds (int64 array)
  - bars_cache.pkl  — dict with keys 'b1', 'b5', 'b15' (lists of bar dicts)

Each bar dict must have:
  {'time_ns': int, 'open': float, 'high': float, 'low': float,
   'close': float, 'date': datetime.date, 'hour': int, 'minute': int}
"""
import os, sys, time, pickle
import numpy as np
from datetime import datetime, timedelta, date as dt_date
from zoneinfo import ZoneInfo
from collections import defaultdict, OrderedDict

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))

CT = ZoneInfo("America/Chicago")

# ── Strategy constants (MUST match live bot) ──
PV = int(os.environ.get("BACKTEST_PV", "20"))
CONTRACTS = int(os.environ.get("BACKTEST_CONTRACTS", "3"))
FEES_RT = float(os.environ.get("BACKTEST_FEES", "8.40"))
MAX_RISK = 1000
COOLDOWN_S = 120
MCL = 3
SLIP = float(os.environ.get("BACKTEST_SLIP", "0.5"))
DLL = -2000
GMCL = int(os.environ.get("BACKTEST_GMCL", "2"))
SIM_START_HOUR = int(os.environ.get("BACKTEST_KZ_START_HOUR", "7"))
SIM_START_MIN = int(os.environ.get("BACKTEST_KZ_START_MIN", "30"))
SIM_END_HOUR = int(os.environ.get("BACKTEST_KZ_END_HOUR", "14"))
SIM_END_MIN = int(os.environ.get("BACKTEST_KZ_END_MIN", "30"))

# ── Best config ──
FLAT_RR = 1.1
ZONE_FILTER = ("disp_fvg", "ifvg")  # both zone types
MIN_SCORE = 1  # no score filter (take all)

DATA_DIR = os.environ.get("BACKTEST_DATA_DIR", os.path.join(os.path.dirname(__file__), "data_new"))
BARS_FILE = os.environ.get("BACKTEST_BARS_FILE", os.path.join(DATA_DIR, "bars_cache.pkl"))


# ═══════════════════════════════════════════════════════════════
# TICK OUTCOME — nanosecond precision stop vs target
# ═══════════════════════════════════════════════════════════════
def outcome_tick(sig, tick_p, tick_t):
    """Determine WIN/LOSS using tick data. Forward-only from entry."""
    entry_ns = int(sig["time"].timestamp() * 1e9)
    side = sig["side"]
    ep, sp = sig["entry"], sig["stop"]
    risk = sig["risk_pts"]
    tp = ep + risk * sig["rr"] if side == "bull" else ep - risk * sig["rr"]

    idx0 = np.searchsorted(tick_t, entry_ns, side='right')  # first tick AFTER entry
    # Scope to ~8 hours (trades resolve same session, 390min timeout max)
    eod_ns = entry_ns + 8 * 3_600_000_000_000
    idx_end = np.searchsorted(tick_t, eod_ns, side='right')
    post = tick_p[idx0:idx_end]

    if len(post) == 0:
        return "OPEN", -FEES_RT, tp, 0

    if side == "bull":
        sh = np.where(post <= sp)[0]   # stop hit
        th = np.where(post >= tp)[0]   # target hit
    else:
        sh = np.where(post >= sp)[0]
        th = np.where(post <= tp)[0]

    fs = sh[0] if len(sh) else len(post)
    ft = th[0] if len(th) else len(post)

    if fs < ft:  # stop hit first
        exit_ns = tick_t[idx0 + fs]
        if side == "bull":
            return "LOSS", (sp - ep) * PV * CONTRACTS - FEES_RT, tp, exit_ns
        else:
            return "LOSS", (ep - sp) * PV * CONTRACTS - FEES_RT, tp, exit_ns
    elif ft < fs:  # target hit first
        exit_ns = tick_t[idx0 + ft]
        if side == "bull":
            return "WIN", (tp - ep) * PV * CONTRACTS - FEES_RT, tp, exit_ns
        else:
            return "WIN", (ep - tp) * PV * CONTRACTS - FEES_RT, tp, exit_ns

    return "OPEN", -FEES_RT, tp, 0


# ═══════════════════════════════════════════════════════════════
# FULL SIMULATION — with cooldown, MCL, zone dedup (matches live bot)
# ═══════════════════════════════════════════════════════════════
def simulate_ticks_full(sigs, tick_p, tick_t):
    """Simulate with cooldown/MCL/GMCL/DLL/zone dedup using tick data."""
    sigs = sorted(sigs, key=lambda x: x["time"])
    trades = []
    current_day = None
    in_pos = False
    pos_exit_ns = 0
    cool_ns = 0
    cl_b = cl_r = 0
    cl_global = 0
    day_pnl = 0.0
    day_done = False
    used = set()

    for s in sigs:
        if s["date"] != current_day:
            current_day = s["date"]
            in_pos = False; pos_exit_ns = 0; cool_ns = 0
            cl_b = cl_r = 0; cl_global = 0
            day_pnl = 0.0; day_done = False; used = set()

        if day_done:
            continue

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
        if cl_global >= GMCL:
            day_done = True
            continue

        result, pnl, tp, exit_ns = outcome_tick(s, tick_p, tick_t)

        used.add(zk)
        if exit_ns > 0:
            exit_time = datetime.fromtimestamp(exit_ns / 1e9, tz=CT)
            in_pos = True
            pos_exit_ns = exit_ns
            cool_ns = exit_ns + COOLDOWN_S * 1_000_000_000
        else:
            exit_time = None
            in_pos = True
            pos_exit_ns = int((datetime.now(CT) + timedelta(hours=24)).timestamp() * 1e9)

        if result == "LOSS":
            if s["side"] == "bull": cl_b += 1
            else: cl_r += 1
            cl_global += 1
        elif result == "WIN":
            if s["side"] == "bull": cl_b = 0
            else: cl_r = 0
            cl_global = 0

        day_pnl += pnl
        trades.append({
            **s, "target": tp, "result": result,
            "exit_time": exit_time, "pnl": pnl,
        })

        if day_pnl <= DLL:
            day_done = True

    return trades


# ═══════════════════════════════════════════════════════════════
# STATS
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


def calc_drawdown(trades):
    """Calculate max drawdown from trade list."""
    dpnl = defaultdict(float)
    for t in trades:
        dpnl[t["date"]] += t["pnl"]
    sorted_days = sorted(dpnl.keys())
    if not sorted_days:
        return 0, 0, 0
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
    return max_dd, losing_days, len(sorted_days)


# ═══════════════════════════════════════════════════════════════
# BUILD DATE RANGES (same as backtest_topstep.build_dr)
# ═══════════════════════════════════════════════════════════════
def build_dr(bars):
    """Build date range index: {date: (start_idx, end_idx)}.
    end_idx is EXCLUSIVE (Python slice style: last_bar_idx + 1).
    Matches ptnut_bot._build_dr and backtest_topstep.build_dr so that
    range(ds, de) in get_liquidity_levels covers all session bars.
    """
    dr = {}
    for i, bar in enumerate(bars):
        d = bar["date"]
        if d not in dr:
            dr[d] = (i, i + 1)
        else:
            dr[d] = (dr[d][0], i + 1)
    return dr


def build_dr_htf(bars):
    """Build HTF date range index (same logic)."""
    return build_dr(bars)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    from v106_dynamic_rr_zone_entry import (
        get_liquidity_levels, NS_MIN,
    )
    from backtest_entry_modes import (
        gen_sweep_entries_enriched, apply_entry_mode,
        MODE_CLOSE_ENTRY,
    )

    t0 = time.time()
    print("=" * 80)
    print("BACKTEST RUNNER — V106 ICT Strategy")
    print(f"  Config: flat {FLAT_RR} RR | zones: {ZONE_FILTER} | min score: {MIN_SCORE}")
    print(f"  Constants: PV={PV} CONTRACTS={CONTRACTS} FEES={FEES_RT} SLIP={SLIP}")
    print(f"  Cooldown={COOLDOWN_S}s MCL={MCL} MaxRisk=${MAX_RISK}")
    print("=" * 80)

    # ── Load tick data from numpy ──
    print("\nLoading tick data from data_new/...")
    # Load tick data — supports both npz (compressed) and separate npy files
    npz_path = os.path.join(DATA_DIR, "NQ_ticks.npz")
    if os.path.exists(npz_path):
        ticks = np.load(npz_path)
        tick_p, tick_t = ticks["prices"], ticks["times"]
    else:
        tick_p = np.load(os.path.join(DATA_DIR, "NQ_prices.npy"))
        tick_t = np.load(os.path.join(DATA_DIR, "NQ_times.npy"))
    print(f"  Ticks: {len(tick_p):,}")

    # Determine date range from timestamps
    t_first = datetime.fromtimestamp(tick_t[0] / 1e9, tz=CT)
    t_last = datetime.fromtimestamp(tick_t[-1] / 1e9, tz=CT)
    print(f"  Range: {t_first.date()} → {t_last.date()}")
    print(f"  Price: {tick_p.min():.2f} – {tick_p.max():.2f}")

    # ── Load bar cache ──
    print("\nLoading bar cache...")
    with open(BARS_FILE, "rb") as f:
        cache = pickle.load(f)
    b5 = cache["b5"]
    b1 = cache["b1"]
    b15 = cache["b15"]
    print(f"  5m: {len(b5)} | 1m: {len(b1)} | 15m: {len(b15)}")

    # ── Build date ranges ──
    dr5 = build_dr(b5)
    dr15 = build_dr_htf(b15)
    all_dates = sorted(dr5.keys())
    print(f"  Trading dates in bars: {len(all_dates)} "
          f"({all_dates[0]} → {all_dates[-1]})")

    # ── Filter to dates that have BOTH tick data AND bar data ──
    # Build set of dates in tick data
    tick_dates = set()
    for i in range(0, len(tick_t), max(1, len(tick_t) // 10000)):
        d = datetime.fromtimestamp(tick_t[i] / 1e9, tz=CT).date()
        tick_dates.add(d)
    # Also check boundaries
    tick_dates.add(datetime.fromtimestamp(tick_t[0] / 1e9, tz=CT).date())
    tick_dates.add(datetime.fromtimestamp(tick_t[-1] / 1e9, tz=CT).date())

    trade_dates = [d for d in all_dates if d in tick_dates and d.weekday() < 5]
    start_date_str = os.environ.get("BACKTEST_START_DATE")
    if start_date_str:
        from datetime import date as _date
        sd = _date.fromisoformat(start_date_str)
        trade_dates = [d for d in trade_dates if d >= sd]
        print(f"  Filtered to dates >= {sd}")
    print(f"  Dates with both tick + bar data: {len(trade_dates)} "
          f"({trade_dates[0] if trade_dates else '?'} → "
          f"{trade_dates[-1] if trade_dates else '?'})")

    # ── Generate enriched signals (bar-by-bar, no look-ahead) ──
    # Day-scoped b1: only pass current day's bars (other Claude's optimization)
    b1_times = [b["time_ns"] for b in b1]
    print("\nGenerating signals (bar-by-bar, b5[:cursor+1] slicing)...")
    all_raw = []
    for di, d in enumerate(trade_dates):
        ds5, de5 = dr5[d]
        liq = get_liquidity_levels(b5, dr5, d, all_dates)

        # Day-scoped b1 start: 1hr before session
        sess_ns = b5[ds5]["time_ns"]
        b1_day_start = 0
        for _i in range(len(b1_times)):
            if b1_times[_i] >= sess_ns - 3600_000_000_000:
                b1_day_start = _i
                break

        seen_ns = set()
        b1_cutoff = b1_day_start
        for cursor in range(ds5 + 1, de5):  # de5 is exclusive (last_idx+1)
            # CRITICAL: b1_cutoff = next 5m bar boundary (no look-ahead)
            if cursor < de5:
                next_ns = b5[cursor]["time_ns"] + 5 * NS_MIN
            else:
                next_ns = b5[cursor - 1]["time_ns"] + 10 * NS_MIN
            while b1_cutoff < len(b1) and b1[b1_cutoff]["time_ns"] < next_ns:
                b1_cutoff += 1

            # CRITICAL: b5[:cursor + 1] — only bars up to current cursor
            # Pass killzone to signal gen so it matches the backtest session filter
            sim_kz = [((SIM_START_HOUR, SIM_START_MIN), (SIM_END_HOUR, SIM_END_MIN))]
            ents = gen_sweep_entries_enriched(
                b5[:cursor + 1], b1[b1_day_start:b1_cutoff], ds5, cursor, d, liq, kz=sim_kz)

            for e in sorted(ents, key=lambda x: (x["ns"],
                            -{"ifvg": 2, "disp_fvg": 1}.get(x["zt"], 0))):
                if e["ns"] in seen_ns:
                    continue
                _et = datetime.fromtimestamp(e["ns"] / 1e9, tz=CT)
                # Killzone filter (handles overnight wraps like 17:00→02:00)
                t_min = _et.hour * 60 + _et.minute
                kz_start = SIM_START_HOUR * 60 + SIM_START_MIN
                kz_end = SIM_END_HOUR * 60 + SIM_END_MIN
                if kz_start < kz_end:
                    if t_min < kz_start or t_min >= kz_end:
                        continue
                else:  # overnight wrap
                    if t_min < kz_start and t_min >= kz_end:
                        continue
                seen_ns.add(e["ns"])
                e["_date"] = d
                e["_ds5"] = ds5
                e["_liq"] = liq
                all_raw.append(e)

        if (di + 1) % 50 == 0 or di == len(trade_dates) - 1:
            print(f"  Day {di+1}/{len(trade_dates)}: {len(all_raw)} raw signals so far")

    print(f"\n  Total raw entries: {len(all_raw)}")
    n_ifvg = sum(1 for e in all_raw if e["zt"] == "ifvg")
    n_dfvg = sum(1 for e in all_raw if e["zt"] == "disp_fvg")
    print(f"  disp_fvg: {n_dfvg} | iFVG: {n_ifvg}")

    # ── Apply entry mode + flat RR ──
    USE_LIQ_TARGET = os.environ.get("BACKTEST_LIQ_TARGET") == "1"
    REQ_CISD = os.environ.get("BACKTEST_REQ_CISD") == "1"        # disp_fvg needs CISD
    REQ_STRUCT = os.environ.get("BACKTEST_REQ_STRUCT") == "1"     # disp_fvg needs 15m struct
    REQ_ANY_CONF = os.environ.get("BACKTEST_REQ_ANY_CONF") == "1" # disp_fvg needs at least 1 confluence
    SKIP_SCORES = set(int(x) for x in os.environ.get("BACKTEST_SKIP_SCORES", "").split(",") if x.strip())
    USE_VAR_RR = os.environ.get("BACKTEST_VAR_RR") == "1"         # variable RR by confluence
    print(f"\nApplying Mode 2 (1m Close) entry + flat {FLAT_RR} RR...")
    if USE_LIQ_TARGET:
        print("  + Liquidity target filter ON")
    sigs = []
    for raw in all_raw:
        sig = apply_entry_mode(
            raw, MODE_CLOSE_ENTRY, b1, b5, raw["_ds5"],
            raw["_liq"], dr15, b15, raw["_date"],
        )
        if sig and sig["zone"] in ZONE_FILTER and sig.get("score", 0) >= MIN_SCORE and sig.get("score", 0) not in SKIP_SCORES:
            # IFVG delayed start: skip IFVGs before 9:00 CT (early morning = noise)
            if os.environ.get("BACKTEST_IFVG_DELAY", "1") == "1" and sig["zone"] == "ifvg":
                sig_hour = sig["time"].hour
                if sig_hour < 9:
                    continue
            # Directional filter: trade must align with 15m structure
            if os.environ.get("BACKTEST_HTF_DIRECTION") == "1":
                struct = sig.get("has_struct")
                if struct is not None and not struct:
                    # 15m structure exists but doesn't match trade direction = skip
                    continue
            # Liquidity target filter: must have a liq level in trade direction
            if USE_LIQ_TARGET:
                ep = sig["entry"]
                liq = raw["_liq"]
                has_target = False
                if sig["side"] == "bull":
                    has_target = any(lvl > ep + 3.0 for lvl, _ in liq)
                else:
                    has_target = any(lvl < ep - 3.0 for lvl, _ in liq)
                if not has_target:
                    continue
            # Confluence filters for disp_fvg
            if sig["zone"] == "disp_fvg":
                if REQ_CISD and not sig.get("has_cisd"):
                    continue
                if REQ_STRUCT and not sig.get("has_struct"):
                    continue
                if REQ_ANY_CONF and not (sig.get("has_cisd") or sig.get("has_struct") or sig.get("has_rej")):
                    continue
            # RR by score: 6+ = 1.5R, 2 = 1.3R, rest = 1.1R
            if USE_VAR_RR:
                sc = sig.get("score", 1)
                if sc >= 6:
                    sig["rr"] = 1.5
                elif sc == 2:
                    sig["rr"] = 1.3
                else:
                    sig["rr"] = 1.1
            else:
                sig["rr"] = FLAT_RR
            sigs.append(sig)
    print(f"  Signals after filter: {len(sigs)}")

    # ── Run tick simulation ──
    print("\nRunning tick-level simulation...")
    trades = simulate_ticks_full(sigs, tick_p, tick_t)
    st = calc_stats(trades)
    max_dd, losing_days, total_days = calc_drawdown(trades)

    # ═══════════════════════════════════════════════════════════════
    # RESULTS
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print(f"RESULTS — flat {FLAT_RR} RR, {'+'.join(ZONE_FILTER)}, score >= {MIN_SCORE}")
    print(f"{'=' * 80}")
    print(f"  Trades:     {st['n']} ({st['w']}W / {st['l']}L)")
    print(f"  Win Rate:   {st['wr']:.1f}%")
    print(f"  P&L:        ${st['pnl']:+,.0f}")
    print(f"  Avg Win:    ${st['avg_w']:+,.0f}")
    print(f"  Avg Loss:   ${st['avg_l']:+,.0f}")
    print(f"  PF:         {st['pf']:.2f}")
    print(f"  Max DD:     ${max_dd:,.0f}")
    print(f"  Losing Days:{losing_days} / {total_days}")
    if total_days > 0:
        print(f"  $/day:      ${st['pnl']/total_days:+,.0f}")
        print(f"  $/trade:    ${st['pnl']/st['n']:+,.0f}" if st['n'] else "")

    # ── By zone type ──
    print(f"\n  ── BY ZONE TYPE ──")
    for zt in ZONE_FILTER:
        zt_trades = [t for t in trades if t["zone"] == zt]
        if not zt_trades:
            continue
        zs = calc_stats(zt_trades)
        print(f"  {zt:12s}: {zs['n']} trades | {zs['wr']:.1f}% WR | "
              f"${zs['pnl']:+,.0f} P&L | PF {zs['pf']:.2f}")

    # ── By side ──
    print(f"\n  ── BY SIDE ──")
    for side in ["bull", "bear"]:
        s_trades = [t for t in trades if t["side"] == side]
        if not s_trades:
            continue
        ss = calc_stats(s_trades)
        print(f"  {side:12s}: {ss['n']} trades | {ss['wr']:.1f}% WR | "
              f"${ss['pnl']:+,.0f} P&L | PF {ss['pf']:.2f}")

    # ── By score ──
    print(f"\n  ── BY SCORE ──")
    scores = sorted(set(t["score"] for t in trades))
    for sc in scores:
        sc_trades = [t for t in trades if t["score"] == sc]
        scs = calc_stats(sc_trades)
        print(f"  score={sc:5d}: {scs['n']:>3d} trades | {scs['wr']:.1f}% WR | "
              f"${scs['pnl']:+,.0f} P&L")

    # ── Daily P&L ──
    print(f"\n  ── DAILY P&L ──")
    dpnl = defaultdict(float)
    dtrades = defaultdict(list)
    for t in trades:
        dpnl[t["date"]] += t["pnl"]
        dtrades[t["date"]].append(t)

    cumul = 0.0
    print(f"  {'Date':12s} {'Trades':>6} {'W':>3} {'L':>3} {'Day P&L':>10} {'Cumul':>10}")
    print(f"  {'─' * 50}")
    for d in sorted(dpnl.keys()):
        dt = dtrades[d]
        w = sum(1 for t in dt if t["result"] == "WIN")
        l = sum(1 for t in dt if t["result"] == "LOSS")
        cumul += dpnl[d]
        marker = " <<<" if dpnl[d] < 0 else ""
        print(f"  {d}  {len(dt):>4}   {w:>3} {l:>3}  ${dpnl[d]:>+9,.0f} ${cumul:>+9,.0f}{marker}")

    # ── Weekly summary ──
    print(f"\n  ── WEEKLY SUMMARY ──")
    weekly_pnl = OrderedDict()
    weekly_trades = OrderedDict()
    for d in sorted(dpnl.keys()):
        yr, wk, _ = d.isocalendar()
        wkey = f"{yr}-W{wk:02d}"
        weekly_pnl[wkey] = weekly_pnl.get(wkey, 0) + dpnl[d]
        weekly_trades[wkey] = weekly_trades.get(wkey, 0) + len(dtrades[d])

    cumul = 0.0
    print(f"  {'Week':10} {'Trades':>6} {'P&L':>10} {'Cumul':>10}")
    print(f"  {'─' * 40}")
    for wk in weekly_pnl:
        cumul += weekly_pnl[wk]
        print(f"  {wk:10} {weekly_trades[wk]:>6} "
              f"${weekly_pnl[wk]:>+9,.0f} ${cumul:>+9,.0f}")

    # ── Monthly summary (for multi-month/year data) ──
    monthly_pnl = OrderedDict()
    monthly_trades = OrderedDict()
    for d in sorted(dpnl.keys()):
        mkey = f"{d.year}-{d.month:02d}"
        monthly_pnl[mkey] = monthly_pnl.get(mkey, 0) + dpnl[d]
        monthly_trades[mkey] = monthly_trades.get(mkey, 0) + len(dtrades[d])

    if len(monthly_pnl) > 1:
        print(f"\n  ── MONTHLY SUMMARY ──")
        cumul = 0.0
        print(f"  {'Month':10} {'Trades':>6} {'P&L':>10} {'Cumul':>10}")
        print(f"  {'─' * 40}")
        for mk in monthly_pnl:
            cumul += monthly_pnl[mk]
            print(f"  {mk:10} {monthly_trades[mk]:>6} "
                  f"${monthly_pnl[mk]:>+9,.0f} ${cumul:>+9,.0f}")

    # ── Yearly summary (for multi-year data) ──
    yearly_pnl = OrderedDict()
    yearly_trades = OrderedDict()
    for d in sorted(dpnl.keys()):
        ykey = str(d.year)
        yearly_pnl[ykey] = yearly_pnl.get(ykey, 0) + dpnl[d]
        yearly_trades[ykey] = yearly_trades.get(ykey, 0) + len(dtrades[d])

    if len(yearly_pnl) > 1:
        print(f"\n  ── YEARLY SUMMARY ──")
        print(f"  {'Year':10} {'Trades':>6} {'P&L':>10}")
        print(f"  {'─' * 30}")
        for yk in yearly_pnl:
            yt = [t for t in trades if str(t["date"].year) == yk]
            ys = calc_stats(yt)
            print(f"  {yk:10} {yearly_trades[yk]:>6} "
                  f"${yearly_pnl[yk]:>+9,.0f}  WR {ys['wr']:.1f}%  PF {ys['pf']:.2f}")

    # ── Detailed trade log ──
    print(f"\n  ── EVERY TRADE ──")
    print(f"  {'Date':>10} {'Time':>5} {'Side':>4} {'Zone':>8} {'Entry':>9} "
          f"{'Stop':>9} {'Target':>9} {'RR':>4} {'Sc':>2} {'Result':>6} {'P&L':>8}")
    print(f"  {'─' * 85}")
    for t in trades:
        print(f"  {t['date']} {t['time'].strftime('%H:%M'):>5} {t['side']:>4} "
              f"{t['zone']:>8} {t['entry']:>9.2f} {t['stop']:>9.2f} "
              f"{t['target']:>9.2f} {t['rr']:>4.1f} {t['score']:>2} "
              f"{t['result']:>6} ${t['pnl']:>+7,.0f}")

    # ── Flat RR comparison (run multiple RRs) ──
    print(f"\n{'=' * 80}")
    print("FLAT RR COMPARISON — both zones, all scores, tick simulation")
    print(f"{'=' * 80}")
    print(f"\n  {'RR':>4} {'Trades':>6} {'W':>4} {'L':>4} {'WR%':>6} "
          f"{'P&L':>10} {'PF':>6} {'MaxDD':>8} {'$/day':>8} {'LoseDays':>10}")
    print(f"  {'─' * 75}")

    for test_rr in [1.0, 1.1, 1.2, 1.3, 1.5, 2.0]:
        test_sigs = []
        for raw in all_raw:
            sig = apply_entry_mode(
                raw, MODE_CLOSE_ENTRY, b1, b5, raw["_ds5"],
                raw["_liq"], dr15, b15, raw["_date"],
            )
            if sig and sig["zone"] in ZONE_FILTER:
                sig["rr"] = test_rr
                test_sigs.append(sig)

        test_trades = simulate_ticks_full(test_sigs, tick_p, tick_t)
        if not test_trades:
            continue
        tst = calc_stats(test_trades)
        mdd, ld, td = calc_drawdown(test_trades)
        pd = tst['pnl'] / td if td else 0

        print(f"  {test_rr:>4.1f} {tst['n']:>6} {tst['w']:>4} {tst['l']:>4} "
              f"{tst['wr']:>5.1f}% ${tst['pnl']:>+9,.0f} {tst['pf']:>6.2f} "
              f"${mdd:>7,.0f} ${pd:>+7,.0f} {ld:>4}/{td}")

    # ── Score filter comparison ──
    print(f"\n{'=' * 80}")
    print(f"SCORE FILTER — flat {FLAT_RR} RR, both zones, tick simulation")
    print(f"{'=' * 80}")
    print(f"\n  {'Filter':>12} {'Trades':>6} {'W':>4} {'L':>4} {'WR%':>6} "
          f"{'P&L':>10} {'PF':>6} {'MaxDD':>8}")
    print(f"  {'─' * 60}")

    for min_sc in [1, 2, 3, 4, 5, 6]:
        test_sigs = []
        for raw in all_raw:
            sig = apply_entry_mode(
                raw, MODE_CLOSE_ENTRY, b1, b5, raw["_ds5"],
                raw["_liq"], dr15, b15, raw["_date"],
            )
            if sig and sig["zone"] in ZONE_FILTER and sig.get("score", 0) >= min_sc:
                sig["rr"] = FLAT_RR
                test_sigs.append(sig)

        test_trades = simulate_ticks_full(test_sigs, tick_p, tick_t)
        if not test_trades:
            continue
        tst = calc_stats(test_trades)
        mdd, _, _ = calc_drawdown(test_trades)

        print(f"  score >= {min_sc:>2} {tst['n']:>6} {tst['w']:>4} {tst['l']:>4} "
              f"{tst['wr']:>5.1f}% ${tst['pnl']:>+9,.0f} {tst['pf']:>6.2f} "
              f"${mdd:>7,.0f}")

    elapsed = time.time() - t0
    print(f"\n{'=' * 80}")
    print(f"Done in {elapsed:.1f}s")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
