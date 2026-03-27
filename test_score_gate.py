#!/usr/bin/env python3
"""
SCORE GATE TEST — Wide-stop disp_fvg trades need extra confluence
=================================================================
Grid: for disp_fvg trades with stop > X pt, require score >= Y.
Tight-stop trades pass unrestricted. iFVG trades kept as-is.

Uses tick_data_1yr/ + .bar_cache/ (same as backtest_tick.py).
"""
import os, sys, time
import glob as globmod
import numpy as np
from datetime import datetime, timedelta, date as dt_date
from zoneinfo import ZoneInfo
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))

CT = ZoneInfo("America/Chicago")
TICK_DIR = os.path.join(os.path.dirname(__file__), "tick_data_1yr")
BAR_CACHE_DIR = os.path.join(os.path.dirname(__file__), ".bar_cache")

PV = 20
CONTRACTS = 3
FEES_RT = 8.40
MAX_RISK = 1000
COOLDOWN_S = 120
MCL = 3
SLIP = 0.5
SIM_START_HOUR = 7
SIM_START_MIN = 30
FLAT_RR = 1.1
DLL = 2000.0


# ── Zero-copy list view (no data copying on cursor steps) ──
class _LV:
    __slots__ = ('_l', '_n')
    def __init__(self, lst, n): self._l = lst; self._n = n
    def __len__(self): return self._n
    def __getitem__(self, i):
        if isinstance(i, slice):
            s, e, st = i.indices(self._n); return self._l[s:e:st]
        if i < 0: i += self._n
        return self._l[i]
    def __iter__(self): return (self._l[i] for i in range(self._n))


def load_ticks(dates):
    import databento as db
    all_prices, all_ts = [], []
    loaded = []
    for d in sorted(dates):
        fname = f"glbx-mdp3-{d.strftime('%Y%m%d')}.trades.dbn.zst"
        fpath = os.path.join(TICK_DIR, fname)
        if not os.path.exists(fpath):
            continue
        store = db.DBNStore.from_file(fpath)
        df = store.to_df()
        nq_syms = [s for s in df['symbol'].unique()
                   if s.startswith('NQ') and '-' not in s and len(s) == 4]
        if not nq_syms:
            continue
        best = max(nq_syms, key=lambda s: len(df[df['symbol'] == s]))
        sub = df[df['symbol'] == best]
        prices = sub['price'].values.astype(np.float64)
        ts_ns = sub.index.view(np.int64)
        all_prices.append(prices)
        all_ts.append(ts_ns)
        loaded.append(d)
    if not all_prices:
        return None, None, []
    return np.concatenate(all_prices), np.concatenate(all_ts), loaded


def load_bar_cache(tf, sd, ed):
    import json
    path = os.path.join(BAR_CACHE_DIR, f"bars_{tf}m_{sd}_{ed}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        bars = json.load(f)
    for b in bars:
        # Fix date
        if "date" not in b or b["date"] is None:
            dt = datetime.fromtimestamp(b["time_ns"] / 1e9, tz=CT)
            b["date"] = dt.date()
        elif isinstance(b["date"], str):
            parts = b["date"].split("-")
            b["date"] = dt_date(int(parts[0]), int(parts[1]), int(parts[2]))
        # Fix hour/minute (used by in_kz)
        if "hour" not in b or "minute" not in b:
            dt = datetime.fromtimestamp(b["time_ns"] / 1e9, tz=CT)
            b["hour"] = dt.hour
            b["minute"] = dt.minute
    return bars


def outcome_tick(sig, tick_p, tick_t):
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
        return "LOSS", ((sp - ep) if side == "bull" else (ep - sp)) * PV * CONTRACTS - FEES_RT, tp, exit_ns
    elif ft < fs:
        exit_ns = tick_t[idx0 + ft]
        return "WIN", ((tp - ep) if side == "bull" else (ep - tp)) * PV * CONTRACTS - FEES_RT, tp, exit_ns
    return "OPEN", -FEES_RT, tp, 0


def simulate_ticks_full(sigs, tick_p, tick_t):
    sigs = sorted(sigs, key=lambda x: x["time"])
    trades = []
    current_day = None
    in_pos = False; pos_exit_ns = 0; cool_ns = 0
    cl_b = cl_r = 0; used = set()
    for s in sigs:
        if s["date"] != current_day:
            current_day = s["date"]
            in_pos = False; pos_exit_ns = 0; cool_ns = 0
            cl_b = cl_r = 0; used = set()
        entry_ns = int(s["time"].timestamp() * 1e9)
        if in_pos:
            if pos_exit_ns and entry_ns >= pos_exit_ns: in_pos = False
            else: continue
        if cool_ns and entry_ns < cool_ns: continue
        zk = (s["side"], s["zone"], s["zone_top"], s["zone_bot"])
        if zk in used: continue
        if s["side"] == "bull" and cl_b >= MCL: continue
        if s["side"] == "bear" and cl_r >= MCL: continue
        result, pnl, tp, exit_ns = outcome_tick(s, tick_p, tick_t)
        used.add(zk)
        if exit_ns > 0:
            in_pos = True; pos_exit_ns = exit_ns
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
        trades.append({**s, "target": tp, "result": result, "exit_time": None, "pnl": pnl})
    return trades


def apply_dll(trades, limit=DLL):
    by_day = defaultdict(list)
    for t in trades:
        by_day[t["date"]].append(t)
    result = []
    dll_days = 0
    for d in sorted(by_day.keys()):
        day_trades = sorted(by_day[d], key=lambda x: x["time"])
        cumul = 0.0; hit = False
        for t in day_trades:
            if hit: continue
            result.append(t)
            cumul += t["pnl"]
            if cumul <= -limit:
                hit = True
        if hit:
            dll_days += 1
    return result, dll_days


def calc_stats(trades):
    if not trades:
        return {"n": 0, "w": 0, "l": 0, "wr": 0, "pnl": 0, "pf": 0}
    wins = [t for t in trades if t["result"] == "WIN"]
    losses = [t for t in trades if t["result"] == "LOSS"]
    pnl = sum(t["pnl"] for t in trades)
    gw = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    return {"n": len(trades), "w": len(wins), "l": len(losses),
            "wr": 100 * len(wins) / len(trades) if trades else 0,
            "pnl": pnl, "pf": gw / gl if gl > 0 else float('inf')}


def run_row(label, sigs, tick_p, tick_t, col_w=38):
    gt = simulate_ticks_full(sigs, tick_p, tick_t)
    gt_dll, dll_days = apply_dll(gt)
    if not gt_dll:
        print(f"  {label:<{col_w}} (no trades)")
        return
    gs = calc_stats(gt_dll)
    dpnl = defaultdict(float)
    for t in gt_dll: dpnl[t["date"]] += t["pnl"]
    sorted_days = sorted(dpnl.keys())
    eq = []; r = 0.0
    for d in sorted_days: r += dpnl[d]; eq.append(r)
    pk = 0.0; mdd = 0.0
    for e in eq:
        if e > pk: pk = e
        if pk - e > mdd: mdd = pk - e
    months = max(1, len(sorted_days) / 21)
    mo = gs['pnl'] / months
    print(f"  {label:<{col_w}} {gs['n']:>4} {gs['wr']:>5.1f}% "
          f"${gs['pnl']:>+9,.0f} {gs['pf']:>5.2f} ${mdd:>7,.0f} "
          f"${mo:>+7,.0f} {dll_days:>4}", flush=True)


def build_dr(bars):
    """end_idx is EXCLUSIVE (last_bar_idx + 1) — matches canonical convention."""
    dr = {}
    for i, bar in enumerate(bars):
        d = bar.get("date")
        if d is None:
            d = datetime.fromtimestamp(bar["time_ns"] / 1e9, tz=CT).date()
            bar["date"] = d
        elif isinstance(d, str):
            parts = d.split("-")
            d = dt_date(int(parts[0]), int(parts[1]), int(parts[2]))
            bar["date"] = d
        if d not in dr: dr[d] = (i, i + 1)
        else: dr[d] = (dr[d][0], i + 1)
    return dr


def main():
    from v106_dynamic_rr_zone_entry import get_liquidity_levels, NS_MIN
    from backtest_entry_modes import gen_sweep_entries_enriched, apply_entry_mode, MODE_CLOSE_ENTRY

    t0 = time.time()

    # ── Tick files ──
    all_tick_files = sorted(globmod.glob(os.path.join(TICK_DIR, "glbx-mdp3-*.trades.dbn.zst")))
    target_dates = []
    for f in all_tick_files:
        ds = os.path.basename(f).split('-')[2].split('.')[0]
        d = dt_date(int(ds[:4]), int(ds[4:6]), int(ds[6:8]))
        if d.weekday() < 5 and os.path.getsize(f) > 500_000:
            target_dates.append(d)
    print(f"Tick data: {len(target_dates)} days ({target_dates[0]} → {target_dates[-1]})")

    print("Loading ticks...")
    tick_p, tick_t, loaded_dates = load_ticks(target_dates)
    print(f"  {len(tick_p):,} ticks loaded")

    # ── Bars ──
    sd, ed = str(target_dates[0]), str(target_dates[-1])
    print("Loading bars...")
    b5 = load_bar_cache(5, sd, ed)
    b1 = load_bar_cache(1, sd, ed)
    b15 = load_bar_cache(15, sd, ed)
    if b5 is None or b1 is None or b15 is None:
        print("Bar cache missing! Run backtest_tick.py first."); return
    print(f"  5m: {len(b5)} | 1m: {len(b1)} | 15m: {len(b15)}")

    dr5 = build_dr(b5)
    dr15 = build_dr(b15)
    all_dates = sorted(dr5.keys())
    trade_dates = [d for d in loaded_dates if d in dr5]
    print(f"  Trading dates: {len(trade_dates)} ({trade_dates[0]} → {trade_dates[-1]})")

    # Numpy index for fast b1 search
    b1_times_np = np.array([b["time_ns"] for b in b1], dtype=np.int64)

    # ── Signal generation ──
    print(f"\nGenerating signals...", flush=True)
    all_raw = []
    sig_t0 = time.time()
    for di, d in enumerate(trade_dates):
        ds5, de5 = dr5[d]
        liq = get_liquidity_levels(b5, dr5, d, all_dates)
        day_b1_ns = b5[ds5]["time_ns"] - 2 * 60 * NS_MIN
        b1_day_start = int(np.searchsorted(b1_times_np, day_b1_ns, side='left'))
        seen_ns = set()
        b1_cutoff = b1_day_start
        for cursor in range(ds5 + 1, de5):  # de5 is exclusive (last_idx+1)
            if cursor < de5:
                next_ns = b5[cursor]["time_ns"] + 5 * NS_MIN
            else:
                next_ns = b5[cursor - 1]["time_ns"] + 10 * NS_MIN
            while b1_cutoff < len(b1) and b1[b1_cutoff]["time_ns"] < next_ns:
                b1_cutoff += 1
            ents = gen_sweep_entries_enriched(
                _LV(b5, cursor + 1), b1[b1_day_start:b1_cutoff],
                ds5, cursor, d, liq)
            for e in sorted(ents, key=lambda x: (x["ns"], -{"ifvg": 2, "disp_fvg": 1}.get(x["zt"], 0))):
                if e["ns"] in seen_ns: continue
                _et = datetime.fromtimestamp(e["ns"] / 1e9, tz=CT)
                if (_et.hour < SIM_START_HOUR or
                        (_et.hour == SIM_START_HOUR and _et.minute < SIM_START_MIN)):
                    continue
                seen_ns.add(e["ns"])
                e["c1_b1_idx"] += b1_day_start
                e["_date"] = d; e["_ds5"] = ds5; e["_liq"] = liq
                all_raw.append(e)
        if (di + 1) % 50 == 0 or di == len(trade_dates) - 1:
            print(f"  {di+1}/{len(trade_dates)} days | {len(all_raw)} raw | {time.time()-sig_t0:.0f}s", flush=True)

    n_ifvg = sum(1 for e in all_raw if e["zt"] == "ifvg")
    n_dfvg = sum(1 for e in all_raw if e["zt"] == "disp_fvg")
    print(f"Signal gen done: {len(all_raw)} raw in {time.time()-sig_t0:.0f}s")
    print(f"disp_fvg: {n_dfvg} | ifvg: {n_ifvg}")

    # ── Build all signals flat 1.1 RR ──
    all_sigs = []
    for raw in all_raw:
        sig = apply_entry_mode(raw, MODE_CLOSE_ENTRY, b1, b5, raw["_ds5"],
                               raw["_liq"], dr15, b15, raw["_date"])
        if sig and sig["zone"] in ("disp_fvg", "ifvg"):
            sig["rr"] = FLAT_RR
            all_sigs.append(sig)

    dfvg_sigs = [s for s in all_sigs if s["zone"] == "disp_fvg"]
    ifvg_sigs = [s for s in all_sigs if s["zone"] == "ifvg"]
    print(f"\nSignals: {len(all_sigs)} total | dfvg={len(dfvg_sigs)} | ifvg={len(ifvg_sigs)}")

    # ── Score distribution by stop bucket ──
    print(f"\n{'=' * 80}")
    print("DISP_FVG: SCORE DISTRIBUTION BY STOP SIZE")
    print(f"{'=' * 80}")
    buckets = [(0, 5, "≤5pt"), (5, 8, "5-8pt"), (8, 10, "8-10pt"),
               (10, 12, "10-12pt"), (12, 15, "12-15pt"), (15, 99, ">15pt")]
    for lo, hi, label in buckets:
        grp = [s for s in dfvg_sigs if lo < s["risk_pts"] <= hi or (lo == 0 and s["risk_pts"] <= hi)]
        if not grp: continue
        scores = sorted(set(s["score"] for s in grp))
        sc_str = " | ".join(f"sc{sc}:{sum(1 for s in grp if s['score']==sc)}" for sc in scores)
        print(f"  {label:<10}: {len(grp):>4} sigs | {sc_str}")

    hdr = f"  {'Description':<38} {'Tr':>4} {'WR%':>6} {'P&L':>10} {'PF':>5} {'MaxDD':>8} {'$/mo':>8} {'DLL':>4}"
    sep = f"  {'─' * 90}"

    # ── GRID: disp_fvg score gate ──
    print(f"\n{'=' * 80}")
    print(f"SCORE GATE GRID — flat {FLAT_RR} RR | DLL ${DLL:.0f} | 1yr")
    print("  Rule: disp_fvg stop > threshold → require score >= min_sc")
    print("        iFVG: no gate (all kept)")
    print(f"{'=' * 80}")
    print(hdr); print(sep)

    run_row("BASELINE (no gate)", all_sigs, tick_p, tick_t)

    for stop_thresh in [8, 10, 12, 15]:
        for min_sc in [2, 3, 4, 5]:
            gated = [s for s in all_sigs
                     if not (s["zone"] == "disp_fvg"
                             and s["risk_pts"] > stop_thresh
                             and s.get("score", 0) < min_sc)]
            dropped = len(all_sigs) - len(gated)
            label = f"dfvg >{stop_thresh}pt→sc>={min_sc} (-{dropped} sigs)"
            run_row(label, gated, tick_p, tick_t)

    # ── iFVG score gate ──
    print(f"\n{'=' * 80}")
    print("iFVG SCORE GATE TEST")
    print(f"{'=' * 80}")
    print(hdr); print(sep)

    run_row("BASELINE (no gate)", all_sigs, tick_p, tick_t)
    for min_sc in [2, 3, 4]:
        gated = [s for s in all_sigs
                 if not (s["zone"] == "ifvg" and s.get("score", 0) < min_sc)]
        kept_ifvg = sum(1 for s in gated if s["zone"] == "ifvg")
        label = f"ifvg sc>={min_sc} ({kept_ifvg} ifvg kept)"
        run_row(label, gated, tick_p, tick_t)

    # ── COMBINED: best combos ──
    print(f"\n{'=' * 80}")
    print("COMBINED: dfvg score gate + iFVG stop cap")
    print(f"{'=' * 80}")
    print(hdr); print(sep)

    combos = [
        (99, 1, 99,  "BASELINE"),
        (10, 3, 99,  "dfvg>10→sc>=3 | ifvg uncapped"),
        (10, 4, 99,  "dfvg>10→sc>=4 | ifvg uncapped"),
        (12, 3, 99,  "dfvg>12→sc>=3 | ifvg uncapped"),
        (12, 4, 99,  "dfvg>12→sc>=4 | ifvg uncapped"),
        (10, 3, 12,  "dfvg>10→sc>=3 | ifvg <=12pt"),
        (10, 4, 12,  "dfvg>10→sc>=4 | ifvg <=12pt"),
        (12, 3, 12,  "dfvg>12→sc>=3 | ifvg <=12pt"),
        (12, 4, 12,  "dfvg>12→sc>=4 | ifvg <=12pt"),
        (10, 3, 10,  "dfvg>10→sc>=3 | ifvg <=10pt"),
        (10, 4, 10,  "dfvg>10→sc>=4 | ifvg <=10pt"),
        (12, 4, 10,  "dfvg>12→sc>=4 | ifvg <=10pt"),
    ]
    for dfvg_thresh, dfvg_sc, ifvg_cap, label in combos:
        gated = []
        for s in all_sigs:
            if s["zone"] == "disp_fvg":
                if s["risk_pts"] > dfvg_thresh and s.get("score", 0) < dfvg_sc:
                    continue
            elif s["zone"] == "ifvg":
                if s["risk_pts"] > ifvg_cap:
                    continue
            gated.append(s)
        run_row(label, gated, tick_p, tick_t)

    print(f"\nDone in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
