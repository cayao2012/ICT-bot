"""
Test individual filters + stacked combos ON TOP of Mode 3 (Zone+Confirm).
Goal: find which filters add edge without killing trade count.

BASE = Mode 3 (Zone+Confirm): 75% WR, $10,177, PF 5.67

INDIVIDUAL FILTERS (each applied to Mode 3):
  A. RSI loose (45/55)
  B. RSI tight (40/60)
  C. Rejection wick required
  D. 1m displacement candle (strong body at zone)
  E. Zone width filter (skip zones > 10pt wide)
  F. Zone width filter (skip zones > 6pt wide)
  G. Score >= 4 filter

STACKED COMBOS:
  S1. RSI loose + rejection
  S2. RSI loose + 1m displacement
  S3. RSI loose + zone width <= 10
  S4. RSI loose + rejection + zone width <= 10
  S5. RSI loose + 1m displacement + zone width <= 10
  S6. Score >= 4 + RSI loose
  S7. Score >= 4 + rejection
  S8. Best combo from above
"""
import os, sys, time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))

from tsxapipy import authenticate, APIClient
from backtest_topstep import fetch_with_rollover, build_dr, build_dr_htf
from v106_dynamic_rr_zone_entry import (
    get_liquidity_levels, detect_sweep_at, cisd_5m,
    structure_15m, sweep_15m, in_kz, KZ, NS_MIN,
)
# Reuse the enriched gen + helpers from entry_modes
from backtest_entry_modes import (
    gen_sweep_entries_enriched, calc_rsi, simulate_trades,
    SLIP, PV, CONTRACTS, MAX_RISK, FEES_RT,
)

CT = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")


# ═══════════════════════════════════════════════════════════════
# HELPER: 1m displacement candle check
# ═══════════════════════════════════════════════════════════════
def is_1m_displacement(b1, touch_idx, direction, lookback=5):
    """
    Check if the touch bar OR a bar just BEFORE it is a strong displacement candle.
    ONLY checks bars that have CLOSED by entry time (no look-ahead).
      - Body >= 60% of range
      - Range >= 1.3x average of prior N candles
      - Direction matches trade side
    """
    if touch_idx < lookback + 1 or touch_idx >= len(b1):
        return False

    # Average range of prior bars
    avg_range = sum(
        b1[touch_idx - i]["high"] - b1[touch_idx - i]["low"]
        for i in range(1, lookback + 1)
    ) / lookback
    if avg_range <= 0:
        return False

    # ONLY check touch bar (offset 0) and 1 bar before (offset -1)
    # Both have closed by entry time. NO future bars.
    for offset in range(0, -2, -1):
        idx = touch_idx + offset
        if idx < 0 or idx >= len(b1):
            continue
        bar = b1[idx]
        rng = bar["high"] - bar["low"]
        if rng <= 0:
            continue
        body = abs(bar["close"] - bar["open"])
        body_ratio = body / rng

        if body_ratio < 0.60:
            continue
        if rng < avg_range * 1.3:
            continue

        # Direction check
        if direction == "bull" and bar["close"] > bar["open"]:
            return True
        if direction == "bear" and bar["close"] < bar["open"]:
            return True

    return False


# ═══════════════════════════════════════════════════════════════
# BUILD BASE MODE 3 SIGNAL from enriched entry
# ═══════════════════════════════════════════════════════════════
def build_base_signal(raw, b1, b5, ds5, liq_levels, dr15, b15, d):
    """Build a Mode 3 (zone+confirm) signal with all metadata for filtering."""
    if not raw["close_confirmed"]:
        return None

    side = raw["side"]
    ep = raw["ep_zone"]
    sp = raw["sp"]
    if side == "bull":
        ep += SLIP
    else:
        ep -= SLIP

    risk = abs(ep - sp)
    if risk <= 0:
        return None
    risk_d = risk * PV * CONTRACTS
    if risk_d > MAX_RISK:
        return None

    # Score
    score = 1
    if raw.get("rej"):
        score += 1
    cisd = cisd_5m(b5, raw["bar_idx"], ds5)
    if cisd == side:
        score += 1
    sw_d, _, _ = detect_sweep_at(b5, raw["bar_idx"], liq_levels, lookback=8)
    if sw_d == side:
        score += 2
    struct = None
    if b15 is not None and dr15 is not None:
        struct = structure_15m(b15, dr15, d, raw["ns"])
        if struct == side:
            score += 2
    sw15 = False
    if b15 is not None and dr15 is not None:
        sw15 = sweep_15m(b15, dr15, d, raw["ns"], liq_levels, side)
        if sw15:
            score += 1

    has_sweep = sw_d == side
    has_struct = struct == side if struct else False
    has_cisd = cisd == side
    if has_sweep:
        rr = 2.0
    elif has_struct:
        rr = 1.7
    elif has_cisd:
        rr = 1.5
    else:
        rr = 1.3

    entry_dt = datetime.fromtimestamp(raw["ns"] / 1e9, tz=CT)

    # Compute all filter metadata
    rsi = calc_rsi(b1, raw["c1_b1_idx"], period=14)
    has_rej = raw.get("rej", False)
    has_disp = is_1m_displacement(b1, raw["c1_b1_idx"], side)
    zone_width = raw["zone_top"] - raw["zone_bot"]

    return {
        "date": d, "side": side, "entry": ep, "stop": sp,
        "risk_pts": risk, "risk_$": risk_d,
        "score": score, "rr": rr, "zone": raw["zt"],
        "time": entry_dt,
        "zone_top": raw["zone_top"], "zone_bot": raw["zone_bot"],
        "c1_close": raw["c1_close"],
        "dist_from_zone": abs(ep - (raw["zone_top"] if side == "bull" else raw["zone_bot"])),
        # Filter metadata
        "rsi": rsi,
        "has_rej": has_rej,
        "has_disp": has_disp,
        "zone_width": zone_width,
    }


# ═══════════════════════════════════════════════════════════════
# FILTER DEFINITIONS
# ═══════════════════════════════════════════════════════════════
def filt_rsi_loose(s):
    """RSI < 45 for bull, > 55 for bear."""
    if s["side"] == "bull":
        return s["rsi"] < 45
    return s["rsi"] > 55

def filt_rsi_tight(s):
    """RSI < 40 for bull, > 60 for bear."""
    if s["side"] == "bull":
        return s["rsi"] < 40
    return s["rsi"] > 60

def filt_rsi_medium(s):
    """RSI < 42 for bull, > 58 for bear."""
    if s["side"] == "bull":
        return s["rsi"] < 42
    return s["rsi"] > 58

def filt_rejection(s):
    """Rejection wick into zone center."""
    return s["has_rej"]

def filt_displacement(s):
    """1m displacement candle at zone."""
    return s["has_disp"]

def filt_zone_width_10(s):
    """Zone width <= 10 pts."""
    return s["zone_width"] <= 10.0

def filt_zone_width_6(s):
    """Zone width <= 6 pts."""
    return s["zone_width"] <= 6.0

def filt_score_4(s):
    """Score >= 4."""
    return s["score"] >= 4

def filt_score_3(s):
    """Score >= 3."""
    return s["score"] >= 3


# All filter configs: (name, list_of_filter_functions)
FILTER_CONFIGS = [
    # ── BASE ──
    ("BASE (Zone+Confirm)",             []),
    # ── INDIVIDUAL ──
    ("A. RSI loose (45/55)",            [filt_rsi_loose]),
    ("B. RSI tight (40/60)",            [filt_rsi_tight]),
    ("B2. RSI medium (42/58)",          [filt_rsi_medium]),
    ("C. Rejection wick",               [filt_rejection]),
    ("D. 1m displacement",              [filt_displacement]),
    ("E. Zone width <= 10pt",           [filt_zone_width_10]),
    ("F. Zone width <= 6pt",            [filt_zone_width_6]),
    ("G. Score >= 4",                   [filt_score_4]),
    ("G2. Score >= 3",                  [filt_score_3]),
    # ── STACKED COMBOS ──
    ("S1. RSI loose + rej",             [filt_rsi_loose, filt_rejection]),
    ("S2. RSI loose + disp",            [filt_rsi_loose, filt_displacement]),
    ("S3. RSI loose + zw<=10",          [filt_rsi_loose, filt_zone_width_10]),
    ("S4. RSI loose + rej + zw<=10",    [filt_rsi_loose, filt_rejection, filt_zone_width_10]),
    ("S5. RSI loose + disp + zw<=10",   [filt_rsi_loose, filt_displacement, filt_zone_width_10]),
    ("S6. Score>=4 + RSI loose",        [filt_score_4, filt_rsi_loose]),
    ("S7. Score>=4 + rej",              [filt_score_4, filt_rejection]),
    ("S8. Score>=4 + RSI loose + rej",  [filt_score_4, filt_rsi_loose, filt_rejection]),
    ("S9. RSI med + rej",              [filt_rsi_medium, filt_rejection]),
    ("S10. RSI med + disp",            [filt_rsi_medium, filt_displacement]),
    ("S11. Score>=3 + RSI loose",       [filt_score_3, filt_rsi_loose]),
    ("S12. Score>=3 + rej",             [filt_score_3, filt_rejection]),
    ("S13. Rej + disp",                 [filt_rejection, filt_displacement]),
    ("S14. RSI loose + rej + disp",     [filt_rsi_loose, filt_rejection, filt_displacement]),
]


def main():
    t0 = time.time()
    print("=" * 75)
    print("FILTER TEST — Individual + Stacked on top of Mode 3 (Zone+Confirm)")
    print("=" * 75)

    # ── Config ──
    start = datetime(2026, 3, 10, 0, 0, 0, tzinfo=CT)
    end = datetime(2026, 3, 20, 0, 0, 0, tzinfo=CT)
    lookback = start - timedelta(days=7)
    SIM_START_HOUR = 7
    SIM_START_MIN = 30

    # ── Auth + fetch ──
    token, token_time = authenticate()
    if not token:
        print("Auth failed!"); sys.exit(1)
    api = APIClient(initial_token=token, token_acquired_at=token_time)
    print("Authenticated.\n")

    print("Fetching bars...")
    b5 = fetch_with_rollover(api, 5, lookback, end)
    b15 = fetch_with_rollover(api, 15, lookback, end)
    b1 = fetch_with_rollover(api, 1, lookback, end)
    print(f"  5m: {len(b5)} | 15m: {len(b15)} | 1m: {len(b1)}")

    dr5 = build_dr(b5)
    dr15 = build_dr_htf(b15)
    all_dates = sorted(dr5.keys())
    target_start = start.date()
    trade_dates = [d for d in all_dates if d >= target_start]
    print(f"  Trading dates: {[str(d) for d in trade_dates]}\n")

    # ── Generate all base Mode 3 signals with filter metadata ──
    print("Generating base signals with filter metadata...")
    all_sigs = []

    for d in trade_dates:
        if d not in dr5:
            continue
        ds5, de5 = dr5[d]
        liq = get_liquidity_levels(b5, dr5, d, all_dates)

        seen_ns = set()
        b1_cutoff = 0
        for cursor in range(ds5 + 1, de5 + 1):
            if cursor < de5:
                next_bar_ns = b5[cursor]["time_ns"] + 5 * 60_000_000_000
            else:
                next_bar_ns = b5[cursor - 1]["time_ns"] + 10 * 60_000_000_000
            while b1_cutoff < len(b1) and b1[b1_cutoff]["time_ns"] < next_bar_ns:
                b1_cutoff += 1

            ents = gen_sweep_entries_enriched(
                b5, b1[:b1_cutoff], ds5, cursor, d, liq)

            for e in sorted(ents, key=lambda x: (x["ns"],
                            -{"ifvg": 2, "disp_fvg": 1}.get(x["zt"], 0))):
                if e["ns"] in seen_ns:
                    continue
                _et = datetime.fromtimestamp(e["ns"] / 1e9, tz=CT)
                if (_et.hour < SIM_START_HOUR or
                        (_et.hour == SIM_START_HOUR and _et.minute < SIM_START_MIN)):
                    continue
                seen_ns.add(e["ns"])

                sig = build_base_signal(e, b1, b5, ds5, liq, dr15, b15, d)
                if sig:
                    all_sigs.append(sig)

    print(f"  Base Mode 3 signals: {len(all_sigs)}")

    # Show filter metadata distribution
    rsi_vals = [s["rsi"] for s in all_sigs]
    rej_count = sum(1 for s in all_sigs if s["has_rej"])
    disp_count = sum(1 for s in all_sigs if s["has_disp"])
    widths = [s["zone_width"] for s in all_sigs]
    scores = [s["score"] for s in all_sigs]

    print(f"  RSI range: {min(rsi_vals):.0f} - {max(rsi_vals):.0f} | "
          f"median: {sorted(rsi_vals)[len(rsi_vals)//2]:.0f}")
    print(f"  Rejection wick: {rej_count}/{len(all_sigs)} ({100*rej_count/len(all_sigs):.0f}%)")
    print(f"  1m displacement: {disp_count}/{len(all_sigs)} ({100*disp_count/len(all_sigs):.0f}%)")
    print(f"  Zone width: {min(widths):.1f} - {max(widths):.1f}pt | "
          f"median: {sorted(widths)[len(widths)//2]:.1f}pt")
    print(f"  Score distribution: ", end="")
    from collections import Counter
    sc = Counter(scores)
    print(" | ".join(f"sc={k}:{v}" for k, v in sorted(sc.items())))

    # ── Run each filter config ──
    print(f"\n{'='*75}")
    print(f"  {'Filter':<32} {'Tr':>4} {'W':>3} {'L':>3} {'WR%':>6} "
          f"{'PnL':>10} {'AvgW':>7} {'AvgL':>7} {'PF':>6} {'$/day':>7}")
    print(f"  {'-'*72}")

    n_days = len(trade_dates)
    results = []

    for name, filters in FILTER_CONFIGS:
        # Apply filters
        filtered = all_sigs
        for f in filters:
            filtered = [s for s in filtered if f(s)]

        # Simulate trades
        trades = simulate_trades(filtered, b1)

        if not trades:
            print(f"  {name:<32} {'—':>4}")
            results.append((name, 0, 0, 0, 0, 0, 0, 0, 0))
            continue

        wins = [t for t in trades if t["result"] == "WIN"]
        losses = [t for t in trades if t["result"] == "LOSS"]
        total_pnl = sum(t["pnl"] for t in trades)
        wr = 100 * len(wins) / len(trades)
        avg_w = sum(t["pnl"] for t in wins) / len(wins) if wins else 0
        avg_l = sum(t["pnl"] for t in losses) / len(losses) if losses else 0
        gross_w = sum(t["pnl"] for t in wins)
        gross_l = abs(sum(t["pnl"] for t in losses))
        pf = gross_w / gross_l if gross_l > 0 else 999.0
        per_day = total_pnl / n_days

        flag = ""
        if pf >= 5.0 and len(trades) >= 8:
            flag = " ***"
        elif pf >= 4.0 and len(trades) >= 10:
            flag = " **"
        elif pf >= 3.0 and len(trades) >= 12:
            flag = " *"

        print(f"  {name:<32} {len(trades):>4} {len(wins):>3} {len(losses):>3} "
              f"{wr:>5.1f}% ${total_pnl:>9,.0f} ${avg_w:>6,.0f} ${avg_l:>6,.0f} "
              f"{pf:>6.2f} ${per_day:>6,.0f}{flag}")

        results.append((name, len(trades), len(wins), len(losses),
                        wr, total_pnl, avg_w, avg_l, pf))

    # ── Show trades for top configs ──
    # Find top 3 by PnL that have >= 5 trades
    viable = [(r[0], r[5], r[4], r[8]) for r in results if r[1] >= 5]
    viable.sort(key=lambda x: x[1], reverse=True)

    print(f"\n{'='*75}")
    print("TOP CONFIGS (>= 5 trades, sorted by P&L)")
    print(f"{'='*75}")
    for name, pnl, wr, pf in viable[:5]:
        print(f"  {name:<35} ${pnl:>9,.0f} | {wr:.1f}% WR | PF {pf:.2f}")

    # Show detailed trades for top 3
    for rank, (top_name, _, _, _) in enumerate(viable[:3]):
        # Find matching filter config
        for name, filters in FILTER_CONFIGS:
            if name == top_name:
                filtered = all_sigs
                for f in filters:
                    filtered = [s for s in filtered if f(s)]
                trades = simulate_trades(filtered, b1)

                print(f"\n{'='*75}")
                print(f"#{rank+1} DETAIL — {top_name}")
                print(f"{'='*75}")
                for t in trades:
                    exit_str = (f"exit {t['exit_time'].strftime('%H:%M')}"
                                if t.get("exit_time") else "OPEN")
                    print(f"  {t['date']} {t['time'].strftime('%H:%M')} "
                          f"| {t['side'].upper():4s} @ {t['entry']:.2f} "
                          f"| SL {t['stop']:.2f} TP {t['target']:.2f} "
                          f"| risk ${t['risk_$']:.0f} sc={t['score']} rr={t['rr']} "
                          f"| {t['result']} {exit_str} ${t['pnl']:+,.0f} "
                          f"| rsi={t.get('rsi',0):.0f} "
                          f"rej={'Y' if t.get('has_rej') else 'N'} "
                          f"disp={'Y' if t.get('has_disp') else 'N'} "
                          f"zw={t.get('zone_width',0):.1f}")

                # Daily breakdown
                daily = {}
                for t in trades:
                    daily.setdefault(t["date"], 0.0)
                    daily[t["date"]] += t["pnl"]
                print(f"\n  Daily: ", end="")
                for dd in sorted(daily):
                    print(f"{str(dd)[5:]} ${daily[dd]:+,.0f}  ", end="")
                print()
                break

    print(f"\n  Completed in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
