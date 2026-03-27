"""
Compare 5 entry methods for the V106 ICT strategy.
Same zones, different entry logic. Runs on multi-day data.

MODES:
  1. ZONE_LIMIT    — Limit order at zone edge. No close confirmation.
  2. CLOSE_ENTRY   — Enter at 1m close price (current production).
  3. ZONE_CONFIRM  — Zone price + 1m close confirmation (hybrid).
  4. FVG_1M        — Zone confirmed → 1m FVG forms → enter on FVG retest.
  5. RSI_ZONE      — Zone confirmed + RSI oversold/overbought filter.
"""
import os, sys, time
from datetime import datetime, timedelta, date as dt_date
from zoneinfo import ZoneInfo
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))

try:
    from tsxapipy import authenticate, APIClient
    from backtest_topstep import fetch_with_rollover, build_dr, build_dr_htf
except ImportError:
    pass  # Not needed when imported by the live bot
from v106_dynamic_rr_zone_entry import (
    get_liquidity_levels, detect_sweep_at, cisd_5m,
    structure_15m, sweep_15m, in_kz, KZ, NS_MIN,
)

CT = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")

SLIP = float(os.environ.get("BACKTEST_SLIP", "0.5"))
PV = 20
CONTRACTS = 3
MAX_RISK = 1000
COOLDOWN_S = 120
MCL = 3
FEES_RT = 8.40

# ── iFVG zone quality thresholds ──
IFVG_MIN_GAP   = 1.0    # minimum FVG gap in points (was 0 → 0.25pt noise)
IFVG_MAX_WIDTH = 12.0   # max zone width in points  (was uncapped → 83pt zones)
IFVG_LOOKBACK  = 8      # max bars to scan back     (was 40 → 3+ hours stale)
IFVG_INV_BODY  = 0.35   # inverting candle min body/range ratio
IFVG_INV_CLEAR = 0.5    # close must clear FVG edge by this many points

# ═══════════════════════════════════════════════════════════════
# MODE LABELS
# ═══════════════════════════════════════════════════════════════
MODE_ZONE_LIMIT  = "zone_limit"
MODE_CLOSE_ENTRY = "close_entry"
MODE_ZONE_CONF   = "zone_conf"
MODE_FVG_1M      = "fvg_1m"
MODE_RSI_ZONE    = "rsi_zone"

ALL_MODES = [MODE_ZONE_LIMIT, MODE_CLOSE_ENTRY, MODE_ZONE_CONF,
             MODE_FVG_1M, MODE_RSI_ZONE]

MODE_NAMES = {
    MODE_ZONE_LIMIT:  "1. Zone Limit",
    MODE_CLOSE_ENTRY: "2. 1m Close",
    MODE_ZONE_CONF:   "3. Zone+Confirm",
    MODE_FVG_1M:      "4. 1m FVG",
    MODE_RSI_ZONE:    "5. RSI+Zone",
}


# ═══════════════════════════════════════════════════════════════
# HELPER: RSI on 1m closes
# ═══════════════════════════════════════════════════════════════
def calc_rsi(bars, end_idx, period=14):
    """Wilder RSI at end_idx."""
    start = max(0, end_idx - period * 3)
    if end_idx - start < period + 1:
        return 50.0
    closes = [bars[i]["close"] for i in range(start, end_idx + 1)]
    if len(closes) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(0, d))
        losses.append(max(0, -d))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return 100 - 100 / (1 + avg_g / avg_l)


# ═══════════════════════════════════════════════════════════════
# HELPER: Find 1m FVG near zone after touch
# ═══════════════════════════════════════════════════════════════
def find_1m_fvg(b1, touch_idx, direction, zone_top, zone_bot, max_scan=12,
                max_retest_wait=15):
    """
    After zone touch at touch_idx, scan next 1m bars for an FVG in the
    signal direction.  Return (fvg_entry_price, entry_bar_idx) or (None, None).

    Bull FVG: bar[k].low > bar[k-2].high  (gap up on 1m)
    Bear FVG: bar[k].high < bar[k-2].low  (gap down on 1m)
    """
    for k in range(touch_idx + 2, min(touch_idx + max_scan + 2, len(b1))):
        if k < 2:
            continue
        if direction == "bull":
            gap = b1[k]["low"] - b1[k - 2]["high"]
            if gap >= 0.25:
                fvg_top = b1[k]["low"]
                fvg_bot = b1[k - 2]["high"]
                # FVG should be near the zone (within 8pts)
                if fvg_bot > zone_top + 8.0:
                    continue
                # Wait for retest: price dips back to FVG top
                for j in range(k + 1, min(k + max_retest_wait, len(b1))):
                    if b1[j]["low"] <= fvg_top:
                        return fvg_top + 0.25, j  # enter just above FVG
                return None, None  # FVG found but no retest
        else:
            gap = b1[k - 2]["low"] - b1[k]["high"]
            if gap >= 0.25:
                fvg_top = b1[k - 2]["low"]
                fvg_bot = b1[k]["high"]
                if fvg_top < zone_bot - 8.0:
                    continue
                for j in range(k + 1, min(k + max_retest_wait, len(b1))):
                    if b1[j]["high"] >= fvg_bot:
                        return fvg_bot - 0.25, j
                return None, None
    return None, None


# ═══════════════════════════════════════════════════════════════
# ENRICHED gen_sweep_entries — returns both zone & close prices
# ═══════════════════════════════════════════════════════════════
def gen_sweep_entries_enriched(b5, b1, ds5, de5, d, liq_levels,
                               stop_b5=None, kz=None):
    """
    Same zone detection as v106, but returns enriched entries:
      ep_zone, ep_close, c1_close/high/low/open, c1_b1_idx, close_confirmed
    Runs with NO close confirmation requirement so mode 1 can use results too.
    """
    if stop_b5 is None:
        stop_b5 = b5
    if kz is None:
        kz = KZ
    entries = []
    used = set()
    session_shs = []
    session_sls = []
    pending_sh = None
    pending_sl = None

    for i in range(ds5, de5):
        if i >= len(b5):
            break
        if pending_sh is not None:
            session_shs.append(pending_sh); pending_sh = None
        if pending_sl is not None:
            session_sls.append(pending_sl); pending_sl = None
        if i > ds5 + 1:
            if b5[i-1]["high"] > b5[i-2]["high"] and b5[i-1]["high"] > b5[i]["high"]:
                pending_sh = (b5[i-1]["high"], "ses_sh")
            if b5[i-1]["low"] < b5[i-2]["low"] and b5[i-1]["low"] < b5[i]["low"]:
                pending_sl = (b5[i-1]["low"], "ses_sl")
        if not in_kz(b5[i]["hour"], b5[i]["minute"], kz):
            continue
        live_levels = liq_levels + session_shs[-6:] + session_sls[-6:]
        sw_dir, sw_lvl, sw_bar = detect_sweep_at(b5, i, live_levels, lookback=12)
        if sw_dir is None:
            continue

        disp_idx = None
        for j in range(sw_bar, min(sw_bar + 6, len(b5))):
            bar = b5[j]
            body = abs(bar["close"] - bar["open"])
            rng = bar["high"] - bar["low"]
            if rng <= 0:
                continue
            if body / rng >= 0.35:
                if sw_dir == "bull" and bar["close"] > bar["open"]:
                    disp_idx = j; break
                if sw_dir == "bear" and bar["close"] < bar["open"]:
                    disp_idx = j; break
        if disp_idx is None or disp_idx in used:
            continue

        # ── Zone detection ──
        zones = []
        entries_from_this = 0

        # ── PB Blake IFVG: highest TF inversion FVG in the leg ──
        # Blake checks 1m→5m, uses the HIGHEST timeframe FVG that exists.
        # Must be SINGULAR (only gap in the leg on that TF).
        disp_ns = b5[disp_idx]["time_ns"]
        sweep_ns = b5[sw_bar]["time_ns"]
        # Find 1m bar range for the leg: ~15 bars before sweep through displacement
        leg_b1_start = None
        leg_b1_end = None
        for ki in range(len(b1)):
            if b1[ki]["time_ns"] >= sweep_ns - 15 * NS_MIN and leg_b1_start is None:
                leg_b1_start = ki
            if b1[ki]["time_ns"] >= disp_ns:
                leg_b1_end = ki
                break
        if leg_b1_start is not None and leg_b1_end is not None and leg_b1_end > leg_b1_start + 2:
            leg_1m = b1[leg_b1_start:leg_b1_end]

            # Build N-minute bars from 1m bars in the leg
            def _build_nm(bars_1m, n):
                """Aggregate 1m bars into n-minute bars."""
                if n == 1:
                    return list(bars_1m)
                out = []
                for i in range(0, len(bars_1m) - n + 1, n):
                    grp = bars_1m[i:i+n]
                    if len(grp) < n:
                        break
                    out.append({
                        "open": grp[0]["open"], "high": max(b["high"] for b in grp),
                        "low": min(b["low"] for b in grp), "close": grp[-1]["close"],
                        "time_ns": grp[0]["time_ns"],
                    })
                return out

            # Scan for contrary FVGs on a given bar list
            def _find_fvgs(bars, direction, min_gap=0.5):
                fvgs = []
                for k in range(len(bars) - 2):
                    p, c, n = bars[k], bars[k+1], bars[k+2]
                    if direction == "bull":
                        gap = p["low"] - n["high"]  # bearish FVG (contrary)
                        if gap < min_gap: continue
                        ft, fb = p["low"], n["high"]
                    else:
                        gap = n["low"] - p["high"]  # bullish FVG (contrary)
                        if gap < min_gap: continue
                        ft, fb = n["low"], p["high"]
                    if (ft - fb) > IFVG_MAX_WIDTH: continue
                    # Check not already violated within remaining leg bars
                    bad = False
                    for kk in range(k+3, len(bars)):
                        if direction == "bull" and bars[kk]["close"] < fb:
                            bad = True; break
                        if direction == "bear" and bars[kk]["close"] > ft:
                            bad = True; break
                    if not bad:
                        fvgs.append({"top": ft, "bot": fb, "bar_k": k})
                return fvgs

            # Check timeframes highest to lowest: 5m, 4m, 3m, 2m, 1m
            best_fvg = None
            best_tf = 0
            for tf in [5, 4, 3, 2, 1]:
                tf_bars = _build_nm(leg_1m, tf)
                if len(tf_bars) < 3:
                    continue
                fvgs = _find_fvgs(tf_bars, sw_dir)
                # Singular: must be exactly 1 FVG in the leg on this TF
                if len(fvgs) == 1:
                    best_fvg = fvgs[0]
                    best_tf = tf
                    break  # highest TF with singular FVG wins

            # Fallback: if no singular found, use highest TF with any FVGs (extreme edge)
            if best_fvg is None:
                for tf in [5, 4, 3, 2, 1]:
                    tf_bars = _build_nm(leg_1m, tf)
                    if len(tf_bars) < 3:
                        continue
                    fvgs = _find_fvgs(tf_bars, sw_dir)
                    if fvgs:
                        # Use extreme edge (all must be inverted)
                        if sw_dir == "bull":
                            best_fvg = max(fvgs, key=lambda f: f["top"])
                        else:
                            best_fvg = min(fvgs, key=lambda f: f["bot"])
                        best_tf = tf
                        break

            if best_fvg is not None:
                ifvg_1m = [best_fvg]
                if sw_dir == "bull":
                    inv_level = best_fvg["top"]
                else:
                    inv_level = best_fvg["bot"]
                # Scan 1m AFTER displacement bar closes for the inverting candle
                # Can't know it's displacement until the 5m bar fully closes.
                disp_bar_end_ns = b5[disp_idx]["time_ns"] + 5 * NS_MIN
                ifvg_b1_start = leg_b1_end
                for _si in range(leg_b1_end, len(b1)):
                    if b1[_si]["time_ns"] >= disp_bar_end_ns:
                        ifvg_b1_start = _si; break
                for ki in range(ifvg_b1_start, min(ifvg_b1_start + 20, len(b1))):
                    if not in_kz(b1[ki]["hour"], b1[ki]["minute"], kz): continue
                    c1 = b1[ki]
                    body = abs(c1["close"] - c1["open"])
                    rng = c1["high"] - c1["low"]
                    if rng <= 0: continue
                    inverted = False
                    if sw_dir == "bull":
                        if (c1["close"] > c1["open"] and
                            c1["close"] > inv_level + IFVG_INV_CLEAR and
                            body / rng >= IFVG_INV_BODY):
                            inverted = True
                    else:
                        if (c1["close"] < c1["open"] and
                            c1["close"] < inv_level - IFVG_INV_CLEAR and
                            body / rng >= IFVG_INV_BODY):
                            inverted = True
                    if not inverted:
                        continue
                    # Stop at swing low/high of the 5m leg
                    leg_range = range(sw_bar, min(disp_idx + 1, len(b5)))
                    if sw_dir == "bull":
                        leg_ext = min(b5[m]["low"] for m in leg_range) if leg_range else c1["low"]
                        sp = min(sw_lvl, leg_ext) - 1.0
                    else:
                        leg_ext = max(b5[m]["high"] for m in leg_range) if leg_range else c1["high"]
                        sp = max(sw_lvl, leg_ext) + 1.0
                    ep = c1["close"]
                    risk = abs(ep + (SLIP if sw_dir == "bull" else -SLIP) - sp)
                    if risk < 1.0 or risk * PV * CONTRACTS > MAX_RISK:
                        break
                    # Displacement high/low for structural target capping
                    disp_high = b5[disp_idx]["high"]
                    disp_low = b5[disp_idx]["low"]
                    entries.append({
                        "ep_zone": ep, "ep_close": ep,
                        "sp": sp, "sp_struct": sp,
                        "ns": c1["time_ns"] + NS_MIN,
                        "rej": False,
                        "hour": c1["hour"], "bar_idx": disp_idx, "sw_bar": sw_bar,
                        "c1_close": c1["close"], "c1_high": c1["high"],
                        "c1_low": c1["low"], "c1_open": c1["open"],
                        "c1_b1_idx": ki, "close_confirmed": True,
                        "side": sw_dir, "zt": "ifvg",
                        "swept": sw_lvl,
                        "zone_top": ifvg_1m[0]["top"],
                        "zone_bot": ifvg_1m[0]["bot"],
                        "disp_high": disp_high,
                        "disp_low": disp_low,
                    })
                    used.add(disp_idx)
                    entries_from_this += 1
                    break

        # ── disp_fvg zones (displacement FVG — created BY the displacement move) ──
        for k in range(sw_bar, min(disp_idx + 2, len(b5))):
            if k < 1 or k + 1 >= len(b5): continue
            p, c, n = b5[k-1], b5[k], b5[k+1]
            if sw_dir == "bull":
                gap = n["low"] - p["high"]
                if gap > 0:
                    ft, fb = n["low"], p["high"]
                    mit = any(b5[j]["close"] < fb for j in range(k+2, len(b5)))
                    if not mit:
                        zones.append({"top": ft, "bot": fb, "ce": (ft+fb)/2, "type": "disp_fvg"})
            else:
                gap = p["low"] - n["high"]
                if gap > 0:
                    ft, fb = p["low"], n["high"]
                    mit = any(b5[j]["close"] > ft for j in range(k+2, len(b5)))
                    if not mit:
                        zones.append({"top": ft, "bot": fb, "ce": (ft+fb)/2, "type": "disp_fvg"})
        if not zones and entries_from_this == 0:
            continue

        # ── 1m touch scanning for disp_fvg zones ──
        # Live-accurate: scan 1m bars in real-time order. No 5m close filter.
        # Enter on first 1m bar that touches zone AND closes inside it.
        touched_zones = set()
        disp_end_ns = b5[disp_idx]["time_ns"] + 5 * NS_MIN
        for zi, z in enumerate(zones[:4]):
            if entries_from_this >= 2: break
            first_touch = None
            for k in range(len(b1)):
                c1 = b1[k]
                if c1["time_ns"] < disp_end_ns: continue
                if not in_kz(c1["hour"], c1["minute"], kz): continue
                if sw_dir == "bull":
                    if not (c1["low"] <= z["top"] and c1["close"] >= z["bot"]): continue
                    # Last completed 5m bar at this 1m bar's time (for stop calc)
                    j5 = disp_idx
                    for jj in range(disp_idx + 1, de5):
                        if jj >= len(b5): break
                        if b5[jj]["time_ns"] + 5 * NS_MIN <= c1["time_ns"]: j5 = jj
                        else: break
                    stop_end = min(j5 + 1, len(stop_b5))
                    pl = (min(stop_b5[m]["low"]
                          for m in range(max(disp_idx+1, sw_bar), stop_end)
                          if m < len(stop_b5))
                          if stop_end > max(disp_idx+1, sw_bar) else c1["low"])
                    sp = min(sw_lvl, pl, c1["low"]) - 1.0
                    sp_struct = min(sw_lvl, pl) - 1.0
                    rej = c1["low"] < z["ce"] and min(c1["open"], c1["close"]) >= z["bot"]
                    first_touch = {
                        "ep_zone": z["top"] + 1.0, "ep_close": c1["close"],
                        "sp": sp, "sp_struct": sp_struct,
                        "ns": c1["time_ns"] + NS_MIN, "rej": rej,
                        "hour": c1["hour"], "bar_idx": j5, "sw_bar": sw_bar,
                        "c1_close": c1["close"], "c1_high": c1["high"],
                        "c1_low": c1["low"], "c1_open": c1["open"],
                        "c1_b1_idx": k, "close_confirmed": True,
                    }
                    break
                else:
                    if not (c1["high"] >= z["bot"] and c1["close"] <= z["top"]): continue
                    j5 = disp_idx
                    for jj in range(disp_idx + 1, de5):
                        if jj >= len(b5): break
                        if b5[jj]["time_ns"] + 5 * NS_MIN <= c1["time_ns"]: j5 = jj
                        else: break
                    stop_end = min(j5 + 1, len(stop_b5))
                    ph = (max(stop_b5[m]["high"]
                          for m in range(max(disp_idx+1, sw_bar), stop_end)
                          if m < len(stop_b5))
                          if stop_end > max(disp_idx+1, sw_bar) else c1["high"])
                    sp = max(sw_lvl, ph, c1["high"]) + 1.0
                    sp_struct = max(sw_lvl, ph) + 1.0
                    rej = c1["high"] > z["ce"] and max(c1["open"], c1["close"]) <= z["top"]
                    first_touch = {
                        "ep_zone": z["bot"] - 1.0, "ep_close": c1["close"],
                        "sp": sp, "sp_struct": sp_struct,
                        "ns": c1["time_ns"] + NS_MIN, "rej": rej,
                        "hour": c1["hour"], "bar_idx": j5, "sw_bar": sw_bar,
                        "c1_close": c1["close"], "c1_high": c1["high"],
                        "c1_low": c1["low"], "c1_open": c1["open"],
                        "c1_b1_idx": k, "close_confirmed": True,
                    }
                    break
            if first_touch:
                entries.append({
                    **first_touch, "side": sw_dir, "zt": z["type"],
                    "swept": sw_lvl,
                    "zone_top": z["top"], "zone_bot": z["bot"],
                })
                used.add(first_touch["bar_idx"])
                entries_from_this += 1
                touched_zones.add(zi)

        used.add(disp_idx)
    return entries


# ═══════════════════════════════════════════════════════════════
# APPLY ENTRY MODE — produce a trade signal from enriched entry
# ═══════════════════════════════════════════════════════════════
def apply_entry_mode(raw, mode, b1, b5, ds5, liq_levels, dr15=None, b15=None, d=None):
    """
    Given a raw enriched entry, return a trade signal dict for the given mode,
    or None if the mode filters it out.
    """
    side = raw["side"]

    # ── Mode 1: Zone Limit (no close confirmation needed) ──
    if mode == MODE_ZONE_LIMIT:
        ep = raw["ep_zone"]
        sp = raw["sp_struct"]  # structural stop, not c1-based
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
        return _build_sig(raw, ep, sp, risk, risk_d, raw["ns"], mode, b5, ds5,
                          liq_levels, dr15, b15, d)

    # Modes 2-5 all require close confirmation
    if not raw["close_confirmed"]:
        return None

    # ── Mode 2: 1m Close Entry ──
    if mode == MODE_CLOSE_ENTRY:
        ep = raw["ep_close"]
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
        return _build_sig(raw, ep, sp, risk, risk_d, raw["ns"], mode, b5, ds5,
                          liq_levels, dr15, b15, d)

    # ── Mode 3: Zone Hold — enter after zone proves it's holding ──
    # Touch candle confirmed (close_confirmed checked above).
    # Now watch next HOLD_BARS candles: if ALL hold the zone, enter
    # at the last hold candle's close (a real, achievable price).
    if mode == MODE_ZONE_CONF:
        HOLD_BARS = 2  # wait 2 candles after touch
        touch_idx = raw["c1_b1_idx"]

        for h in range(1, HOLD_BARS + 1):
            check_idx = touch_idx + h
            if check_idx >= len(b1):
                return None  # not enough data
            candle = b1[check_idx]
            if side == "bull" and candle["close"] < raw["zone_bot"]:
                return None  # zone failed
            if side == "bear" and candle["close"] > raw["zone_top"]:
                return None  # zone failed

        # Zone held — enter at last hold candle's close
        last_idx = touch_idx + HOLD_BARS
        ep = b1[last_idx]["close"]
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
        entry_ns = b1[last_idx]["time_ns"] + NS_MIN
        return _build_sig(raw, ep, sp, risk, risk_d, entry_ns, mode, b5, ds5,
                          liq_levels, dr15, b15, d)

    # ── Mode 4: 1m FVG Confluence ──
    if mode == MODE_FVG_1M:
        fvg_ep, fvg_bar_idx = find_1m_fvg(
            b1, raw["c1_b1_idx"], side,
            raw["zone_top"], raw["zone_bot"],
        )
        if fvg_ep is None:
            return None
        ep = fvg_ep
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
        # Entry time is at the FVG retest bar
        entry_ns = b1[fvg_bar_idx]["time_ns"] + NS_MIN
        return _build_sig(raw, ep, sp, risk, risk_d, entry_ns, mode, b5, ds5,
                          liq_levels, dr15, b15, d)

    # ── Mode 5: RSI + Zone Confirmed ──
    if mode == MODE_RSI_ZONE:
        rsi = calc_rsi(b1, raw["c1_b1_idx"], period=14)
        # Bull: RSI < 40 (oversold at demand zone)
        # Bear: RSI > 60 (overbought at supply zone)
        if side == "bull" and rsi >= 40:
            return None
        if side == "bear" and rsi <= 60:
            return None
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
        sig = _build_sig(raw, ep, sp, risk, risk_d, raw["ns"], mode, b5, ds5,
                         liq_levels, dr15, b15, d)
        if sig:
            sig["rsi"] = rsi
        return sig

    return None


def _build_sig(raw, ep, sp, risk, risk_d, entry_ns, mode,
               b5, ds5, liq_levels, dr15, b15, d):
    """Build the final trade signal dict with scoring + RR."""
    side = raw["side"]
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
    if b15 is not None and dr15 is not None and d is not None:
        struct = structure_15m(b15, dr15, d, entry_ns)
        if struct == side:
            score += 2
    sw15 = False
    if b15 is not None and dr15 is not None and d is not None:
        sw15 = sweep_15m(b15, dr15, d, entry_ns, liq_levels, side)
        if sw15:
            score += 1

    # 4T-v3 RR
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

    entry_dt = datetime.fromtimestamp(entry_ns / 1e9, tz=CT)
    return {
        "date": d, "side": side, "entry": ep, "stop": sp,
        "risk_pts": risk, "risk_$": risk_d,
        "score": score, "rr": rr, "zone": raw["zt"],
        "time": entry_dt, "mode": mode,
        "has_cisd": has_cisd, "has_struct": has_struct,
        "has_sweep": has_sweep, "has_rej": bool(raw.get("rej")),
        "zone_top": raw["zone_top"], "zone_bot": raw["zone_bot"],
        "c1_close": raw["c1_close"],
        "dist_from_zone": abs(ep - (raw["zone_top"] if side == "bull" else raw["zone_bot"])),
        "disp_high": raw.get("disp_high"),
        "disp_low": raw.get("disp_low"),
    }


# ═══════════════════════════════════════════════════════════════
# TRADE SIMULATION (walk 1m bars forward)
# ═══════════════════════════════════════════════════════════════
def simulate_trades(sigs, b1):
    """Simulate trades with zone dedup, MCL, cooldown — like the bot."""
    sigs = sorted(sigs, key=lambda x: x["time"])
    trades = []
    in_position = False
    pos_exit_time = None
    cooldown_until = None
    current_day = None
    cl_bull = 0
    cl_bear = 0
    used_zones = set()
    day_pnl = 0.0

    for s in sigs:
        if s["date"] != current_day:
            current_day = s["date"]
            in_position = False
            pos_exit_time = None
            cooldown_until = None
            cl_bull = 0
            cl_bear = 0
            used_zones = set()
            day_pnl = 0.0

        entry_time = s["time"]
        if in_position:
            if pos_exit_time and entry_time >= pos_exit_time:
                in_position = False
            else:
                continue
        if cooldown_until and entry_time < cooldown_until:
            continue
        zone_key = (s["side"], s["zone"], s["zone_top"], s["zone_bot"])
        if zone_key in used_zones:
            continue
        if s["side"] == "bull" and cl_bull >= MCL:
            continue
        if s["side"] == "bear" and cl_bear >= MCL:
            continue

        risk_pts = s["risk_pts"]
        if s["side"] == "bull":
            target = s["entry"] + risk_pts * s["rr"]
        else:
            target = s["entry"] - risk_pts * s["rr"]

        entry_ns = int(entry_time.timestamp() * 1e9)
        result = "OPEN"
        exit_time = None
        pnl = 0.0

        for bar in b1:
            if bar["time_ns"] <= entry_ns:
                continue
            if s["side"] == "bull":
                if bar["low"] <= s["stop"]:
                    result = "LOSS"
                    exit_time = datetime.fromtimestamp(bar["time_ns"] / 1e9, tz=CT)
                    pnl = (s["stop"] - s["entry"]) * PV * CONTRACTS
                    break
                if bar["high"] >= target:
                    result = "WIN"
                    exit_time = datetime.fromtimestamp(bar["time_ns"] / 1e9, tz=CT)
                    pnl = (target - s["entry"]) * PV * CONTRACTS
                    break
            else:
                if bar["high"] >= s["stop"]:
                    result = "LOSS"
                    exit_time = datetime.fromtimestamp(bar["time_ns"] / 1e9, tz=CT)
                    pnl = (s["entry"] - s["stop"]) * PV * CONTRACTS
                    break
                if bar["low"] <= target:
                    result = "WIN"
                    exit_time = datetime.fromtimestamp(bar["time_ns"] / 1e9, tz=CT)
                    pnl = (s["entry"] - target) * PV * CONTRACTS
                    break

        pnl -= FEES_RT
        used_zones.add(zone_key)

        if exit_time:
            in_position = True
            pos_exit_time = exit_time
            cooldown_until = exit_time + timedelta(seconds=COOLDOWN_S)
        else:
            in_position = True
            pos_exit_time = datetime.now(CT)

        if result == "LOSS":
            if s["side"] == "bull": cl_bull += 1
            else: cl_bear += 1
        elif result == "WIN":
            if s["side"] == "bull": cl_bull = 0
            else: cl_bear = 0

        day_pnl += pnl
        trades.append({
            **s, "target": target, "result": result,
            "exit_time": exit_time, "pnl": pnl,
        })
    return trades


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    t0 = time.time()
    print("=" * 70)
    print("ENTRY MODE COMPARISON — V106 ICT Strategy")
    print("  1. Zone Limit    — first touch, no close confirmation")
    print("  2. 1m Close      — enter at 1m close (production)")
    print("  3. Zone+Confirm  — zone price, wait for close confirmation")
    print("  4. 1m FVG        — zone confirmed → 1m FVG → retest entry")
    print("  5. RSI+Zone      — zone confirmed + RSI filter (< 40 bull / > 60 bear)")
    print("=" * 70)

    # ── Config ──
    start = datetime(2026, 3, 10, 0, 0, 0, tzinfo=CT)
    end = datetime(2026, 3, 20, 0, 0, 0, tzinfo=CT)
    lookback = start - timedelta(days=7)
    SIM_START_HOUR = 7
    SIM_START_MIN = 30

    # ── Auth + fetch ──
    token, token_time = authenticate()
    if not token:
        print("Auth failed!")
        sys.exit(1)
    api = APIClient(initial_token=token, token_acquired_at=token_time)
    print("Authenticated.\n")

    print("Fetching 5m bars...")
    b5 = fetch_with_rollover(api, 5, lookback, end)
    print(f"  5m: {len(b5)} bars")

    print("Fetching 15m bars...")
    b15 = fetch_with_rollover(api, 15, lookback, end)
    print(f"  15m: {len(b15)} bars")

    print("Fetching 1m bars...")
    b1 = fetch_with_rollover(api, 1, lookback, end)
    print(f"  1m: {len(b1)} bars")

    dr5 = build_dr(b5)
    dr15 = build_dr_htf(b15)
    all_dates = sorted(dr5.keys())
    target_start = start.date()
    trade_dates = [d for d in all_dates if d >= target_start]

    print(f"\nDates: {[str(d) for d in all_dates]}")
    print(f"Trading: {[str(d) for d in trade_dates]}")
    print()

    # ── Generate enriched signals ──
    print("Generating enriched signals...")
    all_raw = []  # enriched entries from gen_sweep_entries_enriched
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
                b5[:cursor + 1], b1[:b1_cutoff], ds5, cursor, d, liq)

            for e in sorted(ents, key=lambda x: (x["ns"], -{"ifvg": 2, "disp_fvg": 1}.get(x["zt"], 0))):
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

    print(f"  Raw enriched entries: {len(all_raw)}")
    confirmed = sum(1 for e in all_raw if e["close_confirmed"])
    print(f"  Close confirmed: {confirmed} / {len(all_raw)}")

    # ── Apply each entry mode ──
    mode_sigs = {m: [] for m in ALL_MODES}

    for raw in all_raw:
        for mode in ALL_MODES:
            sig = apply_entry_mode(
                raw, mode, b1, b5, raw["_ds5"],
                raw["_liq"], dr15, b15, raw["_date"],
            )
            if sig:
                mode_sigs[mode].append(sig)

    # ── Print signal counts ──
    print(f"\n{'='*70}")
    print("SIGNALS PER MODE (before trade sim)")
    print(f"{'='*70}")
    for mode in ALL_MODES:
        sigs = mode_sigs[mode]
        passes = sum(1 for s in sigs if s["risk_$"] <= MAX_RISK)
        print(f"  {MODE_NAMES[mode]:<20} {len(sigs):>4} signals ({passes} pass risk)")

    # ── Simulate trades for each mode ──
    mode_trades = {}
    for mode in ALL_MODES:
        mode_trades[mode] = simulate_trades(mode_sigs[mode], b1)

    # ── Print detailed trades per mode ──
    for mode in ALL_MODES:
        trades = mode_trades[mode]
        print(f"\n{'='*70}")
        print(f"TRADES — {MODE_NAMES[mode]}")
        print(f"{'='*70}")
        if not trades:
            print("  (no trades)")
            continue
        for t in trades:
            exit_str = f"exit {t['exit_time'].strftime('%H:%M')}" if t.get("exit_time") else "OPEN"
            dist_str = f"dist={t.get('dist_from_zone', 0):.1f}pt" if "dist_from_zone" in t else ""
            rsi_str = f"rsi={t.get('rsi', 0):.0f}" if "rsi" in t else ""
            extra = " ".join(filter(None, [dist_str, rsi_str]))
            print(f"  {t['date']} {t['time'].strftime('%H:%M')} | {t['side'].upper():4s} "
                  f"@ {t['entry']:.2f} | SL {t['stop']:.2f} TP {t['target']:.2f} "
                  f"| risk {t['risk_pts']:.1f}pt ${t['risk_$']:.0f} "
                  f"| sc={t['score']} rr={t['rr']} | {t['zone']} "
                  f"| {t['result']} {exit_str} ${t['pnl']:+,.0f} | {extra}")

    # ── COMPARISON TABLE ──
    print(f"\n{'='*70}")
    print("COMPARISON TABLE")
    print(f"{'='*70}")
    print(f"  {'Mode':<20} {'Tr':>4} {'W':>3} {'L':>3} {'WR%':>6} "
          f"{'TotalPnL':>10} {'AvgWin':>8} {'AvgLoss':>8} {'PF':>5}")
    print(f"  {'-'*65}")

    for mode in ALL_MODES:
        trades = mode_trades[mode]
        if not trades:
            print(f"  {MODE_NAMES[mode]:<20} {'—':>4}")
            continue
        wins = [t for t in trades if t["result"] == "WIN"]
        losses = [t for t in trades if t["result"] == "LOSS"]
        total_pnl = sum(t["pnl"] for t in trades)
        wr = 100 * len(wins) / len(trades) if trades else 0
        avg_w = sum(t["pnl"] for t in wins) / len(wins) if wins else 0
        avg_l = sum(t["pnl"] for t in losses) / len(losses) if losses else 0
        gross_w = sum(t["pnl"] for t in wins)
        gross_l = abs(sum(t["pnl"] for t in losses))
        pf = gross_w / gross_l if gross_l > 0 else float('inf')
        print(f"  {MODE_NAMES[mode]:<20} {len(trades):>4} {len(wins):>3} {len(losses):>3} "
              f"{wr:>5.1f}% ${total_pnl:>9,.0f} ${avg_w:>7,.0f} ${avg_l:>7,.0f} {pf:>5.2f}")

    # ── Daily breakdown per mode ──
    print(f"\n{'='*70}")
    print("DAILY P&L BY MODE")
    print(f"{'='*70}")
    header = f"  {'Date':<12}"
    for mode in ALL_MODES:
        header += f" {MODE_NAMES[mode][:10]:>10}"
    print(header)
    print(f"  {'-'*12}" + f" {'-'*10}" * len(ALL_MODES))

    all_trade_dates = set()
    for mode in ALL_MODES:
        for t in mode_trades[mode]:
            all_trade_dates.add(t["date"])

    for d in sorted(all_trade_dates):
        row = f"  {str(d):<12}"
        for mode in ALL_MODES:
            day_pnl = sum(t["pnl"] for t in mode_trades[mode] if t["date"] == d)
            day_trades = [t for t in mode_trades[mode] if t["date"] == d]
            if day_trades:
                row += f" ${day_pnl:>8,.0f}"
            else:
                row += f" {'—':>10}"
        print(row)

    # Totals row
    row = f"  {'TOTAL':<12}"
    for mode in ALL_MODES:
        total = sum(t["pnl"] for t in mode_trades[mode])
        row += f" ${total:>8,.0f}"
    print(f"  {'='*12}" + f" {'='*10}" * len(ALL_MODES))
    print(row)

    # ── Average distance from zone ──
    print(f"\n{'='*70}")
    print("ENTRY QUALITY — Avg distance from zone edge at entry")
    print(f"{'='*70}")
    for mode in ALL_MODES:
        trades = mode_trades[mode]
        if not trades:
            continue
        avg_dist = sum(t.get("dist_from_zone", 0) for t in trades) / len(trades)
        win_dist = ([t.get("dist_from_zone", 0) for t in trades if t["result"] == "WIN"])
        loss_dist = ([t.get("dist_from_zone", 0) for t in trades if t["result"] == "LOSS"])
        avg_win_dist = sum(win_dist) / len(win_dist) if win_dist else 0
        avg_loss_dist = sum(loss_dist) / len(loss_dist) if loss_dist else 0
        print(f"  {MODE_NAMES[mode]:<20} avg={avg_dist:>5.1f}pt | "
              f"wins={avg_win_dist:>5.1f}pt | losses={avg_loss_dist:>5.1f}pt")

    print(f"\n  Completed in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
