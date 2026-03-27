"""V106 — Dynamic RR by Score + 15m Sweep Confluence
=====================================================
V104 base + V105 confluences + NEW:
  - Dynamic RR: higher score → higher RR target
  - 15m sweep: +1 if liquidity swept on 15m timeframe
"""
import os, sys, time
from collections import defaultdict
from datetime import datetime, date as dt_date

# Backtest-only imports — bot imports this as a module, so __name__ != "__main__"
if __name__ == "__main__":
    import os
    os.chdir("/Users/gtrades/trading-bot")
    sys.stdout.reconfigure(line_buffering=True)
    import numpy as np
    from ict_v10_combined_3 import in_kz, tick_start, COMMISSION
    from bear_v46_combo import _build_dr_htf
    from bear_explore2 import load_full
    from tick_engine import calc_stats
else:
    # Lightweight fallback for bot (no ict_v10_combined_3 dependency)
    COMMISSION = 4.5

    def in_kz(h, m, kz_list):
        if not kz_list:  # empty list = all hours pass
            return True
        t = h * 60 + m
        for (sh, sm), (eh, em) in kz_list:
            s = sh * 60 + sm
            e = eh * 60 + em
            if s > e:  # overnight wrap (e.g. 17:00-02:00)
                if t >= s or t < e:
                    return True
            else:
                if s <= t < e:
                    return True
        return False

NS_MIN = 60_000_000_000
INST = {"NQ": {"pv": 20, "slip": 0.5, "mf": 2.0}}
_G = {}
KZ = [((7, 30), (14, 30))]

# ═══════════════════════════════════════════════════════════════
# LIQUIDITY LEVELS
# ═══════════════════════════════════════════════════════════════
def get_liquidity_levels(b5, dr5, d, all_dates):
    levels = []
    sorted_dates = sorted(all_dates)
    d_idx = None
    for i, dd in enumerate(sorted_dates):
        if dd == d: d_idx = i; break
    if d_idx is None: return levels
    if d_idx > 0:
        prev_d = sorted_dates[d_idx - 1]
        if prev_d in dr5:
            ds, de = dr5[prev_d]; pdh = pdl = None
            for i in range(ds, de):
                if pdh is None or b5[i]["high"] > pdh: pdh = b5[i]["high"]
                if pdl is None or b5[i]["low"] < pdl: pdl = b5[i]["low"]
            if pdh: levels.append((pdh, "pdh"))
            if pdl: levels.append((pdl, "pdl"))
    if d_idx > 1:
        prev2_d = sorted_dates[d_idx - 2]
        if prev2_d in dr5:
            ds, de = dr5[prev2_d]; p2h = p2l = None
            for i in range(ds, de):
                if p2h is None or b5[i]["high"] > p2h: p2h = b5[i]["high"]
                if p2l is None or b5[i]["low"] < p2l: p2l = b5[i]["low"]
            if p2h: levels.append((p2h, "p2h"))
            if p2l: levels.append((p2l, "p2l"))
    if d_idx > 0:
        prev_d = sorted_dates[d_idx - 1]
        if prev_d in dr5:
            ds, de = dr5[prev_d]; ah = al = None
            for i in range(ds, de):
                if 18 <= b5[i]["hour"] <= 23:
                    if ah is None or b5[i]["high"] > ah: ah = b5[i]["high"]
                    if al is None or b5[i]["low"] < al: al = b5[i]["low"]
            if ah: levels.append((ah, "asia_h"))
            if al: levels.append((al, "asia_l"))
    if d in dr5:
        ds, de = dr5[d]; lh = ll = pmh = pml = None
        for i in range(ds, de):
            if 0 <= b5[i]["hour"] <= 3:
                if lh is None or b5[i]["high"] > lh: lh = b5[i]["high"]
                if ll is None or b5[i]["low"] < ll: ll = b5[i]["low"]
            if 4 <= b5[i]["hour"] <= 7:
                if pmh is None or b5[i]["high"] > pmh: pmh = b5[i]["high"]
                if pml is None or b5[i]["low"] < pml: pml = b5[i]["low"]
        if lh: levels.append((lh, "london_h"))
        if ll: levels.append((ll, "london_l"))
        if pmh: levels.append((pmh, "premarket_h"))
        if pml: levels.append((pml, "premarket_l"))
        for i in range(ds + 2, de):
            if b5[i]["hour"] >= 8: break
            if b5[i]["hour"] < 4: continue
            if i + 1 >= len(b5): break
            if b5[i]["high"] > b5[i-1]["high"] and b5[i]["high"] > b5[i+1]["high"]:
                levels.append((b5[i]["high"], "pre_sh"))
            if b5[i]["low"] < b5[i-1]["low"] and b5[i]["low"] < b5[i+1]["low"]:
                levels.append((b5[i]["low"], "pre_sl"))
    if d_idx > 0:
        prev_d = sorted_dates[d_idx - 1]
        if prev_d in dr5:
            ds, de = dr5[prev_d]; day_shs = []; day_sls = []
            for i in range(ds + 1, de):
                if i + 1 >= len(b5): break
                if not (8 <= b5[i]["hour"] <= 14): continue
                if b5[i]["high"] > b5[i-1]["high"] and b5[i]["high"] > b5[i+1]["high"]:
                    day_shs.append(b5[i]["high"])
                if b5[i]["low"] < b5[i-1]["low"] and b5[i]["low"] < b5[i+1]["low"]:
                    day_sls.append(b5[i]["low"])
            if day_shs: levels.append((max(day_shs), "prev_ses_sh"))
            if day_sls: levels.append((min(day_sls), "prev_ses_sl"))
    # ── 15m FVG levels (HTF key levels — Blake uses these as draw on liquidity) ──
    if os.environ.get("BACKTEST_HTF_FVG") == "1":
        # Find active 15m FVGs from yesterday + today pre-session
        # These act as key levels that attract price
        if d_idx > 0:
            prev_d = sorted_dates[d_idx - 1]
            if prev_d in dr5:
                ds, de = dr5[prev_d]
                for i in range(ds + 1, min(de - 1, len(b5) - 1)):
                    if i < 1 or i + 1 >= len(b5): continue
                    p, c, n = b5[i-1], b5[i], b5[i+1]
                    # Bullish 15m-equivalent FVG (using 5m bars, 3-bar = 15m)
                    if i >= 2 and i + 2 < len(b5):
                        # Check 3-bar gap (approximately 15m)
                        b_prev = b5[i-2]
                        b_next = b5[i+2]
                        bull_gap = b_next["low"] - b_prev["high"]
                        bear_gap = b_prev["low"] - b_next["high"]
                        if bull_gap > 2.0:  # significant bullish gap
                            levels.append((b_next["low"], "htf_fvg_top"))
                            levels.append((b_prev["high"], "htf_fvg_bot"))
                        if bear_gap > 2.0:  # significant bearish gap
                            levels.append((b_prev["low"], "htf_fvg_top"))
                            levels.append((b_next["high"], "htf_fvg_bot"))

    levels.sort(key=lambda x: x[0])
    deduped = []
    for lvl, tag in levels:
        if not deduped or abs(lvl - deduped[-1][0]) > 2.0:
            deduped.append((lvl, tag))
    return deduped

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def detect_sweep_at(b5, idx, levels, lookback=12):
    for i in range(idx, max(0, idx - lookback) - 1, -1):
        if i >= len(b5): continue
        bar = b5[i]; br = bar["high"] - bar["low"]
        if br <= 0: continue
        for lp, lt in levels:
            if lp is None: continue
            if bar["low"] < lp and bar["close"] > lp:
                if (lp - bar["low"]) / br >= 0.1: return "bull", lp, i
            if bar["high"] > lp and bar["close"] < lp:
                if (bar["high"] - lp) / br >= 0.1: return "bear", lp, i
    return None, None, None

def cisd_5m(b5, idx, ds, lookback=30, recency=10):
    s = max(ds + 2, idx - lookback)
    if idx - s < 8: return None
    shs = []; sls = []
    for i in range(s + 1, idx):
        if i + 1 >= len(b5): break
        if b5[i]["high"] > b5[i-1]["high"] and b5[i]["high"] > b5[i+1]["high"]:
            shs.append((b5[i]["high"], i))
        if b5[i]["low"] < b5[i-1]["low"] and b5[i]["low"] < b5[i+1]["low"]:
            sls.append((b5[i]["low"], i))
    if len(shs) < 2 or len(sls) < 2: return None
    if sls[-1][0] < sls[-2][0]:
        lvl = shs[-1][0]
        for i in range(shs[-1][1]+1, idx+1):
            if i >= len(b5): break
            if b5[i]["close"] > lvl:
                if idx - i <= recency: return "bull"
                break
    if shs[-1][0] > shs[-2][0]:
        lvl = sls[-1][0]
        for i in range(sls[-1][1]+1, idx+1):
            if i >= len(b5): break
            if b5[i]["close"] < lvl:
                if idx - i <= recency: return "bear"
                break
    return None

def structure_15m(b15, dr15, d, entry_ns):
    if d not in dr15: return None
    ds, de = dr15[d]
    end_idx = ds
    for i in range(ds, de):
        if i >= len(b15): break
        if b15[i]["time_ns"] > entry_ns: break
        end_idx = i
    lookback_start = max(ds, end_idx - 60)
    if end_idx - lookback_start < 6: return None
    shs = []; sls = []
    for i in range(lookback_start + 1, end_idx):
        if i + 1 >= len(b15): break
        if b15[i]["high"] > b15[i-1]["high"] and b15[i]["high"] > b15[i+1]["high"]:
            shs.append((b15[i]["high"], i))
        if b15[i]["low"] < b15[i-1]["low"] and b15[i]["low"] < b15[i+1]["low"]:
            sls.append((b15[i]["low"], i))
    if len(shs) < 2 or len(sls) < 2: return None
    if sls[-1][0] > sls[-2][0]:
        last_sh = shs[-1][0]
        for i in range(shs[-1][1] + 1, end_idx + 1):
            if i >= len(b15): break
            if b15[i]["close"] > last_sh: return "bull"
    if shs[-1][0] < shs[-2][0]:
        last_sl = sls[-1][0]
        for i in range(sls[-1][1] + 1, end_idx + 1):
            if i >= len(b15): break
            if b15[i]["close"] < last_sl: return "bear"
    return None

# ═══════════════════════════════════════════════════════════════
# NEW: 15m SWEEP CONFLUENCE
# ═══════════════════════════════════════════════════════════════
def sweep_15m(b15, dr15, d, entry_ns, liq_levels, direction, lookback=8):
    """Check if there was a sweep on 15m bars near entry time matching direction."""
    if d not in dr15: return False
    ds, de = dr15[d]
    # Find 15m bar at entry time
    entry_idx = None
    for i in range(ds, de):
        if i >= len(b15): break
        if b15[i]["time_ns"] > entry_ns: break
        entry_idx = i
    if entry_idx is None: return False

    # Check for sweep in recent 15m bars
    for i in range(entry_idx, max(ds, entry_idx - lookback) - 1, -1):
        if i >= len(b15): continue
        bar = b15[i]; br = bar["high"] - bar["low"]
        if br <= 0: continue
        for lp, lt in liq_levels:
            if lp is None: continue
            if direction == "bull":
                if bar["low"] < lp and bar["close"] > lp:
                    if (lp - bar["low"]) / br >= 0.1: return True
            elif direction == "bear":
                if bar["high"] > lp and bar["close"] < lp:
                    if (bar["high"] - lp) / br >= 0.1: return True
    return False

# ═══════════════════════════════════════════════════════════════
# SWEEP ENTRY GENERATOR (from V104)
# ═══════════════════════════════════════════════════════════════
def gen_sweep_entries(b5, b1, ds5, de5, d, liq_levels, stop_b5=None, kz=None):
    """kz=None → default NY KZ. kz=[] → no filter (all hours). kz=[(s,e),...] → custom."""
    if stop_b5 is None:
        stop_b5 = b5
    if kz is None:
        kz = KZ  # default NY [(7,30)-(14,30)]
    entries = []
    used = set()
    session_shs = []; session_sls = []
    # FIX 2: Track pending swings — only confirm after i+1 closes
    pending_sh = None; pending_sl = None
    for i in range(ds5, de5):
        if i >= len(b5): break
        # Confirm pending swings from previous bar (now i+1 has closed relative to the pending bar)
        if pending_sh is not None:
            session_shs.append(pending_sh); pending_sh = None
        if pending_sl is not None:
            session_sls.append(pending_sl); pending_sl = None
        # Check if PREVIOUS bar (i-1) is a swing — we now have i as confirmation
        # Session swings detected at all hours (overnight sessions need them too)
        if i > ds5 + 1:
            if b5[i-1]["high"] > b5[i-2]["high"] and b5[i-1]["high"] > b5[i]["high"]:
                pending_sh = (b5[i-1]["high"], "ses_sh")
            if b5[i-1]["low"] < b5[i-2]["low"] and b5[i-1]["low"] < b5[i]["low"]:
                pending_sl = (b5[i-1]["low"], "ses_sl")
        if not in_kz(b5[i]["hour"], b5[i]["minute"], kz): continue
        live_levels = liq_levels + session_shs[-6:] + session_sls[-6:]
        sw_dir, sw_lvl, sw_bar = detect_sweep_at(b5, i, live_levels, lookback=12)
        if sw_dir is None: continue
        disp_idx = None
        for j in range(sw_bar, min(sw_bar + 6, len(b5))):
            bar = b5[j]; body = abs(bar["close"] - bar["open"]); rng = bar["high"] - bar["low"]
            if rng <= 0: continue
            if body / rng >= 0.35:
                if sw_dir == "bull" and bar["close"] > bar["open"]: disp_idx = j; break
                if sw_dir == "bear" and bar["close"] < bar["open"]: disp_idx = j; break
        if disp_idx is None or disp_idx in used: continue
        zones = []
        entries_from_this = 0

        # ── Multi-TF IFVG (PB Blake model) ──
        # Find 1m bars for the sweep→displacement leg
        disp_ns = b5[disp_idx]["time_ns"]
        sweep_ns = b5[sw_bar]["time_ns"]
        leg_b1_start = leg_b1_end = None
        for ki in range(len(b1)):
            if b1[ki]["time_ns"] >= sweep_ns - 15 * NS_MIN and leg_b1_start is None:
                leg_b1_start = ki
            if b1[ki]["time_ns"] >= disp_ns:
                leg_b1_end = ki
                break
        ifvg_zone = None
        if leg_b1_start is not None and leg_b1_end is not None and leg_b1_end > leg_b1_start + 2:
            leg_1m = b1[leg_b1_start:leg_b1_end]

            def _build_nm(bars_1m, n):
                if n == 1: return list(bars_1m)
                out = []
                for ii in range(0, len(bars_1m) - n + 1, n):
                    grp = bars_1m[ii:ii+n]
                    if len(grp) < n: break
                    out.append({"open": grp[0]["open"], "high": max(b["high"] for b in grp),
                                "low": min(b["low"] for b in grp), "close": grp[-1]["close"],
                                "time_ns": grp[0]["time_ns"]})
                return out

            def _find_fvgs(bars, direction, min_gap=0.5, max_width=12.0):
                fvgs = []
                for kk in range(len(bars) - 2):
                    pp, cc, nn = bars[kk], bars[kk+1], bars[kk+2]
                    if direction == "bull":
                        gap = pp["low"] - nn["high"]
                        if gap < min_gap: continue
                        ft, fb = pp["low"], nn["high"]
                    else:
                        gap = nn["low"] - pp["high"]
                        if gap < min_gap: continue
                        ft, fb = nn["low"], pp["high"]
                    if (ft - fb) > max_width: continue
                    bad = False
                    for jj in range(kk+3, len(bars)):
                        if direction == "bull" and bars[jj]["close"] < fb: bad = True; break
                        if direction == "bear" and bars[jj]["close"] > ft: bad = True; break
                    if not bad: fvgs.append({"top": ft, "bot": fb})
                return fvgs

            # Check 5m→1m, use highest TF with singular FVG
            for tf in [5, 4, 3, 2, 1]:
                tf_bars = _build_nm(leg_1m, tf)
                if len(tf_bars) < 3: continue
                fvgs = _find_fvgs(tf_bars, sw_dir)
                if len(fvgs) == 1:
                    ifvg_zone = fvgs[0]
                    break
            # Fallback: highest TF with any FVGs
            if ifvg_zone is None:
                for tf in [5, 4, 3, 2, 1]:
                    tf_bars = _build_nm(leg_1m, tf)
                    if len(tf_bars) < 3: continue
                    fvgs = _find_fvgs(tf_bars, sw_dir)
                    if fvgs:
                        if sw_dir == "bull":
                            ifvg_zone = max(fvgs, key=lambda f: f["top"])
                        else:
                            ifvg_zone = min(fvgs, key=lambda f: f["bot"])
                        break

            if ifvg_zone is not None:
                # IFVG entry = inversion (body close through the FVG)
                # Scan 1m from displacement onward for inverting candle
                inv_level = ifvg_zone["top"] if sw_dir == "bull" else ifvg_zone["bot"]
                for ki in range(leg_b1_end, min(leg_b1_end + 20, len(b1))):
                    if not in_kz(b1[ki]["hour"], b1[ki]["minute"], KZ): continue
                    c1 = b1[ki]
                    body = abs(c1["close"] - c1["open"])
                    rng = c1["high"] - c1["low"]
                    if rng <= 0: continue
                    inverted = False
                    if sw_dir == "bull" and c1["close"] > c1["open"] and c1["close"] > inv_level + 0.5 and body/rng >= 0.35:
                        inverted = True
                    elif sw_dir == "bear" and c1["close"] < c1["open"] and c1["close"] < inv_level - 0.5 and body/rng >= 0.35:
                        inverted = True
                    if not inverted: continue
                    # Stop at leg extreme
                    leg_range = range(sw_bar, min(disp_idx + 1, len(b5)))
                    if sw_dir == "bull":
                        leg_ext = min(b5[mm]["low"] for mm in leg_range) if leg_range else c1["low"]
                        sp_ifvg = min(sw_lvl, leg_ext) - 1.0
                    else:
                        leg_ext = max(b5[mm]["high"] for mm in leg_range) if leg_range else c1["high"]
                        sp_ifvg = max(sw_lvl, leg_ext) + 1.0
                    ep_ifvg = c1["close"]
                    risk_ifvg = abs(ep_ifvg - sp_ifvg)
                    if risk_ifvg < 1.0: break
                    rej_ifvg = False
                    entries.append({"ep": ep_ifvg, "sp": sp_ifvg, "ns": c1["time_ns"] + NS_MIN,
                                    "rej": rej_ifvg, "hour": c1["hour"], "bar_idx": disp_idx,
                                    "side": sw_dir, "zt": "ifvg", "swept": sw_lvl, "sw_bar": sw_bar,
                                    "zone_top": ifvg_zone["top"], "zone_bot": ifvg_zone["bot"]})
                    used.add(disp_idx)
                    entries_from_this += 1
                    break
        for k in range(sw_bar, min(disp_idx + 2, len(b5))):
            if k < 1 or k + 1 >= len(b5): continue
            p, c, n = b5[k-1], b5[k], b5[k+1]
            if sw_dir == "bull":
                gap = n["low"] - p["high"]
                if gap > 0:
                    ft, fb = n["low"], p["high"]
                    mit = any(b5[j]["close"] < fb for j in range(k+2, len(b5)))
                    if not mit: zones.append({"top": ft, "bot": fb, "ce": (ft+fb)/2, "type": "disp_fvg"})
            else:
                gap = p["low"] - n["high"]
                if gap > 0:
                    ft, fb = p["low"], n["high"]
                    mit = any(b5[j]["close"] > ft for j in range(k+2, len(b5)))
                    if not mit: zones.append({"top": ft, "bot": fb, "ce": (ft+fb)/2, "type": "disp_fvg"})
        if not zones and entries_from_this == 0: continue
        zones.sort(key=lambda z: (0 if z["type"] == "ifvg" else 1))
        touched_zones = set()
        for zi, z in enumerate(zones[:4]):
            for j in range(max(disp_idx+1, i+1), min(disp_idx+60, de5)):
                if j >= len(b5) or j in used: continue
                if not in_kz(b5[j]["hour"], b5[j]["minute"], kz): continue
                bar = b5[j]; touched = False
                if sw_dir == "bull" and bar["low"] <= z["top"] and bar["close"] >= z["bot"]: touched = True
                elif sw_dir == "bear" and bar["high"] >= z["bot"] and bar["close"] <= z["top"]: touched = True
                if not touched: continue
                ns0 = bar["time_ns"]; ns1 = ns0 + 5*NS_MIN; first_touch = None
                for k in range(len(b1)):
                    if b1[k]["time_ns"] >= ns1: break
                    if b1[k]["time_ns"] < ns0: continue
                    c1 = b1[k]
                    if sw_dir == "bull" and c1["low"] <= z["top"] and c1["close"] >= z["bot"]:
                        ep = c1["close"]; stop_end = min(j+1, len(stop_b5))
                        pl = min(stop_b5[m]["low"] for m in range(max(disp_idx+1,sw_bar), stop_end) if m < len(stop_b5)) if stop_end > max(disp_idx+1,sw_bar) else c1["low"]
                        sp = min(sw_lvl, pl, c1["low"]) - 1.0; risk = ep - sp
                        if risk <= 0: continue
                        rej = c1["low"] < z["ce"] and min(c1["open"],c1["close"]) >= z["bot"]
                        first_touch = (ep, sp, c1["time_ns"]+NS_MIN, rej, c1["hour"], j, sw_bar)
                        break
                    elif sw_dir == "bear" and c1["high"] >= z["bot"] and c1["close"] <= z["top"]:
                        ep = c1["close"]; stop_end = min(j+1, len(stop_b5))
                        ph = max(stop_b5[m]["high"] for m in range(max(disp_idx+1,sw_bar), stop_end) if m < len(stop_b5)) if stop_end > max(disp_idx+1,sw_bar) else c1["high"]
                        sp = max(sw_lvl, ph, c1["high"]) + 1.0; risk = sp - ep
                        if risk <= 0: continue
                        rej = c1["high"] > z["ce"] and max(c1["open"],c1["close"]) <= z["top"]
                        first_touch = (ep, sp, c1["time_ns"]+NS_MIN, rej, c1["hour"], j, sw_bar)
                        break  # first touch = entry
                if first_touch:
                    entries.append({"ep": first_touch[0], "sp": first_touch[1], "ns": first_touch[2], "rej": first_touch[3],
                                    "hour": first_touch[4], "bar_idx": first_touch[5], "side": sw_dir,
                                    "zt": z["type"], "swept": sw_lvl, "sw_bar": first_touch[6],
                                    "zone_top": z["top"], "zone_bot": z["bot"]})
                    used.add(first_touch[5])
                    entries_from_this += 1
                    touched_zones.add(zi)
                    break
            if entries_from_this >= 2: break
        # If no touch on completed 5m bars, check 1m bars beyond last 5m bar
        # Same logic — drop to 1m, first touch = entry
        if entries_from_this < 2 and b5 and b1:
            last_5m_end = b5[de5-1]["time_ns"] + 5*NS_MIN if de5 > 0 else 0
            # Find start index in b1 (skip bars before last 5m end)
            b1_start = 0
            for _ki in range(len(b1)-1, -1, -1):
                if b1[_ki]["time_ns"] < last_5m_end:
                    b1_start = _ki + 1; break
            for zi, z in enumerate(zones[:4]):
                if entries_from_this >= 2: break
                if zi in touched_zones: continue
                for k in range(b1_start, len(b1)):
                    if not in_kz(b1[k]["hour"], b1[k]["minute"], kz): continue
                    c1 = b1[k]; first_touch = None
                    if sw_dir == "bull" and c1["low"] <= z["top"] and c1["close"] >= z["bot"]:
                        ep = c1["close"]; stop_end = min(de5, len(stop_b5))
                        pl = min(stop_b5[m]["low"] for m in range(max(disp_idx+1,sw_bar), stop_end) if m < len(stop_b5)) if stop_end > max(disp_idx+1,sw_bar) else c1["low"]
                        sp = min(sw_lvl, pl, c1["low"]) - 1.0; risk = ep - sp
                        if risk <= 0: continue
                        rej = c1["low"] < z["ce"] and min(c1["open"],c1["close"]) >= z["bot"]
                        first_touch = (ep, sp, c1["time_ns"]+NS_MIN, rej, c1["hour"], de5-1, sw_bar)
                    elif sw_dir == "bear" and c1["high"] >= z["bot"] and c1["close"] <= z["top"]:
                        ep = c1["close"]; stop_end = min(de5, len(stop_b5))
                        ph = max(stop_b5[m]["high"] for m in range(max(disp_idx+1,sw_bar), stop_end) if m < len(stop_b5)) if stop_end > max(disp_idx+1,sw_bar) else c1["high"]
                        sp = max(sw_lvl, ph, c1["high"]) + 1.0; risk = sp - ep
                        if risk <= 0: continue
                        rej = c1["high"] > z["ce"] and max(c1["open"],c1["close"]) <= z["top"]
                        first_touch = (ep, sp, c1["time_ns"]+NS_MIN, rej, c1["hour"], de5-1, sw_bar)
                    if first_touch:
                        entries.append({"ep": first_touch[0], "sp": first_touch[1], "ns": first_touch[2], "rej": first_touch[3],
                                        "hour": first_touch[4], "bar_idx": first_touch[5], "side": sw_dir,
                                        "zt": z["type"], "swept": sw_lvl, "sw_bar": first_touch[6],
                                        "zone_top": z["top"], "zone_bot": z["bot"]})
                        entries_from_this += 1
                        break
        used.add(disp_idx)
    return entries


def generate_all(dates):
    sym = "NQ"
    b5 = _G[sym][2]["5m"]; b1 = _G[sym][2]["1m"]
    b15 = _G[sym][2]["15m"]
    dr5 = _G[sym][3]; dr15 = _G[sym][7]
    all_dates = _G[sym][5]
    slip = INST[sym]["slip"]

    sigs = []
    for d in all_dates:
        if d not in dates or d not in dr5: continue
        ds5, de5 = dr5[d]
        liq = get_liquidity_levels(b5, dr5, d, all_dates)
        ents = gen_sweep_entries(b5, b1, ds5, de5, d, liq)

        seen_bars = set()
        for e in sorted(ents, key=lambda x: (x["ns"], -{"ifvg": 2, "disp_fvg": 1}.get(x["zt"], 0))):
            if e["bar_idx"] in seen_bars: continue
            seen_bars.add(e["bar_idx"])

            ep = e["ep"]; sp = e["sp"]; side = e["side"]
            if side == "bull": ep += slip
            else: ep -= slip
            risk = abs(ep - sp)
            if risk <= 0: continue

            # Scoring: sweep=2, struct=2, rest=1. Threshold: sc>=4 → 2.0R
            score = 1  # zone (any type)

            if e.get("rej"): score += 1

            cisd = cisd_5m(b5, e["bar_idx"], ds5)
            if cisd == side: score += 1

            sw_d, _, _ = detect_sweep_at(b5, e["bar_idx"], liq, lookback=8)
            if sw_d == side: score += 2  # sweep@entry = strong edge

            struct = structure_15m(b15, dr15, d, e["ns"])
            if struct == side: score += 2  # 15m structure = strong edge

            # 15m sweep
            sw15 = sweep_15m(b15, dr15, d, e["ns"], liq, side)
            if sw15: score += 1

            sigs.append({
                "sym": sym, "date": d, "side": side,
                "t": e["ns"], "e": ep, "sp": sp, "risk": risk,
                "score": score, "zt": e["zt"],
                "rej": e.get("rej", False), "hour": e.get("hour", 0),
                "cisd": cisd == side, "struct_15m": struct == side,
                "sw15": sw15,
            })
    return sigs

# ═══════════════════════════════════════════════════════════════
# TICK SIM
# ═══════════════════════════════════════════════════════════════
def _ex(tp,tt,si,ei,d,sp,tp1,ep,to1,slip,pv,comm,nct):
    cmn=comm*nct
    for i in range(si,ei):
        ts=tt[i];px=tp[i]
        if d==1:
            if px<=sp:return -1,ts,px,-(abs(ep-sp)*pv*nct+cmn)
            if px>=tp1:return 1,ts,tp1,(tp1-ep)*pv*nct-cmn
        else:
            if px>=sp:return -1,ts,px,-(abs(sp-ep)*pv*nct+cmn)
            if px<=tp1:return 1,ts,tp1,(ep-tp1)*pv*nct-cmn
        if ts>=to1:
            if d==1:p2=px-slip;return 0,ts,p2,(p2-ep)*pv*nct-cmn
            else:p2=px+slip;return 0,ts,p2,(ep-p2)*pv*nct-cmn
    if ei>si:
        lt=tt[ei-1];lp=tp[ei-1]
        if d==1:p2=lp-slip;return 0,lt,p2,(p2-ep)*pv*nct-cmn
        else:p2=lp+slip;return 0,lt,p2,(ep-p2)*pv*nct-cmn
    return 0,0,0.0,0.0

def sim_dynamic(sigs, ds_, rr_map, nct, to1=390, mdl=-2000, gmcl=5, mcl=3, mr=1000):
    """Sim with dynamic RR based on score."""
    cd=2*NS_MIN; inst=INST["NQ"]
    ss=sorted(sigs,key=lambda x:(x["t"],-x.get("score",0)))
    res=[];dp=0.0;cl_b=0;cl_s=0;gc=0;cd_=None;pe=0;lep=0
    for s in ss:
        dto=datetime.fromtimestamp(s["t"]/1e9);dd_=dto.date()
        if dd_ not in ds_: continue
        side=s["side"]
        if dd_!=cd_: cd_=dd_;dp=0.0;gc=0;cl_b=0;cl_s=0;lep=0;pe=0
        if s["t"]<pe or dp<=mdl or gc>=gmcl: continue
        cl_cur=cl_b if side=="bull" else cl_s
        if cl_cur>=mcl: continue
        if lep>0 and s["t"]<lep+cd: continue
        ar=s["risk"]*inst["pv"]*nct
        if ar>mr or ar<=0: continue
        # Dynamic RR — IFVG always 1.0 (Blake model), disp_fvg uses score-based
        sc = s.get("score", 2)
        if s.get("zt") == "ifvg":
            rr = 1.0
        else:
            rr = rr_map.get(sc, rr_map.get("default", 1.5))
        d=1 if side=="bull" else -1
        tp1=s["e"]+s["risk"]*rr*d
        tp_,tt_=_G["NQ"][:2]
        ton1=s["t"]+to1*NS_MIN
        si_=tick_start(tt_,s["t"]);ei=min(tick_start(tt_,ton1+NS_MIN),len(tp_))
        if si_>=ei: continue
        et,en,ep2,pnl=_ex(tp_[si_:ei],tt_[si_:ei],0,ei-si_,d,s["sp"],tp1,s["e"],ton1,inst["slip"],inst["pv"],COMMISSION,nct)
        if en<=0: continue
        lep=int(en);dp+=pnl;pe=int(en)
        if pnl<0:
            if side=="bull":cl_b+=1
            else:cl_s+=1
            gc+=1
        else:
            if side=="bull":cl_b=0
            else:cl_s=0
            gc=0
        res.append({"d":dd_,"sym":"NQ","side":side,"pnl":pnl,"nct":nct,
            "res":"WIN" if pnl>0 else("LOSS" if et==-1 else "T/O"),
            "t":s["t"],"score":sc,"zt":s.get("zt",""),"rr_used":rr})
    return res

def sim_fixed(sigs, ds_, rr, nct, to1=390, mdl=-2000, gmcl=5, mcl=3, mr=1000):
    """Sim with fixed RR for comparison."""
    rr_map = {"default": rr}
    for i in range(20): rr_map[i] = rr
    return sim_dynamic(sigs, ds_, rr_map, nct, to1, mdl, gmcl, mcl, mr)

def mdd(res):
    if not res: return 0.0
    dp=defaultdict(float)
    for r in res: dp[r["d"]]+=r["pnl"]
    eq=pk=mx=0.0
    for d in sorted(dp): eq+=dp[d];pk=max(pk,eq);mx=max(mx,pk-eq)
    return mx

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    t0 = time.time()
    print("="*70)
    print("V106 — HONEST ENGINE (3 fixes applied)")
    print("  FIX 1: First 1m touch = entry (no cherry-pick rejection)")
    print("  FIX 2: Session swings delayed 1 bar (confirmed, not look-ahead)")
    print("  FIX 3: Entry time = 1m bar CLOSE, not open (+60s shift)")
    print("  + Dynamic RR by score | + 15m sweep confluence")
    print("="*70)

    data = load_full("NQ")
    dr15 = _build_dr_htf(data[2]["15m"]); dr1h = _build_dr_htf(data[2]["1h"])
    _G["NQ"] = data[:7] + (dr15, dr1h)
    print(f"  NQ: {len(data[5])} days")

    dp = np.array([100.,101.,99.,102.,98.,103.,97.,104.], dtype=np.float64)
    dt_ = np.array([1,2,3,4,5,6,7,8], dtype=np.int64)
    _ex(dp, dt_, 0, 8, 1, 97., 102., 100., 5, 0.5, 20.0, 4.5, 3.0)

    ad = sorted(_G["NQ"][5])
    ad = [d for d in ad if d.weekday() < 5 and not((d.month==12 and d.day>=26) or (d.month==1 and d.day<=2))]
    train = set(d for d in ad if dt_date(2022,1,1) <= d <= dt_date(2023,12,31))
    test = set(d for d in ad if dt_date(2024,1,1) <= d <= dt_date(2026,2,28))
    all_set = train | test
    n_mo = len(test) / 21.0

    print(f"\nGenerating signals...")
    t1 = time.time()
    raw = generate_all(all_set)
    test_sigs = [s for s in raw if s["date"] in test]
    train_sigs = [s for s in raw if s["date"] in train]
    print(f"  Total: {len(raw)} | Test: {len(test_sigs)} ({len(test_sigs)/n_mo:.1f}/mo) | Train: {len(train_sigs)}")

    from collections import Counter
    sc = Counter(s["score"] for s in test_sigs)
    print(f"  Score distribution: {dict(sorted(sc.items()))}")

    # 15m sweep edge
    sw15_yes = [s for s in test_sigs if s.get("sw15")]
    sw15_no = [s for s in test_sigs if not s.get("sw15")]
    print(f"\n  15m sweep: {len(sw15_yes)} signals ({100*len(sw15_yes)/len(test_sigs):.1f}% hit rate)")
    print(f"  [{time.time()-t1:.0f}s]")

    # ── CONFLUENCE EDGE (quick check on 15m sweep) ──
    print(f"\n{'='*70}")
    print("15m SWEEP EDGE CHECK (3ct 1.5R)")
    print(f"{'='*70}")
    for label, filt in [("With 15m sweep", sw15_yes), ("Without 15m sweep", sw15_no), ("All", test_sigs)]:
        if len(filt) < 3: continue
        re = sim_fixed(filt, test, 1.5, 3)
        if not re: continue
        se = calc_stats(re); dd = mdd(re)
        print(f"  {label:<22} {se['trades']:>5}tr {se.get('wr',0):>5.1f}% ${se.get('pmo',0):>8,.0f}/mo DD:${dd:>6,.0f} {se['trades']/n_mo:>5.1f}tr/mo")

    # ── FIXED RR REFERENCE ──
    print(f"\n{'='*70}")
    print("FIXED RR REFERENCE (3ct)")
    print(f"{'='*70}")
    print(f"  {'Config':<25} {'Tr':>5} {'WR':>6} {'PnL/mo':>9} {'DD':>7} {'Tr/mo':>6}")
    print(f"  {'-'*60}")
    for rr in [1.3, 1.5, 1.7, 2.0]:
        re = sim_fixed(test_sigs, test, rr, 3)
        if not re: continue
        se = calc_stats(re); dd = mdd(re)
        print(f"  {'Fixed '+str(rr)+'R':<25} {se['trades']:>5} {se.get('wr',0):>5.1f}% ${se.get('pmo',0):>8,.0f} ${dd:>6,.0f} {se['trades']/n_mo:>5.1f}")

    # ── DYNAMIC RR CONFIGS ──
    print(f"\n{'='*70}")
    print("DYNAMIC RR BY SCORE (3ct)")
    print(f"{'='*70}")
    print(f"  {'Config':<40} {'Tr':>5} {'WR':>6} {'PnL/mo':>9} {'DD':>7} {'Tr/mo':>6}")
    print(f"  {'-'*70}")

    dynamic_configs = [
        # (name, {score: rr})
        ("Conservative: 1.3/1.5/1.7", {2: 1.3, 3: 1.3, 4: 1.5, 5: 1.5, 6: 1.7, 7: 1.7, 8: 1.7, 9: 1.7, "default": 1.3}),
        ("Moderate: 1.3/1.5/2.0", {2: 1.3, 3: 1.3, 4: 1.5, 5: 1.5, 6: 2.0, 7: 2.0, 8: 2.0, 9: 2.0, "default": 1.3}),
        ("Aggressive: 1.3/1.7/2.0", {2: 1.3, 3: 1.3, 4: 1.7, 5: 1.7, 6: 2.0, 7: 2.0, 8: 2.0, 9: 2.0, "default": 1.3}),
        ("Full spread: 1.3/1.5/1.7/2.0", {2: 1.3, 3: 1.3, 4: 1.5, 5: 1.7, 6: 2.0, 7: 2.0, 8: 2.0, 9: 2.0, "default": 1.3}),
        ("Step up: 1.3/1.5/1.7/2.0/2.5", {2: 1.3, 3: 1.3, 4: 1.5, 5: 1.7, 6: 2.0, 7: 2.5, 8: 2.5, 9: 2.5, "default": 1.3}),
        ("Safe low/push high: 1.5/1.5/2.0", {2: 1.5, 3: 1.5, 4: 1.5, 5: 1.5, 6: 2.0, 7: 2.0, 8: 2.0, 9: 2.0, "default": 1.5}),
        ("1.5 base / 2.0 high: 1.5/2.0", {2: 1.5, 3: 1.5, 4: 1.5, 5: 2.0, 6: 2.0, 7: 2.0, 8: 2.0, 9: 2.0, "default": 1.5}),
        ("Tiered: 1.3/1.5/2.0/2.5", {2: 1.3, 3: 1.5, 4: 1.5, 5: 2.0, 6: 2.0, 7: 2.5, 8: 2.5, 9: 2.5, "default": 1.3}),
    ]

    best_config = None; best_pmo = 0
    results = []
    for name, rr_map in dynamic_configs:
        re = sim_dynamic(test_sigs, test, rr_map, 3)
        if not re: continue
        se = calc_stats(re); dd = mdd(re)
        wr = se.get("wr", 0); pmo = se.get("pmo", 0)
        tpm = se["trades"] / n_mo
        # Train
        re_tr = sim_dynamic(train_sigs, train, rr_map, 3)
        wr_tr = calc_stats(re_tr).get("wr", 0) if re_tr else 0
        print(f"  {name:<40} {se['trades']:>5} {wr:>5.1f}% ${pmo:>8,.0f} ${dd:>6,.0f} {tpm:>5.1f}")
        results.append({"name": name, "rr_map": rr_map, "tr": se["trades"], "wr": wr,
                        "pmo": pmo, "dd": dd, "tpm": tpm, "wr_tr": wr_tr, "re": re})
        if pmo > best_pmo and dd < 2000:
            best_pmo = pmo; best_config = results[-1]

    # ── RR DISTRIBUTION for best config ──
    if best_config:
        print(f"\n{'='*70}")
        print(f"BEST: {best_config['name']}")
        print(f"  {best_config['tr']}tr {best_config['wr']:.1f}% ${best_config['pmo']:,.0f}/mo DD:${best_config['dd']:,.0f} {best_config['tpm']:.1f}tr/mo | Train:{best_config['wr_tr']:.1f}%")
        print(f"{'='*70}")

        # RR distribution
        rr_dist = Counter(r["rr_used"] for r in best_config["re"])
        print(f"\n  RR distribution:")
        for rr in sorted(rr_dist):
            trades = [r for r in best_config["re"] if r["rr_used"] == rr]
            wins = sum(1 for r in trades if r["pnl"] > 0)
            wr = 100 * wins / len(trades) if trades else 0
            avg_pnl = sum(r["pnl"] for r in trades) / len(trades)
            print(f"    {rr}R: {len(trades)} trades, {wr:.1f}% WR, avg ${avg_pnl:,.0f}/trade")

        # Monthly
        print(f"\n  {'Month':>8} {'Tr':>4} {'WR':>6} {'PnL':>9}")
        mo = defaultdict(lambda: {"t":0,"w":0,"p":0.0})
        for r in best_config["re"]:
            mk=f"{r['d'].year}-{r['d'].month:02d}";mo[mk]["t"]+=1;mo[mk]["p"]+=r["pnl"]
            if r["pnl"]>0:mo[mk]["w"]+=1
        neg=0
        for mk in sorted(mo):
            m=mo[mk];wr_m=100*m["w"]/m["t"] if m["t"]>0 else 0
            x=" X" if m["p"]<0 else ""
            if m["p"]<0:neg+=1
            print(f"  {mk:>8} {m['t']:>4}tr {wr_m:5.1f}% ${m['p']:>8,.0f}{x}")
        print(f"  Neg months: {neg}/{len(mo)}")

    # ── Compare best dynamic vs best fixed ──
    print(f"\n{'='*70}")
    print("DYNAMIC vs FIXED COMPARISON")
    print(f"{'='*70}")
    for rr in [1.5, 1.7]:
        re = sim_fixed(test_sigs, test, rr, 3)
        se = calc_stats(re); dd = mdd(re)
        print(f"  Fixed {rr}R:    {se['trades']:>5}tr {se.get('wr',0):>5.1f}% ${se.get('pmo',0):>8,.0f}/mo DD:${dd:>6,.0f}")
    if best_config:
        print(f"  Dynamic best: {best_config['tr']:>5}tr {best_config['wr']:>5.1f}% ${best_config['pmo']:>8,.0f}/mo DD:${best_config['dd']:>6,.0f}")

    print(f"\n  Total: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
