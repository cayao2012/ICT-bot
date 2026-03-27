# 4-Year Backtest Handoff — Everything You Need

## Strategy Steps (V106 ICT)

1. **Map liquidity levels** — PDH/PDL, Asia/London/Pre-market ranges, swing highs/lows (5m + 15m)
2. **Detect sweep** — 5m candle pierces through a liquidity level but closes back (fake breakout, stops got hunted)
3. **Confirm displacement** — Within next 6 bars on 5m, a strong candle (35%+ body) fires in the opposite direction confirming reversal
4. **Find FVG zone** — Either:
   - disp_fvg: gap left by the displacement candle on 5m
   - iFVG: 1m FVGs in the leg that get inverted by displacement
5. **1m entry trigger** —
   - disp_fvg: first 1m candle that touches the zone, enter at its close
   - iFVG: 1m candle closes beyond the FVG edge (0.5pt+ past it), enter at its close
6. **Stop** — Below/above the sweep level and leg extreme, minus 1pt buffer
7. **Target** — Entry + (stop distance x 1.1 RR)
8. **Risk check** — If stop distance x $20/pt x 3 contracts > $1,000, skip the trade

No score filter. No dynamic RR. All timeframes used: 5m (sweep/displacement/zones), 1m (entry/iFVG), 15m (scoring only — tracked but not filtered).

---

## What To Validate

Run the V106 ICT strategy on 4 years of NQ tick data. The 41-day backtest (Jan 19 - Mar 16, 2026) produced these results. We need to know if they hold over 1000+ trading days.

### Best Config (41-day results)
- **182 trades | 141W/41L | 77.5% WR | $69,852 P&L | PF 3.35 | $760 MaxDD | 1 losing day**
- Flat 1.1 RR on all trades, both zone types (disp_fvg + ifvg), all scores, 3 contracts

### What Changed From Original Code
The original strategy had 55% WR. Two things changed:

1. **iFVG implementation rewritten** — old code was broken (29% WR), new code uses PB Trading's model (see below). Now 77.5% WR combined.
2. **Flat 1.1 RR instead of variable RR** — old code used score-based RR tiers (1.3-2.0). 2.0 RR was too greedy. Flat 1.1 takes profit early, wins more often.

---

## Critical Settings

```python
# In backtest_entry_modes.py
SLIP = 0.5
PV = 20
CONTRACTS = 3
MAX_RISK = 1000
COOLDOWN_S = 120
MCL = 3
FEES_RT = 8.40

# iFVG thresholds
IFVG_MIN_GAP   = 1.0    # minimum FVG gap in points
IFVG_MAX_WIDTH = 12.0   # max zone width in points
IFVG_LOOKBACK  = 8      # max bars to scan back
IFVG_INV_BODY  = 0.35   # inverting candle min body/range ratio
IFVG_INV_CLEAR = 0.5    # close must clear FVG edge by this many points ← WAS 3.0, NOW 0.5
```

### RR Setting
**Flat 1.1 RR on ALL trades.** Override whatever the score-based RR logic produces. In the backtest, after `apply_entry_mode()` returns a signal, set `sig["rr"] = 1.1`.

### Zone Types To Include
Both `disp_fvg` and `ifvg`. Filter: `sig["zone"] in ("disp_fvg", "ifvg")`.

### Score Filter
**None.** Take all scores (1-7). The score system exists but is NOT used as a filter in the best config.

---

## The iFVG Fix — What Changed

### Old iFVG (BROKEN — 29% WR):
- Found FVGs on **5m bars**, scanning **40 bars back** (3+ hours stale)
- No minimum gap size (0.25pt gaps = noise on NQ)
- No maximum zone width (zones up to 83pt wide)
- Treated iFVG as a **zone for later retest** — waited for price to come back

### New iFVG (PB Trading model — based on his YouTube course):
- Finds FVGs on **1m bars** within the **specific leg** (sweep → displacement)
- Minimum 0.5pt gap on 1m
- Maximum 12pt zone width
- Entry at the **close of the candle that INVERTS the FVG** — no retest
- Inverting candle must have real body (35%+ body/range ratio)
- Close must clear FVG edge by at least 0.5pt
- "Singular" rule: ALL FVGs in the leg must be inverted (uses the extreme edge)
- Stop at swing low/high of the 5m leg

### The Code (in gen_sweep_entries_enriched, backtest_entry_modes.py, ~line 195-292)

The iFVG detection runs BEFORE disp_fvg zone scanning. It:

1. Finds the 1m bar range for the leg (15 bars before sweep through displacement)
2. Scans backward through leg for contrary 1m FVGs (bearish FVGs for bull setups, bullish for bear)
3. Filters: min 0.5pt gap, max 12pt width, not already body-closed before displacement
4. Gets the extreme edge (max top for bull, min bot for bear) — PB's "singular" requirement
5. Scans forward from displacement for an inverting candle (up to 20 bars):
   - Must be in the right direction (bull close > open for bull entry)
   - Must clear the extreme edge by IFVG_INV_CLEAR (0.5pt)
   - Must have body/range >= IFVG_INV_BODY (0.35)
6. Entry price = inverting candle's close
7. Stop = min/max of sweep level and leg extreme ± 1pt

---

## Look-Forward Bug Warning

**YOU MUST SLICE b5 AND b1 CORRECTLY.** The signal generation loop must use:
```python
b5[:cursor + 1]   # only bars up to current cursor
b1[:b1_cutoff]     # only 1m bars up to next 5m boundary
```

Using `len(b5)` on the FULL unsliced array is a look-forward bug. Our code passes `b5[:cursor + 1]` on line 370 of backtest_tick.py. Verify yours does too.

Also verify `stop_b5` defaults to the sliced `b5`, not the full array.

---

## Scoring System (for reference — NOT used as filter)

```python
score = 1  # base
+1  rejection wick on sweep candle
+1  CISD (change in state of delivery) on 5m
+2  double sweep (both session high AND low swept)
+2  15m structure break confirms side
+1  15m sweep (15m liquidity taken)
```

Score 4+ = has a major confluence (double sweep or 15m structure). Score 1-3 = minor only.

In 41-day data, score >= 4 at flat 1.1 RR: 119 trades, 77.3% WR, $46K. All scores at 1.1 RR: 170 trades, 76.5% WR, $62K. Removing score 1-3 barely helps WR but kills volume.

---

## What To Report

Run these configs and compare:

### Config 1: Best config
- Both zone types (disp_fvg + ifvg)
- Flat 1.1 RR
- All scores
- IFVG_INV_CLEAR = 0.5

### Config 2: disp_fvg only (baseline)
- disp_fvg only (drop ifvg)
- Flat 1.1 RR
- All scores

### Config 3: Score >= 4 filter
- Both zone types
- Flat 1.1 RR
- Score >= 4 only
- IFVG_INV_CLEAR = 0.5

### For each config report:
- Total trades, W/L, WR%
- Total P&L, $/day
- Profit factor
- Max drawdown
- Losing days / total days
- Yearly breakdown (does WR hold across all 4 years or just recent?)
- Monthly breakdown (any months consistently bad?)
- WR by zone type (disp_fvg vs ifvg separately)

### Key questions to answer:
1. Does 77% WR hold over 4 years or was 41 days lucky?
2. Does iFVG add volume without hurting WR long-term?
3. Is flat 1.1 RR consistently better than variable RR across all market regimes?
4. Are there extended losing periods (2+ weeks of drawdown)?
5. Does the strategy work in ranging/choppy markets or only trending?

---

## Files You Need

```
backtest_tick.py            — Main tick-level backtest engine
backtest_entry_modes.py     — Signal generation + iFVG implementation (MODIFIED)
backtest_topstep.py         — Bar fetching from TopstepX API
v106_dynamic_rr_zone_entry.py — Strategy logic (zones, sweeps, displacement, scoring)
tsxapi4py/                  — TopstepX API client library
```

### Tick Data
- Databento GLBX.MDP3, schema: trades, symbols: NQ.FUT, stype_in: parent
- Split by day, .dbn.zst files
- Set TICK_DIR in backtest_tick.py to point to your data directory

### API Bars
- Bars are cached in .bar_cache/ after first fetch
- If date range changes, delete .bar_cache/ and re-fetch
- TopstepX API may not have 4 years of historical bars — the script only generates signals for dates where both tick AND bar data exist

---

## 41-Day Results Summary (for comparison)

### Original code (before fix):
- 156 trades | 55.1% WR | $50,143 P&L | PF 2.20 | $6,189 MaxDD | 10 losing days
- iFVG: 41 trades, 29.3% WR, -$5,672

### After iFVG fix + flat 1.1 RR + IFVG_CLEAR=0.5:
- 182 trades | 77.5% WR | $69,852 P&L | PF 3.35 | $760 MaxDD | 1 losing day
- iFVG: 70 signals, ~71% WR, contributing ~$15K
- disp_fvg: ~112 signals, ~80% WR at 1.1 RR

### Flat RR comparison (both zones, all scores):
| RR  | Trades | WR    | P&L     | MaxDD  | Losing Days |
|-----|--------|-------|---------|--------|-------------|
| 1.0 | 172    | 78.5% | $57,550 | $1,752 | 2/41        |
| 1.1 | 170    | 76.5% | $62,230 | $1,645 | 2/41        |
| 1.2 | 169    | 72.2% | $61,043 | $3,245 | 4/41        |
| 1.3 | 168    | 71.4% | $67,179 | $2,942 | 3/41        |

Note: The IFVG_CLEAR=0.5 numbers (182 trades, 77.5%, $69,852) are with regenerated signals — more iFVG entries than the flat RR table above which used IFVG_CLEAR=3.0.
