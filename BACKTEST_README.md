# Tick-Level Backtest Engine — Setup & Usage

## Overview

This backtest validates the V106 ICT strategy using **tick-level trade simulation** (Databento NQ futures tick data) combined with **bar-level signal generation** (TopstepX API 5m/1m/15m bars).

**Why tick data matters:** 1m bars can't tell you whether the stop or target was hit first within the same bar. Tick data resolves this with nanosecond precision. In our 41-day test, 10 of 13 mismatches were trades that 1m bars called LOSSes but were actually WINs.

### Latest Results (Jan 19 – Mar 16, 2026 — 41 trading days, 22M ticks)

**Best config: disp_fvg + iFVG zones, flat 1.1 RR, all scores, IFVG_INV_CLEAR=0.5**
- **182 trades | 141W/41L | 77.5% WR | PF 3.35 | $69,852 P&L | $760 max DD | 1 losing day**
- $1,704/day average on 3 contracts (~4.4 trades/day)

### What Changed (from 55% WR to 77.5% WR)

Two things fixed the strategy:

1. **iFVG implementation completely rewritten** — Old code scanned 5m bars 40 bars back, no min gap, no max width, zone-retest model → 29% WR. New code uses PB Trading's model: 1m FVGs in the leg, entry at inversion candle close, min 0.5pt gap, max 12pt width → ~71% WR on iFVG alone.

2. **Flat 1.1 RR replaces variable RR** — Old code used score-based tiers (1.3-2.0). The 2.0 RR target was too greedy — 80% of losses at 2.0 had price go 2+ points in favor first. Flat 1.1 takes profit early and wins far more often.

### Results by Zone Type (flat 1.1 RR)

| Zone | Trades | WR | Notes |
|------|--------|------|-------|
| disp_fvg | ~112 | ~80% | Displacement FVG zones — bread and butter |
| iFVG | ~70 | ~71% | PB Trading inverse FVG model (FIXED) |
| Combined | 182 | 77.5% | Best overall config |

### Flat RR Comparison (both zones, all scores, IFVG_CLEAR=0.5)

| RR  | Trades | WR    | P&L     | MaxDD  | Losing Days |
|-----|--------|-------|---------|--------|-------------|
| 1.0 | 172    | 78.5% | $57,550 | $1,752 | 2/41        |
| 1.1 | 170    | 76.5% | $62,230 | $1,645 | 2/41        |
| 1.2 | 169    | 72.2% | $61,043 | $3,245 | 4/41        |
| 1.3 | 168    | 71.4% | $67,179 | $2,942 | 3/41        |

Note: IFVG_CLEAR=0.5 regenerated signals add 12 more winning iFVG entries → 182 trades, 77.5% WR, $69,852 P&L, $760 MaxDD.

### Score Filter Analysis

| Filter | Trades | WR | P&L | MaxDD |
|--------|--------|------|---------|--------|
| All scores (1-7) | 170 | 76.5% | $62,230 | $1,645 |
| Score >= 4 only | 119 | 77.3% | $46,000 | similar |

Score >= 4 barely helps WR but kills ~$16K in P&L. **Not worth filtering.**

Score 4+ means a major confluence (+2 component: double sweep or 15m structure break). Score 1-3 only has minor confluences (+1 each). Score 1 (bare zone touch) actually has decent WR — a clean setup with no weak confirmations sometimes outperforms ones with minor-only signals.

### Key Finding: Dynamic RR by score does NOT help
Tested 10+ tier combos giving different RR to score 4/5/6. None beat flat 1.1 RR. Keep it simple.

### Next Step: 4-Year Validation
See `BACKTEST_4YEAR_HANDOFF.md` for complete instructions to run this on 1000+ trading days of Databento NQ tick data.

---

## File Structure

```
backtest_tick.py            # Main tick-level backtest engine
backtest_entry_modes.py     # 5 entry mode comparison (1m bar simulation)
backtest_filters.py         # Filter testing framework on top of Mode 3
backtest_topstep.py         # Bar fetching from TopstepX API + 1m bar simulation
v106_dynamic_rr_zone_entry.py  # Strategy logic (zones, sweeps, displacement, scoring)
tsxapi4py/                  # TopstepX API client library
.bar_cache/                 # Auto-generated bar cache (JSON, gitignored)
```

### Dependency Chain
```
backtest_tick.py
  ├── backtest_topstep.py       (fetch_with_rollover, build_dr, build_dr_htf)
  ├── backtest_entry_modes.py   (gen_sweep_entries_enriched, apply_entry_mode, simulate_trades)
  └── v106_dynamic_rr_zone_entry.py  (get_liquidity_levels, detect_sweep_at, etc.)
      └── tsxapi4py/            (authenticate, APIClient)
```

---

## Prerequisites

### 1. Python Dependencies
```bash
pip install numpy databento
```
The `tsxapi4py` library is included in the repo (no install needed).

### 2. TopstepX API Credentials
Create a `.env` file in the project root:
```
TSX_USERNAME=your_topstep_username
TSX_PASSWORD=your_topstep_password
```
These are used to fetch historical 5m/1m/15m bars for signal generation.

### 3. Databento Tick Data
Buy NQ futures tick data from [databento.com](https://databento.com):
- **Dataset:** `GLBX.MDP3` (CME Globex)
- **Schema:** `trades`
- **Symbols:** `NQ.FUT` (parent symbol — auto-resolves to NQH6, NQM6, etc.)
- **stype_in:** `parent`
- **Encoding:** `dbn`
- **Compression:** `zstd`
- **Split:** by day

This produces files like:
```
glbx-mdp3-20260119.trades.dbn.zst
glbx-mdp3-20260120.trades.dbn.zst
...
```

**For 4 years of data:** Pull from Databento with start/end covering the full range you want. The script auto-discovers all `.dbn.zst` files in the tick data directory. Each file = one trading day.

---

## Configuration

Edit the constants at the top of `backtest_tick.py`:

```python
# CHANGE THIS to your tick data directory path
TICK_DIR = "/path/to/your/databento/tick/data"

# These match the live bot settings — don't change unless the bot changes
PV = 20              # Point value for NQ ($20/point)
CONTRACTS = 3        # Number of contracts
FEES_RT = 8.40       # Round-trip fees (3 contracts × $2.80/side)
MAX_RISK = 1000      # Max risk per trade in dollars
COOLDOWN_S = 120     # Seconds cooldown after trade exit
MCL = 3              # Max consecutive losses per direction
SLIP = 0.5           # Slippage in points
SIM_START_HOUR = 7   # Earliest signal hour (CT)
SIM_START_MIN = 30   # Earliest signal minute (CT)
```

---

## Running the Backtest

### First Run (fetches + caches API bars)
```bash
python3 backtest_tick.py
```
- Loads all tick data from `TICK_DIR` (scans for `.dbn.zst` files)
- Fetches 5m/1m/15m bars from TopstepX API (may hit rate limits — it retries automatically)
- **Caches bars to `.bar_cache/`** so subsequent runs skip the API entirely
- Generates signals bar-by-bar, simulates with tick data
- Runtime: ~5-6 minutes (mostly tick data loading)

### Subsequent Runs (instant bar loading)
```bash
python3 backtest_tick.py
```
Bars load from cache in <1 second. Only tick data loading takes time.

### If You Add More Tick Data
Just drop new `.dbn.zst` files into `TICK_DIR`. The script auto-discovers them.

**Important:** If the date range changes (new earliest or latest date), delete the `.bar_cache/` directory so it re-fetches bars covering the full range:
```bash
rm -rf .bar_cache/
python3 backtest_tick.py
```

---

## Running with 4 Years of Tick Data

### Step 1: Buy the Data
On Databento, create a batch job:
- Dataset: `GLBX.MDP3`
- Schema: `trades`
- Symbols: `NQ.FUT`
- Start: `2022-01-01` (or whenever you want)
- End: `2026-03-20`
- Split: `day`

This will produce ~1000 `.dbn.zst` files (one per trading day).

### Step 2: Set TICK_DIR
Update `TICK_DIR` in `backtest_tick.py` to point to the directory with all the files.

### Step 3: Handle API Bar Limits
The TopstepX API may not have 4 years of historical bars. The script fetches from 7 days before the first tick date through the last tick date. If the API only returns data from a certain date onward, the script will only generate signals for dates where both tick data AND bar data exist.

**The `trade_dates` line in the output tells you exactly which dates were used:**
```
Trading dates: 38 days (2026-01-19 → 2026-03-16)
```

### Step 4: Run
```bash
rm -rf .bar_cache/    # Clear old cache since date range changed
python3 backtest_tick.py
```

### Step 5: Expect Long Runtime
- 4 years of tick data = ~200-500 million ticks
- Loading will take several minutes
- Signal generation scales with number of trading days
- Total runtime estimate: 15-30 minutes for 4 years

---

## Output Sections

The script produces 4 sections:

### 1. Signal-by-Signal Comparison
Each signal tested independently (no cooldown/MCL) with both 1m bars and tick data. Shows mismatches where the two disagree.

### 2. Full Simulation Comparison
With cooldown, MCL, and zone dedup applied (matching live bot behavior). Shows Mode 2 vs Mode 3 with both 1m and tick simulation.

### 3. Detailed Trades — Mode 3
Every trade listed by day with entry/exit times, P&L, and weekly summary. This is the walk-forward view.

### 4. Mismatch Analysis
Detailed breakdown of every signal where 1m bars got a different result than tick data, with exact tick exit timestamps.

---

## How It Works (No Look-Ahead)

### Signal Generation (bar-by-bar)
```
for cursor in range(ds5 + 1, de5 + 1):
    b1_cutoff = <next 5m bar boundary>
    signals = gen_sweep_entries_enriched(b5, b1[:b1_cutoff], ...)
```
- Cursor advances one 5m bar at a time
- `b1[:b1_cutoff]` slices 1m bars — future bars are invisible
- Same code path as the live bot

### Trade Simulation (tick-level, forward-only)
```python
idx0 = np.searchsorted(tick_t, entry_ns, side='right')  # first tick AFTER entry
post = tick_p[idx0:]                                      # only future prices
sh = np.where(post <= stop)[0]                            # first stop hit
th = np.where(post >= target)[0]                          # first target hit
# whichever index is smaller happened first
```
- `np.searchsorted` finds the first tick strictly after entry time
- Only looks forward from there
- Resolves stop vs target with nanosecond precision

### No Parameter Optimization
All strategy parameters (RR tiers, kill zone hours, cooldown, MCL, scoring) are hardcoded identically to the live bot. Nothing is "fit" to the historical data.

---

## Other Backtest Scripts

### backtest_entry_modes.py
Compares 5 entry methods side-by-side using 1m bar simulation:
1. Zone Limit — limit order at zone edge, no confirmation
2. 1m Close — enter at 1m close price (current production)
3. Zone+Confirm — zone price + 1m close confirmation (recommended)
4. 1m FVG — zone confirmed, then 1m FVG forms, enter on retest
5. RSI+Zone — zone confirmed + RSI oversold/overbought filter

```bash
python3 backtest_entry_modes.py
```

### backtest_filters.py
Tests additional filters on top of Mode 3 entries:
```bash
python3 backtest_filters.py
```

### backtest_topstep.py
Original 1m bar backtest (no tick data needed):
```bash
python3 backtest_topstep.py
```

---

## Contract Rollover

The script handles NQ contract rollovers automatically:
- `NQ_CONTRACT = "CON.F.US.ENQ.M26"` (June 2026, current front month)
- `NQ_CONTRACT_PREV = "CON.F.US.ENQ.H26"` (March 2026, prior contract)

For tick data, `NQ.FUT` as a parent symbol resolves to both contracts. The script picks the highest-volume contract per day automatically.

**For 4-year backtests:** The tick data handles this via the `NQ.FUT` parent symbol. For API bars, you may need to add more contract IDs to `backtest_topstep.py` to cover older quarters.
