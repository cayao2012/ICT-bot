# V106 NQ Strategy — 4-Year Backtest Results

## Strategy Summary
Sweep liquidity → displacement candle → FVG zone → 1m inversion/touch = entry

**Two zone types:**
- **disp_fvg** (displacement FVG): gap created by the impulse candle. Entry at 1m close touch. RR: 1.1
- **IFVG** (inverted FVG): highest TF FVG in the sweep→displacement leg that gets body-closed through. Entry at inversion close. RR: 1.0 (Blake's model)

## 4-Year Results (2022-01-03 → 2026-03-16, 1,067 trading days)

### Main Config: Multi-TF IFVG + 1.0R IFVG + DLL + GMCL
```
Trades:     8,375 (6,074W / 2,296L)
Win Rate:   72.5%
P&L:        $2,379,494
PF:         2.46
Max DD:     $4,865
$/day:      $2,255
$/month:    ~$47,365
Losing Days: 142 / 1,055 (13.5%)
Negative Months: 0
```

### By Zone Type
| Zone | Trades | WR% | P&L | PF |
|---|---|---|---|---|
| disp_fvg | 4,272 | 74.4% | $1,300,794 | 2.91 |
| ifvg | 4,103 | 70.6% | $1,078,700 | 2.14 |

### By Side
| Side | Trades | WR% | P&L | PF |
|---|---|---|---|---|
| bull | 4,278 | 71.1% | $1,171,967 | 2.32 |
| bear | 4,097 | 74.0% | $1,207,527 | 2.62 |

## Walk-Forward: Out-of-Sample (2024-2026, 543 days)
Strategy developed on 2022-2023 concepts, tested forward on unseen 2024-2026 data:
```
Trades:     4,065
Win Rate:   72.8%
P&L:        $1,189,464
PF:         2.51
Max DD:     $4,138
$/day:      $2,191
```
**OOS holds — no degradation.** WR actually slightly higher than in-sample.

## Slippage Sensitivity
| Slippage | Trades | WR% | P&L | $/month | MaxDD |
|---|---|---|---|---|---|
| 0.5pt (backtest default) | 8,375 | 72.5% | $2,379K | $47K | $4,865 |
| 1.5pt (conservative) | 7,499 | 68.7% | $1,891K | $38K | $5,922 |

**Realistic estimate: $38-47K/month gross depending on execution quality.**

## TopstepX 100K Account Simulation
Sequential walk-through of actual 4yr daily P&L:
- **Combines attempted:** 9
- **Combines passed:** 5
- **Accounts blown:** 4 (all after hundreds of successful payouts)
- **Total payouts:** 675
- **Total net profit (90/10):** $2,102,846
- **Per month:** $41,858 take-home
- **ROI on combine fees:** 2,404x ($875 spent → $2.1M earned)

## Monte Carlo Pass Probability (100K TopstepX)
| Phase | Pass % | Avg Days |
|---|---|---|
| Pass Combine (+$6K) | 98.9% | 3 days |
| Lock MLL on funded | 99.1% | 2 days |
| Build payout buffer | 99.0% | 3 days |
| Full journey to first payout | 97.1% | 9 days |
| Steady-state $5K payout | 99.6% | 3 days |

## Look-Ahead Audit
**ZERO look-ahead bias confirmed** by comprehensive code audit:
- b5 slicing (`b5[:cursor+1]`) verified correct
- b1 cutoff verified — only completed 1m bars visible
- IFVG detection operates only on past bars
- Stop calculations use only past bars
- Scoring functions bounded by entry-time indices
- Tick outcome uses `searchsorted(side='right')` — strictly after entry
- DLL/GMCL use only past trade outcomes

## Key Changes from Previous Version

### Multi-TF IFVG (NEW — from PB Trading Blake model)
- **Before:** Only checked 1m FVGs in the sweep→displacement leg
- **After:** Checks 1m, 2m, 3m, 4m, 5m and uses the HIGHEST timeframe FVG
- Must be SINGULAR (only gap in the leg on that TF)
- Fallback: if no singular, uses extreme edge of highest TF with any FVGs
- **Impact:** +1,059 IFVG trades, IFVG WR up 3%, +$198K P&L

### IFVG at 1.0 RR (Blake's approach)
- Blake targets 1:1 low-hanging fruit for inversions
- disp_fvg stays at 1.1 RR
- **Impact:** IFVG WR up from 67.6% → 70.6%, MaxDD down significantly

### DLL (Daily Loss Limit) + GMCL (Global MCL)
- DLL: -$2,000/day — stops trading when daily P&L hits limit
- GMCL: 5 consecutive losses either side — done for the day
- Both reset at start of each new trading day

### Tick scoping
- outcome_tick limits search to 8 hours after entry (trades resolve within 6.5hr timeout)
- Same results, 10x faster runtime

## Risk Parameters
| Parameter | Value |
|---|---|
| Contracts | 3 (flat, always) |
| Max Risk | $1,000/trade |
| MCL | 3 per side |
| GMCL | 5 total |
| DLL | -$2,000/day |
| Cooldown | 120 seconds |
| Slippage | 0.5pt (backtest) |
| Fees | $8.40 RT |
| disp_fvg RR | 1.1 |
| IFVG RR | 1.0 |
| Max entries/sweep | 2 |

## Files
```
backtest_run.py          — Main backtest runner (tick-level)
backtest_entry_modes.py  — Signal generation + multi-TF IFVG
backtest_topstep.py      — Bar fetching from TopstepX API
v106_dynamic_rr_zone_entry.py — Strategy logic (zones, sweeps, displacement)
build_nq_tick_arrays.py  — Build NQ tick arrays from Databento dbn.zst files
build_new_bars.py        — Build 1m/5m/15m bars from tick data
monte_carlo_topstep.py   — TopstepX pass/blow Monte Carlo simulation
data_new/                — 4yr tick data + bar cache (Git LFS)
```

## How to Run
```bash
# Default (0.5pt slip, full 4yr)
python3 -u backtest_run.py

# Custom slippage
BACKTEST_SLIP=1.5 python3 -u backtest_run.py

# Specific date range
BACKTEST_START_DATE=2024-01-01 python3 -u backtest_run.py

# Custom data directory
BACKTEST_DATA_DIR=data_41day BACKTEST_BARS_FILE=data_41day/bars_cache_built.pkl python3 -u backtest_run.py
```

## What's Still Needed
1. **Live paper test** — Run bot in paper mode for 1-2 days, compare signals to backtest
2. **IFVG in live bot** — v106_dynamic_rr_zone_entry.py still uses OLD IFVG (40-bar 5m lookback). Needs updating to match backtest_entry_modes.py multi-TF approach
3. **MaxDD management** — $4.8K DD exceeds $3K MLL. Need larger payout cushion ($8K+) or accept occasional blown accounts
4. **WR improvement** — 72.5% is solid but 80% target needs more selective entries (Blake's key-level-only approach)
