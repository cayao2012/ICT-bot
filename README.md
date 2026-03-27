# PTNUT Bot — V106 NQ ICT Strategy on TopstepX

## Current Config (Eval Mode)
```
Instrument:  NQ (M26)
Contracts:   4ct
RR:          1.1 flat (both disp_fvg + ifvg)
Kill Zone:   7:30-14:30 CT
GMCL:        2 (stop after 2 consecutive losses)
MCL:         3 per side
DLL:         -$2,000
Max Risk:    $1,000/trade
Cooldown:    120s
IFVG delay:  No IFVGs before 9:00 CT
Paper Mode:  ON (flip to False for live)
```

## 4-Year Backtest Results (2022-2026, 1,067 days)
```
Trades:     6,489 | 73.4% WR | $2.08M P&L | PF 2.72
MaxDD:      $5,762 | $/day: $1,976
disp_fvg:   3,872 trades | 74.8% WR
ifvg:       2,617 trades | 71.4% WR
Zero negative months (50+ months)
```

## 52-Day Results (Jan-Mar 2026)
```
Trades:     241 | 76.3% WR | $87K P&L
MaxDD:      $1,157 | 3 losing days out of 52
```

## Monte Carlo: 100K Account, 4ct, $6.2K target, 5 days
```
Pass: 84.9% | Blow: 5.3%
```

## Strategy
Sweep liquidity → displacement candle → FVG zone → 1m touch/inversion = entry

**Two zone types:**
- **disp_fvg** — gap created by displacement candle on 5m. Entry at 1m close touch.
- **IFVG** — highest TF (1m-5m) singular FVG in sweep→displacement leg. Entry at inversion close (body through FVG). Multi-TF: checks 1m, 2m, 3m, 4m, 5m for highest TF gap.

**Key improvements (this session):**
- Multi-TF IFVG (PB Blake model) — checks all timeframes, uses highest singular
- GMCL 2 — Blake's rule: 2 consecutive losses = done for the day
- IFVG delay — skip IFVGs before 9:00 CT (61% vs 77% WR)
- Full mitigation check — scans all bars, not just 10
- pysignalr streams — replaced buggy signalrcore with proper Microsoft SignalR protocol
- Trade-based bar building — bars built from GatewayTrade (matches broker exactly)
- Bar-by-bar backtest verified — zero look-ahead bias confirmed by full audit

## Architecture
```
pysignalr WebSocket (GatewayTrade) → on_tick → builds 1m bars from trades
                                                    │
                                          1m bar close → INSTANT scan
                                                    │
                                    gen_sweep_entries_enriched + apply_entry_mode
                                    (EXACT same functions as backtest)
                                                    │
                                          Signal → entry at 1m close price
```

## Files
```
ptnut_bot.py              — NQ live trading bot (4ct, pysignalr)
backtest_run.py           — Backtest runner (tick-level, bar-by-bar)
backtest_entry_modes.py   — Signal generation + multi-TF IFVG
v106_dynamic_rr.py        — Strategy engine (shared by bot + backtest)
v106_dynamic_rr_zone_entry.py — Same (synced copy)
market_stream.py          — pysignalr market data stream
user_stream.py            — pysignalr user hub stream
build_nq_tick_arrays.py   — Build NQ ticks from Databento dbn.zst
build_new_bars.py         — Build 1m/5m/15m bars from ticks
monte_carlo_topstep.py    — TopstepX pass/blow simulation
BACKTEST_RESULTS.md       — Full backtest findings
data_new/                 — 4yr NQ tick + bar data (Git LFS)
```

## How to Run Backtest
```bash
# Default (4yr, flat 1.1 RR, GMCL 2, IFVG delay)
BACKTEST_GMCL=2 BACKTEST_IFVG_DELAY=1 python3 -u backtest_run.py

# Custom date range
BACKTEST_START_DATE=2026-01-01 python3 -u backtest_run.py

# Custom slippage
BACKTEST_SLIP=1.5 python3 -u backtest_run.py

# Different data
BACKTEST_DATA_DIR=data_41day BACKTEST_BARS_FILE=data_41day/bars_cache_built.pkl python3 -u backtest_run.py

# ES (different params)
BACKTEST_PV=50 BACKTEST_CONTRACTS=2 BACKTEST_FEES=9.00 BACKTEST_SLIP=0.25 python3 -u backtest_run.py
```

## Going Live
1. Verify signals match on paper (bot running now, compare to backtest)
2. Set `PAPER_MODE = False` in ptnut_bot.py
3. Set `KZ_START = (7, 30)` for NY only
4. Restart bot before 7:30 CT

## Verified
- Bar-by-bar signal generation — zero look-ahead (full audit)
- Bot signals match backtest exactly (5/5 days verified)
- Trade-built bars match broker REST bars (OHLC identical)
- pysignalr stable (no disconnects, auto-reconnect)

## Hard Rules
- No breakeven stops (kills WR 80% → 57%)
- No trailing stops
- No position sizing changes
- Confluences don't gate entries (they score, not filter)
- 1m-beyond scanning active (entries on current 5m bar)
- RR ≥ 1.1 always
