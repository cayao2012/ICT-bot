# PTNUT Trading Bot — Complete Implementation Reference
## For Other Claude Sessions: Read This Before Touching ANYTHING

> **Last Updated:** 2026-03-19
> **Status:** LIVE trading on TopstepX (NQ futures)
> **Bot File:** `ptnut_bot.py` (~1950 lines)
> **Strategy File:** `v106_dynamic_rr.py` (~450 lines)
> **API Library:** `tsxapi4py/` (TopstepX/ProjectX Gateway wrapper)

---

## CRITICAL RULES — NEVER BREAK THESE

| Rule | Why |
|------|-----|
| **NEVER re-enable virtual 5m bar (BUG 16)** | Caused phantom signal → $1000 loss on Mar 17. Bot must use COMPLETED bars only. |
| **NEVER change stale signal limit from 120s** | Tuned to NQ volatility. Shorter = missed entries. Longer = bad fills. |
| **NEVER change risk filter ($1000 max)** | 3ct × $20/pt = 16.7pt max stop. Account protection. |
| **NEVER change strategy parameters** | gen_sweep_entries, scoring, RR map — these match the backtest exactly. |
| **NEVER change contracts (3ct)** | Fixed. Anti-martingale was removed — it hurts more than helps at 82% WR. |
| **NEVER clear 5m/15m/dr caches on new_day()** | BUG 19: They contain multi-day lookback data needed by liquidity levels. |
| **NEVER use `pending_out` parameter** | Old system replaced by 1m-beyond-5m scanning in gen_sweep_entries. |
| **OrderStatus FILLED = 2, NOT 4** | tsxapipy constants are WRONG. Bot overrides with correct ProjectX API values. |

---

## Architecture Overview

```
ptnut_bot.py          — Main bot: streams, orders, risk management, state
v106_dynamic_rr.py    — Strategy: sweep → displacement → FVG zone → 1m entry
tsxapi4py/            — TopstepX/ProjectX Gateway API wrapper (REST + SignalR)
  └─ api/client.py    — REST client (bars, orders, positions)
  └─ real_time/       — DataStream (quotes/trades), UserHubStream (fills/positions)
dashboard.py          — Sci-fi HUD web dashboard (localhost:8050)
license_client.py     — License validation (startup + hourly)
license_server/       — Cloudflare Worker at license.ptnuttrading.com
```

---

## Strategy: V106 ICT Sweep → Displacement → FVG Zone

### Signal Generation Flow
1. **Liquidity Levels** — `get_liquidity_levels()`: PDH/PDL, 2-day H/L, session swings, Asia/London/PreMarket
2. **Sweep Detection** — `detect_sweep_at()`: Price sweeps liquidity on 5m bars (lookback=12)
3. **Displacement Confirmation** — Body/range >= 35% in sweep direction (within 6 bars)
4. **FVG Zone Building** — Two types:
   - `ifvg` (inverted FVG): Old gap from before displacement, price traded through it
   - `disp_fvg` (displacement FVG): Gap created by the impulse move itself
5. **1m Entry** — First 1m bar that touches the zone = entry signal

### Confluence Scoring (4T-v3)
| Confluence | Points | Check Function |
|-----------|--------|----------------|
| Base (always) | 1 | — |
| Rejection candle | +1 | `e.get("rej")` |
| CISD on 5m | +1 | `cisd_5m(b5, bar_idx, ds5)` |
| Sweep detection | +2 | `detect_sweep_at(b5, bar_idx, liq, lookback=8)` |
| 15m structure | +2 | `structure_15m(b15, dr15, today, ns)` |
| 15m sweep | +1 | `sweep_15m(b15, dr15, today, ns, liq, side)` |

**Score range: 1-7**

### Dynamic RR Map
| Highest Confluence | RR |
|---|---|
| Sweep detected | 2.0 |
| 15m Structure (no sweep) | 1.7 |
| CISD (no struct) | 1.5 |
| Bare (none) | 1.3 |

### 1m-Beyond-5m Scanning (CRITICAL FIX — Mar 19)
`gen_sweep_entries()` now checks 1m bars PAST the last completed 5m bar for zone touches. This replaces the old `pending_out` / `_pending_zones` / `_check_pending_zones` system entirely.

**How it works:**
- After scanning completed 5m bars for zone touches, if `entries_from_this < 2`:
- Find 1m bars after `last_5m_end` (the end of the last completed 5m bar)
- Check those 1m bars against untouched zones
- First touch = entry (same as 5m logic)
- This gives near-real-time entries without waiting for the next 5m bar

**The old system (`pending_out`) is DEAD CODE.** Do not use it.

---

## Zone Re-Entry Prevention (CRITICAL FIX — Mar 19)

### The Bug
The 1m-beyond-5m scanning finds different 1m bars touching the SAME FVG zone on every scan. Each 1m bar has a different `ns` timestamp, so `self.seen` (keyed by `ns`) doesn't catch duplicates. The bot entered the same bull IFVG zone 3 times on Mar 19:
- 10:35 BULL @ 24411.50 → WIN +$946
- 10:43 BULL @ 24415.25 → LOSS -$884
- 10:48 BULL @ 24411.50 → LOSS -$178

### The Fix
**Zone key:** `(side, zone_type, round(zone_top, 2), round(zone_bot, 2))`

- `zone_top` and `zone_bot` are the FVG zone's actual price bounds (e.g., 24412.75 / 24407.50)
- Added to entry dicts in `gen_sweep_entries()` (both 5m-bar and 1m-beyond-5m sections)
- Stored in signal dict as `zone_top` and `zone_bot`
- `_used_zones` set in V106Scanner tracks which zones have been traded
- Checked BEFORE signals are returned from `scan()` (not just in `mark_executed`)
- **Persisted to `ptnut_state.json`** — survives restarts
- **Restored on startup** via `_load_state()`
- **Cleared on `new_day()`** — fresh zones each trading day

### Why the Old Key Was Wrong
Old: `(side, round(swept, 2))` where `swept` = sweep price level (e.g., 24478.0)
- `swept` is the liquidity level that was swept, NOT the zone entry area
- Two different zones from the same sweep have the same `swept` value
- But the zone bounds (top/bot) are what actually define the entry area

### Files Changed
- `v106_dynamic_rr.py`: Added `zone_top: z["top"], zone_bot: z["bot"]` to both entry append locations
- `ptnut_bot.py`: Changed zone key, added to signal dict, persist/restore in state

---

## Bot Configuration

```python
# Contract
NQ_CONTRACT_FALLBACK = "CON.F.US.ENQ.M26"   # June 2026 (current front month)
NQ_CONTRACT_PREV     = "CON.F.US.ENQ.H26"   # Mar 2026 (history backfill)

NQ = {
    "contract_id": NQ_CONTRACT,
    "pv": 20,            # $20 per point (NQ micro)
    "contracts": 3,      # Always 3
    "slip": 0.5,         # 0.5pt slippage on entry
    "tick_size": 0.25,
}

# Risk Management
MAX_RISK = 1000          # $1000 max risk per trade
MCL = 3                  # Max 3 consecutive losses per side
GMCL = 5                 # Max 5 global consecutive losses → stop trading
DLL = -2000              # Daily loss limit → stop trading
COOLDOWN = 120           # 2min between trades
TIMEOUT = 390            # 6.5hr position timeout (bracket should resolve way before)

# Kill Zone
KZ_START = (7, 30)       # 7:30 AM CT
KZ_END = (14, 30)        # 2:30 PM CT

# Operations
SCAN_INTERVAL = 60       # Fallback scan every 60s
TOKEN_REFRESH_HOURS = 4  # Refresh auth token every 4h
PAPER_MODE = False       # True = alerts only, False = live orders
```

---

## OrderStatus Enum (CRITICAL)

The `tsxapipy` library has WRONG enum values. The bot overrides with correct ProjectX API values:

```python
ORDER_STATUS_NONE       = 0
ORDER_STATUS_OPEN       = 1
ORDER_STATUS_FILLED     = 2   # tsxapipy says 4 — WRONG
ORDER_STATUS_CANCELLED  = 3
ORDER_STATUS_EXPIRED    = 4
ORDER_STATUS_REJECTED   = 5
ORDER_STATUS_PENDING    = 6
ORDER_STATUS_PEND_CANCEL = 7
ORDER_STATUS_SUSPENDED  = 8
```

---

## WebSocket Streams

### DataStream (Market Data)
```python
self.data_stream = DataStream(
    api_client=self.api,
    contract_id_to_subscribe=cid,
    on_quote_callback=self._on_quote,          # Every tick: price, bid/ask
    on_trade_callback=self._on_market_trade,   # Tape trades: price only
    auto_subscribe_quotes=True,
    auto_subscribe_trades=True,
    auto_subscribe_depth=False,
)
```

### UserHubStream (Account Events)
```python
self.user_stream = UserHubStream(
    api_client=self.api,
    account_id_to_watch=self.account_id,
    on_order_update=self._on_order_update,       # SL/TP fill detection
    on_position_update=self._on_position_update, # Backup flat detection
    on_user_trade_update=self._on_user_trade,    # Fill price, P&L, fees
    on_account_update=self._on_account_update,   # canTrade flag
)
```

### Quote Callback Details
- Use `lastUpdated` (NOT `timestamp`) for real-time server clock
- `timestamp` is stale session open time — useless
- `bestBid` / `bestAsk` tracked for spread awareness
- Feeds every tick to `scanner.on_tick()` → builds 1m bars in real-time
- 1m bar close triggers scan via `_scan_event`
- 5m boundary latched via `_is_5m_boundary` flag

### GatewayTrade (Tape) Callback
- Updates `live_price` ONLY
- Do NOT build bars from tape trades — causes duplicate 1m bar closes
- Quote callback handles all bar building

### GatewayUserTrade Callback
- Fields: `price, size, orderId, profitAndLoss, fees, voided`
- Check `voided` — skip voided trades
- Store `_broker_pnl, _broker_fees, _exit_order_id` for accurate exit reporting
- Prefer broker P&L over manual calculation

### Account Update Callback
- `canTrade` boolean from broker
- Checked before every entry: `if not self._can_trade: SKIP`

---

## Order Execution (Separate Orders, NOT Brackets)

TopstepX rejects bracket orders when Position Brackets are configured in the platform. The bot uses separate orders:

1. **Market entry order** → wait 2s for fill
2. **Get actual fill price** from broker position (`average_price`)
3. **Calculate SL/TP from ACTUAL fill price** (not signal entry):
   - `stop_px = fill_price - risk_pts` (bull) or `fill_price + risk_pts` (bear)
   - `target_px = fill_price + risk_pts * rr` (bull) or `fill_price - risk_pts * rr` (bear)
4. **Fill drift check**: If `|fill_price - signal_entry| > 50% of expected reward` → flatten and bail
5. **Place SL** (stop_market, opposite side)
6. **Place TP** (limit, opposite side)

### Exit Detection
- Primary: `_on_order_update` detects SL or TP fill → sets `_exit_event`
- Backup: `_on_position_update` detects flat position → sets `_exit_event`
- Main loop checks `_exit_event.is_set()` → calls `_handle_exit()`

### Exit P&L
- Prefer `_broker_pnl` from GatewayUserTrade (exact)
- Fallback: `(exit - entry) * direction * pv * contracts - fees`
- Fees: `$4.50 per contract per side` (entry + exit = $4.50 × 3 × 2 = $27)

---

## Bar Management

### Bar Types
| Timeframe | Source | When |
|-----------|--------|------|
| 5m | WS-built from 5 completed 1m bars | 5m boundary (instant) |
| 5m (fallback) | REST fetch latest bar | If WS 1m insufficient |
| 15m | WS-built from 15 completed 1m bars | 15m boundary |
| 15m (fallback) | REST fetch latest bar | If WS 1m insufficient |
| 1m | WebSocket quotes (on_tick) | Every tick |

### REST History Loading
- **Startup:** 10 days of 5m + 15m + 1m (yesterday 5pm CT → now)
- **Background sync:** Every 30min, daemon thread, atomic cache swap
- **Rollover backfill:** If current contract < 500 bars, prepend prior contract bars
- **MAX_BARS_PER_REQUEST = 20000** (API supports up to 20,000, library was capping at 1,000)

### BUG 16: Virtual 5m Bar — DISABLED FOREVER
The virtual 5m bar aggregated live 1m data into a "virtual" 5m bar for earlier entry. But it produced phantom signals that the backtest doesn't see (different OHLC). Caused a $1000 BEAR loss on Mar 17. **NEVER re-enable.**

---

## Safety Checks (In Order of Execution)

### Before Entry (in enter_trade)
1. `_can_trade` — broker allows trading
2. `live_price > 0` — WebSocket connected
3. `|live_price - entry| <= 5.0pts` — price drift filter
4. Price not at/past stop — already a loser
5. Price not past target — trade already over
6. Fill drift < 50% of reward — bad fill protection

### Before Signal Processing (in main loop)
1. `sig_age <= 120s` — stale signal filter (time since first discovery)
2. `cl_side < MCL (3)` — per-side consecutive loss limit
3. `gc < GMCL (5)` — global consecutive loss limit
4. `daily_pnl > DLL (-$2000)` — daily loss limit
5. `time since last_exit >= COOLDOWN (120s)` — cooldown between trades
6. Kill zone: 7:30-14:30 CT
7. Trading hours: Mon-Fri 7am-4pm CT
8. Not a holiday

### In Scan (before signal returned)
1. `ns not in self.seen` — signal timestamp dedup
2. `bar_idx not in seen_bars` — one signal per 5m bar
3. `zone_key not in _used_zones` — zone dedup (no re-entry)
4. `risk * pv * contracts <= MAX_RISK` — risk filter

---

## State Persistence

### ptnut_state.json
```json
{
    "date": "2026-03-19",
    "time": "14:23:45",
    "pnl": -759.0,
    "trades": 4,
    "cl_bull": 2,
    "cl_bear": 0,
    "gc": 2,
    "live_price": 24420.50,
    "mode": "LIVE",
    "status": "running",
    "kz_active": true,
    "quote_count": 12543,
    "in_position": false,
    "used_zones": [
        ["bull", "ifvg", 24412.75, 24407.5]
    ]
}
```

- Saved on every trade + every 50 quotes (~5s)
- Restored on startup if date matches today
- `used_zones` persisted as list of lists, restored as set of tuples

### ptnut_trades.json
- Append-only trade log (ENTRY + EXIT records)
- Used by dashboard for trade history display
- Contains: type, date, time, side, entry, stop, target, risk, rr, score, zone_type, contracts, exit, pnl, result

---

## Token Refresh

```python
def _refresh_token(self):
    # Every 4 hours
    # Uses APIClient._ensure_valid_token() to get new token
    # Calls stream.update_token() on both DataStream and UserHubStream
    # Falls back to stream stop/start if update_token fails
    # Less data gap than full restart
```

---

## Stream Heartbeat & Reconnection

- Main loop checks `_last_quote_time` — reconnects if no quote for 120s
- Position mode: reconnects if no quote for 60s (more aggressive)
- Reconnect: stop streams → sleep 2s → start streams
- Tracked via `_last_reconnect` to avoid reconnect storms

---

## Known Bugs Fixed

| Bug # | Description | Fix | Date |
|-------|-------------|-----|------|
| 1 | REST sync blocks scan loop | Background daemon thread | Mar 12 |
| 2 | REST sync misses signals | Force scan_event after sync | Mar 12 |
| 9 | Partial cache update race | Atomic swap (build all → swap all) | Mar 14 |
| 10 | Event clearing loses signals | Don't clear scan_event prematurely | Mar 14 |
| 12 | Silent errors in _on_quote | Log all errors | Mar 14 |
| 13 | Bracket fill price unknown | Store from GatewayUserTrade | Mar 15 |
| 14/15 | Stale check wrong baseline | Measure from first discovery, not bar close | Mar 15 |
| 16 | Virtual 5m phantom signals | DISABLED forever | Mar 17 |
| 18 | WS-built 5m bars wrong OHLC | Use REST or build from completed 1m only | Mar 17 |
| 19 | new_day() wipes HTF cache | Don't clear multi-day lookback caches | Mar 18 |
| 22 | 3min same-side dedup too aggressive | Removed (was preventing valid entries) | Mar 18 |
| — | Zone re-entry (same zone, different 1m bars) | zone_top/zone_bot key + persist | Mar 19 |

---

## How to Launch

```bash
# Set PYTHONPATH for tsxapipy
PYTHONPATH=/Users/tradingbot/trading-bot/tsxapi4py/src python3 ptnut_bot.py
```

Requires:
- `.env` file with TopstepX credentials (USERNAME, API_KEY)
- Internet connection for REST + WebSocket
- Python 3.9+ with: requests, pydantic, python-dotenv, signalrcore, websocket-client

---

## Testing

```bash
# Quick scan test (no orders)
PYTHONPATH=/Users/tradingbot/trading-bot/tsxapi4py/src python3 test_scan.py

# Backtest against historical data
PYTHONPATH=/Users/tradingbot/trading-bot/tsxapi4py/src python3 backtest_topstep.py
```

---

## File Change Summary (Mar 19 Session)

### v106_dynamic_rr.py
- Added `zone_top: z["top"], zone_bot: z["bot"]` to entry dicts (both 5m-bar and 1m-beyond-5m sections)
- No logic changes — just extra fields on existing dicts

### ptnut_bot.py
- **Zone dedup key** changed from `(side, round(swept, 2))` to `(side, zone_type, round(zone_top, 2), round(zone_bot, 2))`
- **Signal dict** now includes `zone_top` and `zone_bot`
- **`mark_executed()`** uses new zone key
- **`_save_state()`** persists `used_zones` to JSON
- **`_load_state()`** restores `used_zones` from JSON on startup
- **Removed old pending zone system** — replaced by gen_sweep_entries' 1m-beyond-5m scanning
- **`lastUpdated` timestamp fix** — use `lastUpdated` (real-time) not `timestamp` (stale)
- **`_on_market_trade`** simplified to price-only (no bar building)
- **`_on_user_trade`** captures all API fields (voided, profitAndLoss, fees, orderId)
- **`_on_account_update`** tracks `canTrade` flag
- **`bestBid`/`bestAsk`** tracking in `_on_quote`

### tsxapi4py/src/tsxapipy/api/client.py
- `MAX_BARS_PER_REQUEST = 20000` (was 1000, API supports 20,000)
