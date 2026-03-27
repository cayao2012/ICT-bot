# Bugs Found — March 9, 2026

## BUG 1: REST sync blocks scan loop → signals fire 6 minutes late

**Impact**: Missed a winning BEAR entry today. Signal entry price was 7pts stale.

**What happened**:
- 07:30:05 — REST sync starts inside `scan()` function (line 406-411 in ptnut_bot.py)
- REST sync takes ~3-6 seconds + has a `time.sleep(3)` hardcoded
- During REST sync, WebSocket 1m bar close events fire but scan loop is BLOCKED
- 07:36:40 — First scan finally runs, finds signal from bar that closed ~07:30-07:35
- Entry price 24398.00 is from that OLD bar's close
- Live price is now 24405.00 — 7pts away → drift filter kills it

**Root cause**: `scan()` at line 370 calls `refresh_htf_bars()` + `time.sleep(3)` + `load_historical_1m()` INSIDE the scan function. This blocks the entire scan loop. No signals can be detected while REST is fetching.

**Fix needed**: Move REST sync OUT of `scan()`. REST should run in a background thread or be called separately from the main loop BEFORE calling scan. The scan function should ONLY use cached data — zero network calls, zero sleeps.

**Files**: `ptnut_bot.py` lines 405-411 (REST inside scan), lines 1297-1342 (main loop)

---

## BUG 2: 60-second fallback scan doesn't catch post-REST signals

**What happened**: Line 1310 has a fallback `time.time() - self._last_scan >= SCAN_INTERVAL` (60s). But after REST sync completes, `_last_scan` gets updated (line 1315), so the fallback timer resets. The next scan won't fire until either a WebSocket bar close event OR 60 more seconds pass.

**Fix needed**: Force an immediate scan after any REST sync completes, don't wait for bar close or 60s timer.

---

## BUG 3: Two signals fired 62 seconds apart for same setup

**What happened**:
- 07:36:40 — BEAR @ 24398.00 (RR 2.0, Score 5, risk 15.5pts)
- 07:37:42 — BEAR @ 24408.50 (RR 1.3, Score 2, risk 5.2pts)

These are two different entries from the same bearish move but with different parameters. The second one (score 2, RR 1.3) is a worse version of the same setup. The `seen` set (line 449) tracks by `e["ns"]` (bar timestamp), so if the second signal comes from a different 1m bar, it passes as "new".

**Question**: Should there be a cooldown between signals on the same side? Or should only the highest-scoring signal be kept?

---

## BUG 4: `time.sleep(3)` inside scan function

**Location**: `ptnut_bot.py` line 409

```python
self.refresh_htf_bars()
time.sleep(3)  # WHY IS THIS HERE?
self.load_historical_1m()
```

This adds 3 seconds of dead time inside every REST sync where the bot can't detect or act on signals.

**Fix**: Remove the sleep entirely, or if it's needed for API rate limiting, move it out of the scan path.

---

## BUG 5: start_bot.sh missing PYTHONPATH

**Fixed already** — added `export PYTHONPATH="$BOT_DIR/tsxapi4py/src:$PYTHONPATH"` to start_bot.sh. Bot was crashing on `ModuleNotFoundError: No module named 'tsxapipy'` on every start.

---

## BUG 6: Stale signal check logs at DEBUG — silently kills signals

**Location**: `ptnut_bot.py` line 1367-1368

```python
if sig_age > 120:
    logger.debug(f"  Skipping stale signal ({sig_age:.0f}s old)")  # DEBUG = invisible
```

**What happened at 09:36:00**: A signal was detected ("Scan: 1 new signal(s)") but NOTHING logged after it. No ">>> ENTRY", no "SKIP". The signal passed the risk filter in `scan()` (otherwise it wouldn't count as a signal), but then got silently killed by the stale check in the main loop because the signal's `time` field is the 1m bar timestamp from minutes ago.

**Fix needed**: Change `logger.debug` to `logger.warning` so stale skips are visible. Also consider whether 120s is too tight — signals from bars that closed 2+ minutes ago are valid if price hasn't moved.

---

## BUG 7 (CONFIRMED BY OTHER CLAUDE): Signal risk too high for some entries

Some raw entries from `gen_sweep_entries()` have risk like $5,220 (87pts × $20 × 3ct). These get filtered at line 492-494 (`ar > MAX_RISK`), but silently — no log. If ALL entries from a scan have risk > $1,000, the scan returns 0 signals and nothing logs. This was the root cause of 0 trades on March 6.

**Fix needed**: Log when entries are filtered by risk so we can see "X entries found, Y filtered by risk, Z passed".

---

## Summary

Two main issues killing trades:
1. **Stale signals**: Signal `time` is from the 1m bar that touched the zone (could be minutes old). The 120s stale check and 5pt drift check both kill these.
2. **Silent filtering**: Risk filter and stale filter log at DEBUG or not at all. Impossible to diagnose without reading code.

Today's result: 0 trades, 3 signals detected but all filtered (2 by drift at 07:36-07:37, 1 by stale at 09:36).

---

## BUG 15: Stop calculation spans entire displacement-to-entry range → 85+ pt stops

**Impact**: ALL signals today killed by risk filter. Stops are 85-89 pts away = $5,000+ risk at 3ct vs $1,000 max.

**Live evidence (10:58:22)**:
```
Risk filter: bull disp_fvg @ 24479.50 — $5,265 > $1000 (risk=87.8pts × 3ct)
Risk filter: bull disp_fvg @ 24556.75 — $5,085 > $1000 (risk=84.8pts × 3ct)
Risk filter: bull disp_fvg @ 24561.25 — $5,355 > $1000 (risk=89.2pts × 3ct)
```

**Root cause** (`v106_dynamic_rr.py` lines 312-314, 322-324):

For bull entries:
```python
pl = min(stop_b5[m]["low"] for m in range(max(disp_idx+1, sw_bar), stop_end))
sp = min(sw_lvl, pl, c1["low"]) - 1.0
```

The stop is `min()` of:
1. `sw_lvl` — the sweep level
2. `pl` — lowest low of ALL 5m bars from displacement to entry
3. `c1["low"]` — the 1m bar's low

If the displacement happened hours ago (e.g. 08:00) and the entry touch is at 10:58, `pl` captures 3 hours of NQ lows. That's why stops are 85+ pts.

**Fix needed**: Stop should be based on immediate structure around entry — e.g. the FVG zone edge, the displacement bar's extreme, or a fixed lookback (last N bars), NOT the entire range from displacement to entry.

**UPDATE**: This is NOT a bug. The backtest uses the same stop calculation and is profitable. The risk filter is working as designed — rejecting entries where stops are too far. Today just didn't produce setups with tight enough stops. Strategy is selective, not broken.

**Live logs 11:37-11:43 (post-fix bot)**:
- Bot scanning every 1m bar close ✓
- 5m bars building from WebSocket ✓ (e.g. "Built 5m bar from WS: 11:35")
- 15m bars loaded (230 in cache) ✓
- 4-5 raw entries found per scan, all filtered by risk
- New entry appeared at 11:41: bull disp_fvg @ 24727.75 — $4,725 (78.8pts × 3ct)
- 1 stale BEAR from 07:37 correctly rejected (12,083s old)
- No valid entries with risk ≤ $1,000 today
