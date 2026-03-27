"""
PTNUT Trading Bot — V106 Sweep > Displacement > FVG Zone
=========================================================
NQ only | 3ct | 4T-v3 Confluence RR (bare→1.3, cisd→1.5, struct→1.7, sweep→2.0)
Uses EXACT same signal functions as v106_dynamic_rr.py backtest.
Same code path = same signals = 100% match.

Strategy:
  1. Build liquidity levels (PDH/PDL, 2-day H/L, session swings, Asia/London/PreMarket)
  2. Detect sweep of liquidity on 5m bars (lookback=12)
  3. Confirm displacement candle (body/range >= 35%)
  4. Build FVG zones: IFVG (inverted old gaps) + disp_fvg (gaps from impulse)
  5. Monitor 1m bars for first touch of zone = entry
  6. Confluence RR: sweep→2.0R, struct→1.7R, cisd→1.5R, bare→1.3R
  7. Bracket order: market entry + stop_market + limit target

Risk: 3ct, MR $1000, MCL 3/side, GMCL 5, DLL -$2000, cooldown 2min, timeout 390min (bracket resolves)
Kill Zone: 7:30-14:30 CT
"""

import os
import sys
import json
import time
import logging
import threading
import requests as _requests
from datetime import datetime, timedelta, date as dt_date
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from typing import Optional

from tsxapipy import (
    authenticate, APIClient, OrderPlacer,
)
from market_stream import MarketStream
from user_stream import UserStream

# Official ProjectX OrderStatus enum (from Swagger spec — tsxapipy constants are WRONG)
# tsxapipy has FILLED=4 but the real API has FILLED=2. Using library = never detect fills.
ORDER_STATUS_NONE       = 0
ORDER_STATUS_OPEN       = 1
ORDER_STATUS_FILLED     = 2
ORDER_STATUS_CANCELLED  = 3
ORDER_STATUS_EXPIRED    = 4
ORDER_STATUS_REJECTED   = 5
ORDER_STATUS_PENDING    = 6
ORDER_STATUS_PEND_CANCEL = 7
ORDER_STATUS_SUSPENDED  = 8
# Contract ID resolved at runtime in PTNUTBot.run(), not import time.
NQ_CONTRACT_FALLBACK = "CON.F.US.ENQ.M26"   # June 2026 (current front month)
NQ_CONTRACT_PREV     = "CON.F.US.ENQ.H26"   # Mar 2026 (prior front month — history backfill)
NQ_CONTRACT = None  # Set by resolve_contract() at startup

# EXACT backtest functions — same code path = same signals
from v106_dynamic_rr import (
    get_liquidity_levels,
    detect_sweep_at,
    cisd_5m,
    structure_15m,
    sweep_15m,
)
from backtest_entry_modes import (
    gen_sweep_entries_enriched,
    apply_entry_mode,
    MODE_CLOSE_ENTRY,
    build_dr_htf,
)

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
CT = ZoneInfo("America/Chicago")
STATE_FILE = "ptnut_state.json"

TELEGRAM_BOT_TOKEN = "8482411404:AAGDE6EkrgEPTlkGdO-EkzdNicEqjkzJ3IU"
TELEGRAM_CHAT_ID = "8203680695"

NQ = {
    "contract_id": NQ_CONTRACT,
    "pv": 20, "contracts": 4, "slip": 0.5, "tick_size": 0.25,
}

MAX_RISK = 1000
MCL = 3
GMCL = 2  # Blake: 2 consecutive losses = done for the day
DLL = -2000
COOLDOWN = 120
TIMEOUT = 390  # Match backtest sim_dynamic(to1=390) — bracket stop/target resolves the trade
KZ_START = (17, 0)   # PAPER VALIDATION: wide KZ to see overnight signals. Set back to (7,30) for live
KZ_END = (14, 30)
SCAN_INTERVAL = 60
TOKEN_REFRESH_HOURS = 4
PAPER_MODE = True  # True = signal alerts only (no orders), False = live trading

logger = logging.getLogger("PTNUT")


# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════
def tg(msg):
    try:
        _requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
# CONTRACT RESOLUTION
# ═══════════════════════════════════════════════════════════════
def _to_utc(ct_dt):
    """Convert CT datetime to UTC ISO string for API calls."""
    return ct_dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S")


def _verify_contract(api_client, cid):
    """Try to fetch bars with a contract ID. Returns True if it works."""
    try:
        now = datetime.now(CT)
        start = _to_utc(now - timedelta(hours=6))
        end = _to_utc(now)
        resp = api_client.get_historical_bars(
            contract_id=cid, start_time_iso=start, end_time_iso=end,
            unit=2, unit_number=5, limit=10, live=False,
        )
        if resp and resp.bars and len(resp.bars) > 0:
            return True
    except Exception:
        pass
    return False


def resolve_contract(api_client):
    """Resolve NQ contract ID at runtime. Validates with actual bar fetch."""
    global NQ_CONTRACT

    # Try API search first, but VERIFY it actually returns bars
    try:
        from tsxapipy.api.contract_utils import get_futures_contract_details
        from datetime import date
        result = get_futures_contract_details(api_client, date.today(), "ENQ")
        if result:
            str_id, int_id = result
            if str_id and str_id != "":
                if _verify_contract(api_client, str_id):
                    NQ_CONTRACT = str_id
                    NQ["contract_id"] = NQ_CONTRACT
                    logger.info(f"Contract resolved via API: {NQ_CONTRACT} (int_id={int_id})")
                    return True
                else:
                    logger.warning(f"API returned {str_id} but bar fetch failed — trying fallback")
    except Exception as e:
        logger.warning(f"Contract API search failed: {e}")

    # Fallback to hardcoded contract ID
    if _verify_contract(api_client, NQ_CONTRACT_FALLBACK):
        NQ_CONTRACT = NQ_CONTRACT_FALLBACK
        NQ["contract_id"] = NQ_CONTRACT
        logger.info(f"Using fallback contract: {NQ_CONTRACT} (verified)")
        return True

    # Last resort: use fallback even without verification (market may be closed)
    NQ_CONTRACT = NQ_CONTRACT_FALLBACK
    NQ["contract_id"] = NQ_CONTRACT
    logger.warning(f"Using fallback contract unverified: {NQ_CONTRACT} (market may be closed)")
    return True


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def tick_round(price, tick=0.25):
    return round(round(price / tick) * tick, 2)


def anti_martingale_cts(gc):
    """3ct → 2ct → 1ct after consecutive losses. Win resets to 3."""
    if gc <= 0: return 3
    if gc == 1: return 2
    return 1


def _in_kz(h, m):
    t = h * 60 + m
    s = KZ_START[0] * 60 + KZ_START[1]
    e = KZ_END[0] * 60 + KZ_END[1]
    if s < e:
        return s <= t < e
    else:  # overnight wrap (e.g. 17:00 -> 14:30)
        return t >= s or t < e


@dataclass
class Position:
    side: str
    entry_price: float
    stop_price: float
    target_price: float
    risk: float
    rr: float
    score: int
    zone_type: str
    contracts: int
    entry_time: datetime
    entry_order_id: int = 0
    stop_order_id: int = 0
    target_order_id: int = 0


# ═══════════════════════════════════════════════════════════════
# TRADING DATE — canonical session boundary helper
# ═══════════════════════════════════════════════════════════════
def _bar_trading_date(ts_ns):
    """Return the trading session date for a bar given its timestamp in nanoseconds.

    CME futures sessions start at 17:00 CT. A bar timestamped at or after 17:00 CT
    belongs to the *next* calendar day's trading session. This matches the convention
    used in bars_cache.pkl (built by build_new_bars.py), which backtest_run.py reads
    via bar["date"]. All live bar date grouping must use this same rule so that
    dr5/dr15 keys are identical between replay and live execution.

    Examples (all CT):
      Mon 16:59  ->  Mon  (same calendar day, before session boundary)
      Mon 17:00  ->  Tue  (at boundary: belongs to Tuesday's session)
      Sun 17:00  ->  Mon  (CME Sunday open = start of Monday session)
      Sun 20:00  ->  Mon  (Sunday evening = Monday's Asia session)
    """
    t = datetime.fromtimestamp(ts_ns / 1e9, tz=CT)
    d = t.date()
    if t.hour >= 17:
        d = d + timedelta(days=1)
    return d


# ═══════════════════════════════════════════════════════════════
# V106 SCANNER — Calls exact backtest functions on REST bars
# ═══════════════════════════════════════════════════════════════
class V106Scanner:
    """
    Calls exact same functions from v106_dynamic_rr.py on live data.
    Same code path = same signals = 100% match.

    Bar strategy:
    - 5m/15m bars: REST-fetched from broker on every 5m/15m boundary (~1-2s)
    - 1m bars: BUILT FROM WEBSOCKET QUOTES in real-time (instant entries)
    - BUG 18 fix: ALL 5m/15m bars come from REST (broker official bars).
      WS-built bars had different OHLC → phantom signals backtest doesn't see.
    """

    def __init__(self, api_client, contract_id):
        self.api = api_client
        self.cid = contract_id
        self.seen = set()  # {ns} of signals already executed
        self._used_zones = set()  # {(side, zt, zone_top, zone_bot)} — zones already traded, no re-entry
        # HTF bar cache (REST-fetched on 5m boundaries)
        self._b5_cache = []
        self._b15_cache = []
        self._dr5_cache = {}
        self._dr15_cache = {}
        self._all_dates_cache = set()
        self._last_5m_refresh = 0.0
        self._last_rest_sync = 0.0  # periodic full REST backup sync
        # 1m bars: REST history + live-built from quotes
        self._base_1m = []        # REST-fetched on startup / periodic sync
        self._live_1m = []        # built from WebSocket quotes
        self._current_bar = None  # bar currently being built
        self._bar_minute = -1     # minute of current bar
        # Bug 3 fix: same-side signal dedup — keep only highest score within 3min window
        self._last_signal_side = None  # "bull" or "bear"
        self._last_signal_ns = 0       # timestamp of last emitted signal
        # Track when signals are first discovered (for stale check)
        self._first_seen = {}         # ns → discovery datetime
        # 1m zone monitoring: zones found on 5m, entry on 1m touch
        self._pending_zones = []

    def _check_pending_zones(self, c1):
        """Check if a 1m bar touches any pending zone. Returns entries.

        This is the real-time 1m entry detection: zones are identified on 5m
        (sweep → displacement → FVG), then we monitor 1m bars for instant entry
        instead of waiting for the next 5m bar to complete (which adds 5min lag).
        Entry logic is IDENTICAL to gen_sweep_entries lines 311-328.

        Stop calculation matches backtest: only considers 5m bars from
        disp_idx+1 to current end of cache (equivalent to j+1 in backtest,
        since in live the cache end IS the current bar position)."""
        NS_MIN = 60_000_000_000
        KZ_START = 7 * 60 + 30   # 07:30 CT
        KZ_END   = 14 * 60 + 30  # 14:30 CT
        entries = []
        remaining = []
        b5_len = len(self._b5_cache)
        for pz in self._pending_zones:
            side = pz["side"]
            z = pz["zone"]
            sw_lvl = pz["sw_lvl"]
            sw_bar = pz["sw_bar"]
            disp_idx = pz["disp_idx"]
            touched = False

            # Expire zones past 60-bar window (matches gen_sweep_entries touch range)
            if b5_len - disp_idx >= 60:
                continue

            # Kill zone check on 1m bar (matches gen_sweep_entries line 305)
            bar_min = c1["hour"] * 60 + c1["minute"]
            if not (KZ_START <= bar_min < KZ_END):
                remaining.append(pz)
                continue

            # Stop range: from displacement+1 to end of cache (= current 5m bar position)
            # Matches backtest: range(max(disp_idx+1, sw_bar), stop_end)
            stop_start = max(disp_idx + 1, sw_bar)
            stop_end = b5_len  # in live, cache end = latest completed 5m bar

            if side == "bull" and c1["low"] <= z["top"] and c1["close"] >= z["bot"]:
                ep = c1["close"]
                if stop_end > stop_start:
                    pl = min(self._b5_cache[m]["low"] for m in range(stop_start, stop_end))
                else:
                    pl = c1["low"]
                sp = min(sw_lvl, pl, c1["low"]) - 1.0
                risk = ep - sp
                if risk > 0:
                    rej = c1["low"] < z["ce"] and min(c1["open"], c1["close"]) >= z["bot"]
                    entries.append({
                        "ep": ep, "sp": sp, "ns": c1["time_ns"] + NS_MIN,
                        "rej": rej, "hour": c1["hour"], "bar_idx": disp_idx + 1,
                        "side": side, "zt": z["type"], "swept": sw_lvl, "sw_bar": sw_bar,
                    })
                    touched = True

            elif side == "bear" and c1["high"] >= z["bot"] and c1["close"] <= z["top"]:
                ep = c1["close"]
                if stop_end > stop_start:
                    ph = max(self._b5_cache[m]["high"] for m in range(stop_start, stop_end))
                else:
                    ph = c1["high"]
                sp = max(sw_lvl, ph, c1["high"]) + 1.0
                risk = sp - ep
                if risk > 0:
                    rej = c1["high"] > z["ce"] and max(c1["open"], c1["close"]) <= z["top"]
                    entries.append({
                        "ep": ep, "sp": sp, "ns": c1["time_ns"] + NS_MIN,
                        "rej": rej, "hour": c1["hour"], "bar_idx": disp_idx + 1,
                        "side": side, "zt": z["type"], "swept": sw_lvl, "sw_bar": sw_bar,
                    })
                    touched = True

            if not touched:
                remaining.append(pz)

        self._pending_zones = remaining
        return entries

    def _build_5m_from_1m(self, now):
        """Build a COMPLETED 5m bar from the last 5 completed 1m bars.
        Called at 5m boundary (e.g., 08:05) to get the 08:00 bar instantly.
        NOT a virtual bar — all 5 bars are closed/completed."""
        # Snap to 5m boundary: at 08:07 → bar_end=08:05, bar_start=08:00
        snapped_min = (now.minute // 5) * 5
        bar_end = now.replace(minute=snapped_min, second=0, microsecond=0)
        bar_start = bar_end - timedelta(minutes=5)
        start_ns = int(bar_start.timestamp() * 1e9)
        end_ns = int(bar_end.timestamp() * 1e9)

        all_1m = self._base_1m + self._live_1m
        period_bars = [b for b in all_1m if start_ns <= b["time_ns"] < end_ns]

        if len(period_bars) < 3:  # need at least 3 of 5 for reasonable OHLC
            return None

        period_bars.sort(key=lambda x: x["time_ns"])
        return {
            "time_ns": start_ns,
            "open": period_bars[0]["open"],
            "high": max(b["high"] for b in period_bars),
            "low": min(b["low"] for b in period_bars),
            "close": period_bars[-1]["close"],
            "hour": bar_start.hour,
            "minute": bar_start.minute,
        }

    def _build_15m_from_1m(self, now):
        """Build a COMPLETED 15m bar from the last 15 completed 1m bars."""
        # Snap to 15m boundary: at 08:17 → bar_end=08:15, bar_start=08:00
        snapped_min = (now.minute // 15) * 15
        bar_end = now.replace(minute=snapped_min, second=0, microsecond=0)
        bar_start = bar_end - timedelta(minutes=15)
        start_ns = int(bar_start.timestamp() * 1e9)
        end_ns = int(bar_end.timestamp() * 1e9)

        all_1m = self._base_1m + self._live_1m
        period_bars = [b for b in all_1m if start_ns <= b["time_ns"] < end_ns]

        if len(period_bars) < 10:  # need at least 10 of 15
            return None

        period_bars.sort(key=lambda x: x["time_ns"])
        return {
            "time_ns": start_ns,
            "open": period_bars[0]["open"],
            "high": max(b["high"] for b in period_bars),
            "low": min(b["low"] for b in period_bars),
            "close": period_bars[-1]["close"],
            "hour": bar_start.hour,
            "minute": bar_start.minute,
        }

    def validate_startup(self):
        """Fetch a few bars and verify REST format is correct. Call before trading."""
        logger.info("Validating REST bar format...")
        now = datetime.now(CT)
        start = _to_utc(now - timedelta(hours=2))
        end = _to_utc(now)
        try:
            resp = self.api.get_historical_bars(
                contract_id=self.cid, start_time_iso=start, end_time_iso=end,
                unit=2, unit_number=5, limit=1000, live=False,
            )
            raw = resp.bars if resp else []
        except Exception as e:
            logger.error(f"  REST bar fetch FAILED: {e}")
            logger.error("  Cannot validate bar format — check API credentials and contract ID")
            return False
        if not raw:
            logger.warning("  No bars returned (market may be closed)")
            return True  # not an error, just no data
        b = raw[0]
        # Verify expected attributes exist (BarData: t, o, h, l, c, v)
        try:
            t = b.t if hasattr(b.t, 'astimezone') else datetime.fromisoformat(str(b.t))
            t = t.astimezone(CT)
            o, h, l, c = float(b.o), float(b.h), float(b.l), float(b.c)
            logger.info(f"  Bar format OK: {t.strftime('%H:%M')} O={o:.2f} H={h:.2f} L={l:.2f} C={c:.2f}")
            logger.info(f"  {len(raw)} bars fetched in test window")
            # Sanity: NQ should be in 10000-30000 range
            if not (5000 < h < 50000):
                logger.warning(f"  Price {h:.2f} looks unusual for NQ — verify contract ID")
            return True
        except AttributeError as e:
            logger.error(f"  Bar attribute error: {e}")
            logger.error(f"  Bar object has: {dir(b)}")
            logger.error("  REST bar format doesn't match expected — CANNOT TRADE")
            return False
        except Exception as e:
            logger.error(f"  Bar validation failed: {e}")
            return False

    def on_tick(self, price, ct_now):
        """Called on every WebSocket quote. Builds 1m bars in real-time.
        Returns True if a 1m bar just closed (scan should fire)."""
        cur_min = ct_now.hour * 60 + ct_now.minute
        bar_closed = False

        if cur_min != self._bar_minute:
            # Minute changed — close previous bar if it exists
            if self._current_bar is not None:
                self._live_1m.append(self._current_bar)
                bar_closed = True

            # Start new bar
            self._bar_minute = cur_min
            t_ns = int(ct_now.replace(second=0, microsecond=0).timestamp() * 1e9)
            self._current_bar = {
                "time_ns": t_ns,
                "open": price, "high": price,
                "low": price, "close": price,
                "hour": ct_now.hour, "minute": ct_now.minute,
            }
        else:
            # Update current bar OHLC
            if self._current_bar is not None:
                if price > self._current_bar["high"]:
                    self._current_bar["high"] = price
                if price < self._current_bar["low"]:
                    self._current_bar["low"] = price
                self._current_bar["close"] = price

        return bar_closed

    def load_historical_1m(self):
        """REST-fetch 1m bars from session open (yesterday 5pm CT) through now.
        Covers overnight Asia/London/PreMarket so liquidity levels are complete."""
        now = datetime.now(CT)
        # Session starts 5pm CT previous day — fetch from there to capture overnight
        yesterday_5pm = (now.replace(hour=17, minute=0, second=0) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
        end = now.strftime("%Y-%m-%dT%H:%M:%S")
        rest_bars = self._fetch(1, yesterday_5pm, end)
        if rest_bars:
            # REST has today's bars — keep any live bars newer than REST
            last_rest_ns = rest_bars[-1]["time_ns"]
            newer_live = [b for b in self._live_1m if b["time_ns"] > last_rest_ns]
            self._base_1m = rest_bars
            self._live_1m = newer_live
            logger.info(f"  1m history loaded: {len(rest_bars)} REST + {len(newer_live)} live kept")
        else:
            # REST returned nothing — keep existing WebSocket-built bars
            logger.info(f"  1m REST returned 0 bars — keeping {len(self._base_1m)} base + {len(self._live_1m)} live")

    def _fetch_with_rollover(self, tf_minutes, start, end):
        """Fetch bars, backfilling from prior contract if current has limited history."""
        bars = self._fetch(tf_minutes, start, end)
        if bars and len(bars) < 500 and NQ_CONTRACT_PREV:
            # Current contract has limited data — backfill older bars from prior contract
            old_cid = self.cid
            self.cid = NQ_CONTRACT_PREV
            prev_bars = self._fetch(tf_minutes, start, end)
            self.cid = old_cid
            if prev_bars and len(prev_bars) > len(bars):
                # Use prior contract bars for dates before current contract starts
                first_new_ns = bars[0]["time_ns"]
                older = [b for b in prev_bars if b["time_ns"] < first_new_ns]
                if older:
                    bars = older + bars
                    logger.info(f"  Rollover backfill: {len(older)} {tf_minutes}m bars from prior contract")
        return bars

    def refresh_htf_bars(self):
        """Refresh 5m and 15m bar cache from REST + preserve WebSocket bars.

        REST may not return current-session bars (confirmed on practice account).
        Any WebSocket-built bars newer than REST data are preserved so the bot
        never loses live-built bars during periodic sync.
        """
        now = datetime.now(CT)
        start_hist = (now - timedelta(days=10)).strftime("%Y-%m-%dT00:00:00")
        end = now.strftime("%Y-%m-%dT%H:%M:%S")

        b5_rest = self._fetch_with_rollover(5, start_hist, end)
        if not b5_rest:
            return False
        # No sleep between fetches — _fetch() already has per-chunk sleep for rate limiting.
        # The old time.sleep(3) here was blocking the scan loop when called from scan().
        b15_rest = self._fetch_with_rollover(15, start_hist, end)

        # Preserve cached bars newer than bulk REST fetch (from _fetch_latest_bar calls)
        b5 = b5_rest
        if b5_rest and self._b5_cache:
            last_rest_ns = b5_rest[-1]["time_ns"]
            newer = [b for b in self._b5_cache if b["time_ns"] > last_rest_ns]
            if newer:
                b5 = b5_rest + newer
                logger.info(f"  Preserved {len(newer)} cached 5m bars newer than REST")

        b15 = b15_rest if b15_rest else []
        if b15_rest and self._b15_cache:
            last_rest_15_ns = b15_rest[-1]["time_ns"]
            newer_15 = [b for b in self._b15_cache if b["time_ns"] > last_rest_15_ns]
            if newer_15:
                b15 = b15_rest + newer_15
                logger.info(f"  Preserved {len(newer_15)} cached 15m bars newer than REST")

        self._b5_cache = b5
        self._b15_cache = b15
        self._dr5_cache = self._build_dr(b5)
        self._dr15_cache = self._build_dr(b15)
        self._all_dates_cache = set(self._dr5_cache.keys())
        self._last_5m_refresh = time.time()
        logger.info(f"  HTF cache refreshed: 5m={len(b5)} 15m={len(b15)}")
        return True

    def initial_load(self):
        """First boot: blocking REST fetch to get 10 days of history.
        Called ONCE before trading loop starts. NOT inside scan()."""
        if self._b5_cache:
            return True
        if not self.refresh_htf_bars():
            return False
        self.load_historical_1m()
        self._last_rest_sync = time.time()
        return True

    def background_rest_sync(self):
        """Periodic REST sync — runs in background thread, NEVER blocks scan().
        BUG 9 fix: Build all new data first, then swap caches atomically so
        scan() never sees partially-updated state (new bars + old date ranges)."""
        try:
            logger.info("  Background REST sync starting...")
            now = datetime.now(CT)
            start_hist = (now - timedelta(days=10)).strftime("%Y-%m-%dT00:00:00")
            end = now.strftime("%Y-%m-%dT%H:%M:%S")

            # Fetch everything first (slow part) — with rollover backfill
            b5_rest = self._fetch_with_rollover(5, start_hist, end)
            if not b5_rest:
                logger.warning("  Background REST sync: no 5m bars returned")
                return
            b15_rest = self._fetch_with_rollover(15, start_hist, end)

            # Preserve cached bars newer than bulk REST (from _fetch_latest_bar calls)
            b5_new = b5_rest
            if self._b5_cache:
                last_ns = b5_rest[-1]["time_ns"]
                newer = [b for b in self._b5_cache if b["time_ns"] > last_ns]
                if newer:
                    b5_new = b5_rest + newer
            b15_new = b15_rest if b15_rest else []
            if b15_rest and self._b15_cache:
                last_ns_15 = b15_rest[-1]["time_ns"]
                newer_15 = [b for b in self._b15_cache if b["time_ns"] > last_ns_15]
                if newer_15:
                    b15_new = b15_rest + newer_15

            # Build date ranges from new data
            dr5_new = self._build_dr(b5_new)
            dr15_new = self._build_dr(b15_new)
            all_dates_new = set(dr5_new.keys())

            # Load 1m bars
            yesterday_5pm = (now.replace(hour=17, minute=0, second=0) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
            rest_1m = self._fetch(1, yesterday_5pm, end)

            # ── ATOMIC SWAP: all caches updated together ──
            self._b5_cache = b5_new
            self._b15_cache = b15_new
            self._dr5_cache = dr5_new
            self._dr15_cache = dr15_new
            self._all_dates_cache = all_dates_new
            self._last_5m_refresh = time.time()
            if rest_1m:
                last_rest_ns = rest_1m[-1]["time_ns"]
                newer_live = [b for b in self._live_1m if b["time_ns"] > last_rest_ns]
                self._base_1m = rest_1m
                self._live_1m = newer_live

            self._last_rest_sync = time.time()
            logger.info(f"  Background REST sync complete: 5m={len(b5_new)} 15m={len(b15_new)} 1m={len(self._base_1m)}+{len(self._live_1m)}")
        except Exception as e:
            logger.error(f"  Background REST sync failed: {e}")

    def needs_rest_sync(self):
        """Check if periodic REST sync is due (every 30 min)."""
        return time.time() - self._last_rest_sync > 1800

    def scan(self, is_5m_boundary=False):
        """Run exact backtest functions on cached bars.

        5m/15m: built from completed WS 1m bars at boundaries (instant, no REST delay).
        Falls back to REST if WS 1m data insufficient.
        1m: WS-built for instant entry detection.
        """
        now = datetime.now(CT)
        # Use trading-date semantics: after 17:00 CT the current scan belongs to
        # tomorrow's session. This must match _build_dr so dr5[today] resolves.
        today = _bar_trading_date(int(now.timestamp() * 1e9))

        # No data yet — initial_load() must be called first
        if not self._b5_cache:
            logger.warning("  Scan aborted: _b5_cache is EMPTY (new_day wipe or initial_load failure)")
            return []

        # INSTANT 5m/15m bars: build COMPLETED bars from WS 1m data at boundaries.
        # NOT virtual bars (BUG 16) — these are fully closed bars from 5 completed 1m bars.
        # BUG 18 (REST-only) caused 4-min delay and killed the score-7 signal on 2026-03-17.
        # Mar 10 winner (+$1,966) was caught with WS-built bars — this restores that behavior.
        if is_5m_boundary:
            ws_5m = self._build_5m_from_1m(now)
            if ws_5m:
                if not self._b5_cache or ws_5m["time_ns"] != self._b5_cache[-1]["time_ns"]:
                    self._b5_cache.append(ws_5m)
                    self._dr5_cache = self._build_dr(self._b5_cache)
                    self._all_dates_cache = set(self._dr5_cache.keys())
                    logger.info(f"  WS 5m bar: {ws_5m['hour']:02d}:{ws_5m['minute']:02d} O={ws_5m['open']:.2f} H={ws_5m['high']:.2f} L={ws_5m['low']:.2f} C={ws_5m['close']:.2f} (total 5m={len(self._b5_cache)})")
                else:
                    logger.info(f"  5m bar duplicate, skipped")
            else:
                # Fallback to REST if WS 1m data insufficient
                new_5m = self._fetch_latest_bar(5)
                if new_5m:
                    if not self._b5_cache or new_5m["time_ns"] != self._b5_cache[-1]["time_ns"]:
                        self._b5_cache.append(new_5m)
                        self._dr5_cache = self._build_dr(self._b5_cache)
                        self._all_dates_cache = set(self._dr5_cache.keys())
                        logger.info(f"  REST 5m bar (fallback): {new_5m['hour']:02d}:{new_5m['minute']:02d} O={new_5m['open']:.2f} H={new_5m['high']:.2f} L={new_5m['low']:.2f} C={new_5m['close']:.2f} (total 5m={len(self._b5_cache)})")
                else:
                    logger.warning(f"  5m boundary but no WS or REST bar available")
            if now.minute % 15 == 0:
                ws_15m = self._build_15m_from_1m(now)
                if ws_15m:
                    if not self._b15_cache or ws_15m["time_ns"] != self._b15_cache[-1]["time_ns"]:
                        self._b15_cache.append(ws_15m)
                        self._dr15_cache = self._build_dr(self._b15_cache)
                        logger.info(f"  WS 15m bar: {ws_15m['hour']:02d}:{ws_15m['minute']:02d} (total 15m={len(self._b15_cache)})")
                else:
                    new_15m = self._fetch_latest_bar(15)
                    if new_15m:
                        if not self._b15_cache or new_15m["time_ns"] != self._b15_cache[-1]["time_ns"]:
                            self._b15_cache.append(new_15m)
                            self._dr15_cache = self._build_dr(self._b15_cache)
                            logger.info(f"  REST 15m bar (fallback): {new_15m['hour']:02d}:{new_15m['minute']:02d} (total 15m={len(self._b15_cache)})")
            self._last_5m_refresh = time.time()

        b5 = self._b5_cache
        b15 = self._b15_cache
        dr5 = self._dr5_cache
        dr15 = self._dr15_cache
        all_dates = self._all_dates_cache

        # 1m bars = REST history + live-built from quotes (ZERO network call)
        b1 = self._base_1m + self._live_1m

        logger.debug(f"  Bars: 5m={len(b5)} 1m={len(b1)} (base={len(self._base_1m)} live={len(self._live_1m)}) 15m={len(b15)}")

        if today not in dr5:
            logger.warning(f"  No 5m bars for today ({today}) in dr5: {sorted(dr5.keys())[-5:]}")
            return []

        # BUG 16: Virtual 5m bar DISABLED — causes phantom signals not in backtest.
        # The 10:41 BEAR loss on 2026-03-17 was a virtual-bar-only signal.
        # Bot must match backtest exactly. Max ~4min delay on completed bars only.
        ds5, de5 = dr5[today]

        # ── EXACT backtest function calls ──
        liq = get_liquidity_levels(b5, dr5, today, self._all_dates_cache)

        # ── gen_sweep_entries handles BOTH 5m-bar touches AND 1m-beyond-5m scanning ──
        # No separate pending zone system needed — gen_sweep_entries checks 1m bars
        # past the last 5m bar for zone touches on every call.
        if is_5m_boundary:
            # Refresh HTF bars first (done above), then scan
            pass

        # Use EXACT same signal generation as backtest
        # b5[:de5] is correct: _build_dr returns exclusive end (de5 = last_idx+1)
        all_raw = gen_sweep_entries_enriched(b5[:de5], b1, ds5, de5, today, liq)

        logger.info(f"  Scan: 5m={len(b5)} 1m={len(b1)} (base={len(self._base_1m)} live={len(self._live_1m)}) 15m={len(b15)} | Liq={len(liq)} Raw={len(all_raw)} [{ds5}:{de5}]")

        new_signals = []
        for raw in sorted(all_raw, key=lambda x: (x["ns"], -{"ifvg": 2, "disp_fvg": 1}.get(x["zt"], 0))):
            if raw["ns"] in self.seen:
                continue
            # Skip zones we already traded
            zone_key = (raw["side"], raw["zt"], round(raw.get("zone_top", 0), 2), round(raw.get("zone_bot", 0), 2))
            if zone_key in self._used_zones:
                logger.debug(f"  Zone dedup: skipping {zone_key}")
                continue

            # Apply entry mode — EXACT same function as backtest
            sig = apply_entry_mode(raw, MODE_CLOSE_ENTRY, b1, b5, ds5, liq, dr15, b15, today)
            if sig is None:
                continue
            if sig["zone"] not in ("disp_fvg", "ifvg"):
                continue

            # IFVG delay: skip IFVGs before 9:00 CT
            if sig["zone"] == "ifvg" and sig["time"].hour < 9:
                logger.info(f"  IFVG delay: skipping {sig['side']} ifvg @ {sig['entry']:.2f} — before 9:00 CT")
                continue

            # Flat 1.1 RR (matches backtest)
            sig["rr"] = 1.1
            ep = sig["entry"]
            sp = sig["stop"]
            risk = sig["risk_pts"]
            side = sig["side"]
            d_dir = 1 if side == "bull" else -1
            tp = ep + risk * sig["rr"] * d_dir
            nct = NQ["contracts"]  # 4ct for eval

            # Re-check MAX_RISK at actual execution contract count.
            # apply_entry_mode gates on CONTRACTS=3 (backtest_entry_modes module constant).
            # If NQ["contracts"] differs, a trade can pass the 3ct gate and execute over-limit.
            if risk * NQ["pv"] * nct > MAX_RISK:
                logger.info(
                    f"  Risk gate (exec): {sig['side']} {sig['zone']} @ {sig['entry']:.2f} "
                    f"rejected — ${risk * NQ['pv'] * nct:.0f} > ${MAX_RISK} at {nct}ct"
                )
                continue

            new_signals.append({
                "side": side,
                "entry": tick_round(ep),
                "stop": tick_round(sp),
                "target": tick_round(tp),
                "risk": risk,
                "rr": sig["rr"],
                "score": sig["score"],
                "zone_type": sig["zone"],
                "swept": raw.get("swept", 0),
                "zone_top": sig.get("zone_top", 0),
                "zone_bot": sig.get("zone_bot", 0),
                "contracts": nct,
                "time": sig["time"],
                "_ns": raw["ns"],
            })

        # Track first-discovery time for each signal (stale = time since discovery, not 1m close)
        for sig in new_signals:
            if sig["_ns"] not in self._first_seen:
                self._first_seen[sig["_ns"]] = now
            sig["_discovered_at"] = self._first_seen[sig["_ns"]]

        return new_signals

    def mark_executed(self, sig):
        self.seen.add(sig["_ns"])
        self._last_signal_side = sig["side"]
        self._last_signal_ns = sig["_ns"]
        # Mark zone as used — no re-entry on same FVG zone bounds
        zone_key = (sig["side"], sig.get("zone_type", ""), round(sig.get("zone_top", 0), 2), round(sig.get("zone_bot", 0), 2))
        self._used_zones.add(zone_key)
        logger.info(f"  Zone used: {zone_key} — no more entries from this zone")

    def new_day(self):
        self.seen.clear()
        self._used_zones.clear()
        # BUG 19 fix: Do NOT clear 5m/15m/dr caches — they contain multi-day
        # lookback data needed by gen_sweep_entries() and get_liquidity_levels().
        # Clearing them killed ALL scans until the next initial_load() (which
        # never happened because it's only called on startup).
        self._last_5m_refresh = 0.0
        self._live_1m = []
        self._current_bar = None
        self._bar_minute = -1
        self._last_signal_side = None
        self._last_signal_ns = 0
        self._first_seen = {}
        self._pending_zones = []
        # Force a REST sync on the next cycle to pick up fresh bars for the new day
        self._last_rest_sync = 0

    def _aggregate_completed_5m(self):
        """Build the just-completed 5m bar from 1m data. Called on 5m boundaries.
        Returns the new bar or None if not enough 1m data."""
        now = datetime.now(CT)
        end_min = (now.minute // 5) * 5
        end_time = now.replace(minute=end_min, second=0, microsecond=0)
        start_time = end_time - timedelta(minutes=5)
        start_ns = int(start_time.timestamp() * 1e9)
        end_ns = int(end_time.timestamp() * 1e9)

        b1 = self._base_1m + self._live_1m
        bars = [b for b in b1 if start_ns <= b["time_ns"] < end_ns]
        if len(bars) < 2:  # need at least 2 of 5 expected 1m bars
            return None

        return {
            "time_ns": start_ns,
            "open": bars[0]["open"],
            "high": max(b["high"] for b in bars),
            "low": min(b["low"] for b in bars),
            "close": bars[-1]["close"],
            "hour": start_time.hour,
            "minute": start_time.minute,
        }

    def _aggregate_completed_15m(self):
        """Build the just-completed 15m bar from the last 3 5m bars in cache."""
        if len(self._b5_cache) < 3:
            return None
        now = datetime.now(CT)
        end_min = (now.minute // 15) * 15
        end_time = now.replace(minute=end_min, second=0, microsecond=0)
        start_time = end_time - timedelta(minutes=15)
        start_ns = int(start_time.timestamp() * 1e9)
        end_ns = int(end_time.timestamp() * 1e9)

        bars = [b for b in self._b5_cache if start_ns <= b["time_ns"] < end_ns]
        if len(bars) < 2:
            return None

        return {
            "time_ns": start_ns,
            "open": bars[0]["open"],
            "high": max(b["high"] for b in bars),
            "low": min(b["low"] for b in bars),
            "close": bars[-1]["close"],
            "hour": start_time.hour,
            "minute": start_time.minute,
        }

    def _fetch_latest_bar(self, tf_minutes):
        """BUG 18 fix: Fetch the latest completed bar from REST API.
        Returns broker's official bar (not WS-built), or None on failure.
        Single API call, takes ~1-2s. Hard 15s timeout to prevent blocking scan loop."""
        import concurrent.futures
        now = datetime.now(CT)
        s_ct = now - timedelta(minutes=tf_minutes * 3)
        UTC = ZoneInfo("UTC")
        try:
            s_utc = s_ct.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
            e_utc = now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
            def _do_fetch():
                return self.api.get_historical_bars(
                    contract_id=self.cid,
                    start_time_iso=s_utc,
                    end_time_iso=e_utc,
                    unit=2, unit_number=tf_minutes, limit=10, live=False,
                )
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                resp = pool.submit(_do_fetch).result(timeout=15)
            if not resp or not resp.bars:
                return None
            b = resp.bars[-1]
            t = b.t if hasattr(b.t, 'astimezone') else datetime.fromisoformat(str(b.t))
            t = t.astimezone(CT)
            ns = int(t.timestamp() * 1e9)
            return {
                "time_ns": ns,
                "open": float(b.o),
                "high": float(b.h),
                "low": float(b.l),
                "close": float(b.c),
                "hour": t.hour,
                "minute": t.minute,
            }
        except Exception as e:
            logger.warning(f"  REST fetch latest {tf_minutes}m bar failed: {e}")
            return None

    def _build_virtual_5m(self):
        """Build a virtual bar for the current incomplete 5m period from live 1m data.
        This lets gen_sweep_entries detect zone touches INSTANTLY on 1m close,
        instead of waiting for the 5m REST refresh."""
        now = datetime.now(CT)
        cur_5m_min = (now.minute // 5) * 5
        cur_5m_start = now.replace(minute=cur_5m_min, second=0, microsecond=0)
        start_ns = int(cur_5m_start.timestamp() * 1e9)

        # Gather completed 1m bars in this 5m window from both REST + live sources
        bars = [b for b in self._base_1m if b["time_ns"] >= start_ns]
        bars += [b for b in self._live_1m if b["time_ns"] >= start_ns]
        if not bars:
            return None

        return {
            "time_ns": start_ns,
            "open": bars[0]["open"],
            "high": max(b["high"] for b in bars),
            "low": min(b["low"] for b in bars),
            "close": bars[-1]["close"],
            "hour": cur_5m_start.hour,
            "minute": cur_5m_min,
        }

    def _fetch(self, tf_minutes, start, end):
        """Fetch bars from REST API and convert to backtest dict format.
        ALL timeframes use chunking to stay under 1000 bar API limit.
        1m: 2-day chunks (~420 bars/day), 5m: 2-day chunks, 15m+: 5-day chunks.
        CRITICAL: API expects UTC timestamps — must convert from CT.
        """
        all_bars = []
        UTC = ZoneInfo("UTC")
        s = datetime.fromisoformat(start).replace(tzinfo=CT)
        e = datetime.fromisoformat(end).replace(tzinfo=CT)

        # Chunk size by timeframe — API supports up to 20,000 bars/request
        if tf_minutes == 1:
            chunk_days = 10   # ~420 bars/day → ~4200/chunk (well under 10k limit)
        elif tf_minutes == 5:
            chunk_days = 10   # ~288 bars/day → ~2880/chunk
        else:
            chunk_days = 10   # 15m = ~96/day → ~960/chunk

        while s < e:
            chunk_end = min(s + timedelta(days=chunk_days), e)
            for attempt in range(3):
                try:
                    # Convert CT → UTC for API (API interprets timestamps as UTC)
                    s_utc = s.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
                    e_utc = chunk_end.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
                    resp = self.api.get_historical_bars(
                        contract_id=self.cid,
                        start_time_iso=s_utc,
                        end_time_iso=e_utc,
                        unit=2, unit_number=tf_minutes, limit=10000, live=False,
                    )
                    if resp and resp.bars:
                        all_bars.extend(resp.bars)
                    break
                except Exception as ex:
                    if attempt == 2:
                        logger.warning(f"Bar fetch ({tf_minutes}m chunk) failed after 3 tries: {ex}")
                    else:
                        time.sleep(3)
            s = chunk_end
            if s < e:
                time.sleep(1)

        # Convert BarData (t, o, h, l, c) → backtest dict format
        seen = set()
        result = []
        for b in all_bars:
            try:
                t = b.t if hasattr(b.t, 'astimezone') else datetime.fromisoformat(str(b.t))
                t = t.astimezone(CT)
                ns = int(t.timestamp() * 1e9)
                if ns in seen:
                    continue
                seen.add(ns)
                result.append({
                    "time_ns": ns,
                    "open": float(b.o),
                    "high": float(b.h),
                    "low": float(b.l),
                    "close": float(b.c),
                    "hour": t.hour,
                    "minute": t.minute,
                })
            except Exception:
                continue
        result.sort(key=lambda x: x["time_ns"])
        return result

    @staticmethod
    def _build_dr(bars):
        """Build date range dict {date: (start_idx, end_idx)} from bar array.
        Uses trading-date semantics: bars at/after 17:00 CT belong to the next
        session. Matches bars_cache.pkl built by build_new_bars.py so that dr5/dr15
        keys are identical between replay and live execution.
        """
        dr = {}
        for i, b in enumerate(bars):
            d = _bar_trading_date(b["time_ns"])
            if d not in dr:
                dr[d] = (i, i + 1)
            else:
                dr[d] = (dr[d][0], i + 1)
        return dr


# ═══════════════════════════════════════════════════════════════
# BOT
# ═══════════════════════════════════════════════════════════════
class PTNUTBot:
    def __init__(self, api_client: APIClient, account_id: int):
        self.api = api_client
        self.account_id = account_id
        self.order_placer = OrderPlacer(api_client, account_id)
        self.scanner = V106Scanner(api_client, NQ["contract_id"])

        # State
        self.position: Optional[Position] = None
        self.daily_pnl = 0.0
        self.trades_today = 0
        self.cl_bull = 0
        self.cl_bear = 0
        self.gc = 0
        self.last_exit_time: Optional[datetime] = None
        self.current_date = ""
        self._last_scan = 0.0

        # Streaming (live price + order fills + bar boundary detection)
        self.live_price: float = 0.0
        self._last_quote_time: float = 0.0     # monotonic time of last quote
        self._quote_count: int = 0             # quotes received this session
        self._exit_fill_price: float = 0.0     # BUG 13 fix: actual fill price from bracket
        self._broker_pnl: Optional[float] = None   # PnL from GatewayUserTrade
        self._broker_fees: Optional[float] = None   # fees from GatewayUserTrade
        self._exit_order_id: Optional[int] = None   # orderId from GatewayUserTrade
        self.best_bid: float = 0.0
        self.best_ask: float = 0.0
        self._can_trade: bool = True                 # from GatewayUserAccount
        self.data_stream: Optional[DataStream] = None
        self.user_stream: Optional[UserHubStream] = None
        self._exit_event = threading.Event()
        self._exit_lock = threading.Lock()
        self._scan_event = threading.Event()   # fires on 1m bar close
        self._is_5m_boundary = False           # True when 5m bar just closed

        # Token refresh
        self._token_time = time.time()
        self._last_reconnect = 0.0  # monotonic time of last stream reconnect
        self._stopping_streams = False  # guard: suppress auto-reconnect during intentional stop

        self._load_state()

    def _load_state(self):
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE) as f:
                    s = json.load(f)
                if s.get("date") == datetime.now(CT).strftime("%Y-%m-%d"):
                    self.daily_pnl = s.get("pnl", 0.0)
                    self.trades_today = s.get("trades", 0)
                    self.cl_bull = s.get("cl_bull", 0)
                    self.cl_bear = s.get("cl_bear", 0)
                    self.gc = s.get("gc", 0)
                    # Restore used zones so restarts don't re-enter same zones
                    uz = s.get("used_zones", [])
                    if uz and hasattr(self, 'scanner') and self.scanner:
                        self.scanner._used_zones = {tuple(z) for z in uz}
                        logger.info(f"  Restored {len(uz)} used zones: {self.scanner._used_zones}")
                    logger.info(f"  Restored state: PnL ${self.daily_pnl:+,.0f} | {self.trades_today} trades")
        except Exception as e:
            logger.warning(f"State load failed: {e}")

    def _save_state(self):
        try:
            now = datetime.now(CT)
            with open(STATE_FILE, "w") as f:
                state = {
                    "date": now.strftime("%Y-%m-%d"),
                    "time": now.strftime("%H:%M:%S"),
                    "pnl": self.daily_pnl,
                    "trades": self.trades_today,
                    "cl_bull": self.cl_bull,
                    "cl_bear": self.cl_bear,
                    "gc": self.gc,
                    "live_price": self.live_price,
                    "mode": "PAPER" if PAPER_MODE else "LIVE",
                    "status": "running",
                    "kz_active": _in_kz(now.hour, now.minute),
                    "quote_count": self._quote_count,
                    "in_position": self.position is not None,
                }
                # Persist used zones for restart dedup
                if hasattr(self, 'scanner') and self.scanner:
                    state["used_zones"] = [list(z) for z in self.scanner._used_zones]
                if self.position:
                    state["position"] = {
                        "side": self.position.side,
                        "entry": self.position.entry_price,
                        "stop": self.position.stop_price,
                        "target": self.position.target_price,
                        "rr": self.position.rr,
                    }
                json.dump(state, f)
        except Exception:
            pass

    def _new_day(self):
        today = datetime.now(CT).strftime("%Y-%m-%d")
        if today != self.current_date:
            # Check for orphan position from previous day
            if self.position:
                logger.warning(f"  Orphan position from {self.current_date} detected on new day")
                try:
                    positions = self.api.search_open_positions(self.account_id)
                    if not positions:
                        logger.info("  Orphan position already closed (bracket filled overnight)")
                        self.position = None
                    else:
                        logger.warning("  Orphan position STILL OPEN — closing now")
                        self._cancel_and_close(NQ["contract_id"])
                        time.sleep(2)
                        self.position = None
                except Exception as e:
                    logger.error(f"  Orphan position check failed: {e}")
                    self.position = None
                self._exit_event.clear()

            self.current_date = today
            self.daily_pnl = 0.0
            self.trades_today = 0
            self.cl_bull = 0
            self.cl_bear = 0
            self.gc = 0
            self.last_exit_time = None
            self.scanner.new_day()
            self._save_state()
            logger.info(f"=== NEW DAY: {today} ===")

    def _refresh_token(self):
        if time.time() - self._token_time < TOKEN_REFRESH_HOURS * 3600:
            return
        try:
            logger.info("Refreshing token...")
            # APIClient handles its own token refresh via _ensure_valid_token()
            new_token = self.api.current_token
            self._token_time = time.time()
            # Use update_token() — documented API way, handles stop/rebuild/restart internally
            # Less data gap than manual stop + start
            if self.data_stream:
                self.data_stream.update_token(new_token)
            if self.user_stream:
                self.user_stream.update_token(new_token)
            logger.info("Token refreshed via update_token()")
        except Exception as e:
            logger.warning(f"Token refresh failed: {e} — falling back to stream restart")
            try:
                self._stop_streams()
                time.sleep(2)
                self._start_streams()
            except Exception:
                pass

    # ── STREAMING (price + fills + real-time 1m bar building) ──
    def _on_quote(self, quote):
        try:
            # Log full quote payload ONCE to discover all available fields
            if not hasattr(self, '_quote_fields_logged'):
                logger.info(f"  Quote payload keys: {list(quote.keys())}")
                logger.info(f"  Quote payload sample: {quote}")
                self._quote_fields_logged = True

            price = quote.get("lastPrice") or quote.get("LastPrice")
            if not price:
                return
            self.live_price = float(price)
            # Track bid/ask for spread awareness
            bid = quote.get("bestBid") or quote.get("BestBid")
            ask = quote.get("bestAsk") or quote.get("BestAsk")
            if bid: self.best_bid = float(bid)
            if ask: self.best_ask = float(ask)
            self._last_quote_time = time.monotonic()
            self._quote_count += 1
            if self._quote_count <= 10 or self._quote_count % 50 == 0:
                logger.info(f"  NQ tick #{self._quote_count}: {self.live_price}")

            # Use server timestamp from quote if available, else fall back to local clock.
            # Server timestamp matches what the broker uses for REST bar aggregation,
            # fixing the WS vs REST bar OHLC mismatch at minute boundaries.
            now = None
            # lastUpdated = real-time server timestamp (when quote was received)
            # timestamp = stale session open time — NOT useful for bar building
            ts = quote.get("lastUpdated") or quote.get("LastUpdated")
            if ts:
                try:
                    if isinstance(ts, str):
                        now = datetime.fromisoformat(ts.rstrip("Z")).replace(tzinfo=ZoneInfo("UTC")).astimezone(CT)
                    elif isinstance(ts, (int, float)):
                        # Could be epoch seconds or milliseconds
                        if ts > 1e12:
                            now = datetime.fromtimestamp(ts / 1000, tz=CT)
                        else:
                            now = datetime.fromtimestamp(ts, tz=CT)
                except Exception:
                    pass
            if now is None:
                now = datetime.now(CT)

            # Update dashboard state every ~5 seconds
            if self._quote_count % 50 == 0:
                self._save_state()

            # Bar building moved to _on_market_trade — trades match broker bars exactly
            # _on_quote only updates live_price for drift checks
        except Exception as e:
            # BUG 12 fix: log errors instead of swallowing — silent failures
            # here kill bar building and the bot stops detecting signals
            logger.error(f"  _on_quote error: {e}")

    def _on_market_trade(self, trade):
        """GatewayTrade — actual tape executions with server timestamps.
        Fields per API docs: symbolId, price, timestamp, type, volume.
        Updates live_price only — bar building stays with _on_quote to avoid
        duplicate 1m bar closes (both fire at minute boundaries)."""
        try:
            if not hasattr(self, '_market_trade_logged'):
                logger.info(f"  MarketTrade payload: {trade}")
                self._market_trade_logged = True

            price = trade.get("price") or trade.get("Price")
            if not price:
                return
            px = float(price)
            self.live_price = px
            self._last_quote_time = time.monotonic()

            # Build bars from TRADES (not quotes) — matches broker bar building
            ts = trade.get("timestamp") or trade.get("Timestamp")
            now = None
            if ts:
                try:
                    if isinstance(ts, str):
                        now = datetime.fromisoformat(ts.rstrip("Z")).replace(tzinfo=ZoneInfo("UTC")).astimezone(CT)
                except Exception:
                    pass
            if now is None:
                now = datetime.now(CT)

            bar_closed = self.scanner.on_tick(px, now)
            if bar_closed and _in_kz(now.hour, now.minute) and not self.position:
                if now.minute % 5 == 0:
                    self._is_5m_boundary = True
                self._scan_event.set()
        except Exception as e:
            logger.error(f"  _on_market_trade error: {e}")

    def _on_order_update(self, order_data):
        try:
            if not self.position:
                return
            status = order_data.get("status") or order_data.get("Status")
            oid = order_data.get("id") or order_data.get("Id")
            side = order_data.get("side") or order_data.get("Side")

            if status == ORDER_STATUS_FILLED and oid != self.position.entry_order_id:
                # Any fill that ISN'T our entry = bracket stop or target hit
                # BUG 13 fix: store actual fill price for accurate PnL
                # With bracket orders, stop/target IDs are server-generated
                # so we detect exit by seeing a fill on an order we didn't place as entry
                fill_px = order_data.get("filledPrice") or order_data.get("FilledPrice") or 0
                if fill_px:
                    self._exit_fill_price = float(fill_px)
                logger.info(f"    Exit fill detected: order {oid} filled @ {fill_px} (status={status})")
                with self._exit_lock:
                    self._exit_event.set()
        except Exception:
            pass

    def _on_account_update(self, acct_data):
        """GatewayUserAccount — tracks canTrade status from broker."""
        try:
            if not hasattr(self, '_acct_fields_logged'):
                logger.info(f"    Account payload: {acct_data}")
                self._acct_fields_logged = True
            ct = acct_data.get("canTrade") or acct_data.get("CanTrade")
            if ct is not None:
                was = self._can_trade
                self._can_trade = bool(ct)
                if was and not self._can_trade:
                    logger.warning("  Account canTrade → FALSE — trading disabled by broker")
                elif not was and self._can_trade:
                    logger.info("  Account canTrade → TRUE — trading re-enabled")
        except Exception as e:
            logger.error(f"  _on_account_update error: {e}")

    def _on_position_update(self, pos_data):
        """Backup exit detection — position goes flat = trade is over."""
        try:
            if not self.position:
                return
            size = pos_data.get("size") or pos_data.get("Size") or 0
            if size == 0:
                logger.info("    Position went flat (size=0) — exit detected via position update")
                with self._exit_lock:
                    self._exit_event.set()
        except Exception:
            pass

    def _on_user_trade(self, trade_data):
        """GatewayUserTrade — actual fill confirmations with exact prices.
        Fields per API docs: id, accountId, contractId, creationTimestamp,
        price, profitAndLoss, fees, side, size, voided, orderId"""
        try:
            # Log first trade payload to discover field names
            if not hasattr(self, '_trade_fields_logged'):
                logger.info(f"    UserTrade payload: {trade_data}")
                self._trade_fields_logged = True

            # Skip voided trades
            voided = trade_data.get("voided") or trade_data.get("Voided")
            if voided:
                logger.info(f"    UserTrade VOIDED — ignoring")
                return

            fill_px = trade_data.get("price") or trade_data.get("Price") or 0
            fill_size = trade_data.get("size") or trade_data.get("Size") or 0
            order_id = trade_data.get("orderId") or trade_data.get("OrderId")
            broker_pnl = trade_data.get("profitAndLoss") or trade_data.get("ProfitAndLoss")
            broker_fees = trade_data.get("fees") or trade_data.get("Fees")

            if fill_px and float(fill_px) > 0:
                self._exit_fill_price = float(fill_px)
                # Store broker PnL and fees for accurate reporting
                if broker_pnl is not None:
                    self._broker_pnl = float(broker_pnl)
                if broker_fees is not None:
                    self._broker_fees = float(broker_fees)
                if order_id is not None:
                    self._exit_order_id = int(order_id)
                logger.info(f"    UserTrade fill: {fill_px} x {fill_size} | orderId={order_id} | pnl={broker_pnl} fees={broker_fees}")
        except Exception as e:
            logger.error(f"  _on_user_trade error: {e}")

    def _on_data_stream_state(self, state_str: str):
        """Callback for DataStream state changes — triggers reconnect on death."""
        logger.info(f"  DataStream state: {state_str}")
        if state_str in ("DISCONNECTED", "ERROR") and not self._stopping_streams:
            elapsed = time.monotonic() - self._last_quote_time if self._last_quote_time > 0 else 0
            logger.warning(f"  DataStream died ({state_str}), last quote {elapsed:.0f}s ago")
            # Schedule async reconnect to avoid blocking signalrcore's callback thread
            threading.Thread(target=self._auto_reconnect_streams, args=("DataStream",), daemon=True).start()

    def _on_data_stream_error(self, error):
        """Callback for DataStream errors."""
        logger.error(f"  DataStream error: {error}")

    def _on_user_stream_state(self, state_str: str):
        """Callback for UserHubStream state changes."""
        logger.info(f"  UserHubStream state: {state_str}")
        if state_str in ("DISCONNECTED", "ERROR") and not self._stopping_streams:
            logger.warning(f"  UserHubStream died ({state_str})")
            threading.Thread(target=self._auto_reconnect_streams, args=("UserHubStream",), daemon=True).start()

    def _on_user_stream_error(self, error):
        """Callback for UserHubStream errors."""
        logger.error(f"  UserHubStream error: {error}")

    def _auto_reconnect_streams(self, source: str):
        """Auto-reconnect with backoff — called from state change callbacks.
        Rebuilds both streams with a fresh token to avoid stale-token reconnects."""
        # Backoff: don't reconnect more than once per 30s
        if time.monotonic() - self._last_reconnect < 30:
            logger.info(f"  {source}: reconnect skipped (backoff, last attempt {time.monotonic() - self._last_reconnect:.0f}s ago)")
            return
        self._last_reconnect = time.monotonic()
        logger.warning(f"  {source}: auto-reconnecting streams...")
        try:
            tg(f"PTNUT: {source} died, auto-reconnecting...")
            self._stop_streams()
            time.sleep(2)
            self._start_streams()
            self._last_quote_time = time.monotonic()
            logger.info(f"  {source}: auto-reconnect complete")
        except Exception as e:
            logger.error(f"  {source}: auto-reconnect failed: {e}")

    def _start_streams(self):
        self._stopping_streams = False
        cid = NQ["contract_id"]
        token = self.api.current_token
        self.data_stream = MarketStream(
            token=token,
            contract_id=cid,
            on_quote=self._on_quote,
            on_trade=self._on_market_trade,
        )
        self.user_stream = UserStream(
            token=token,
            account_id=self.account_id,
            on_order=self._on_order_update,
            on_position=self._on_position_update,
            on_trade=self._on_user_trade,
        )
        try:
            self.data_stream.start()
            self._last_quote_time = time.monotonic()
            logger.info("  NQ MarketStream started (pysignalr)")
        except Exception as e:
            logger.error(f"  MarketStream failed: {e}")
        try:
            self.user_stream.start()
            logger.info("  UserStream started (pysignalr)")
        except Exception as e:
            logger.error(f"  UserStream failed: {e}")

    def _stop_streams(self):
        self._stopping_streams = True  # suppress auto-reconnect from state callbacks
        try:
            if self.data_stream:
                self.data_stream.stop()
        except Exception as e:
            logger.warning(f"  DataStream stop error: {e}")
        try:
            if self.user_stream:
                self.user_stream.stop()
        except Exception as e:
            logger.warning(f"  UserHubStream stop error: {e}")
        self.data_stream = None
        self.user_stream = None

    # ── TRADE LOG ──
    def _log_trade(self, record):
        """Append a trade record to ptnut_trades.json for the dashboard."""
        path = "ptnut_trades.json"
        try:
            trades = []
            if os.path.exists(path):
                with open(path) as f:
                    trades = json.load(f)
            trades.append(record)
            with open(path, "w") as f:
                json.dump(trades, f)
        except Exception:
            pass

    # ── TRADE EXECUTION ──
    def enter_trade(self, sig):
        cid = NQ["contract_id"]
        side_str = "BUY" if sig["side"] == "bull" else "SELL"
        exit_str = "SELL" if sig["side"] == "bull" else "BUY"

        nct = sig["contracts"]
        logger.info(f">>> {'[PAPER] ' if PAPER_MODE else ''}ENTRY: {sig['side'].upper()} @ {sig['entry']:.2f} ({nct}ct)")
        logger.info(f"    SL: {sig['stop']:.2f} | TP: {sig['target']:.2f} | "
                     f"Risk: {sig['risk']:.1f}pts (${sig['risk']*20*nct:.0f}) | RR: {sig['rr']} | Score: {sig['score']}")

        # Stale check already done in main loop — removed duplicate here (BUG 14)
        sig_age = (datetime.now(CT) - sig["time"]).total_seconds()

        # ── BROKER TRADING GATE ──
        if not self._can_trade:
            logger.warning(f"    SKIP: account canTrade=False (broker disabled trading)")
            return

        # ── REQUIRE LIVE PRICE ──
        # Without live price we're entering blind — never do this
        if not self.live_price or self.live_price <= 0:
            logger.warning(f"    SKIP: no live price (WebSocket not connected?)")
            return

        # ── PRICE VALIDATION ──
        # Backtest enters at 1m close + 0.5 slip. We execute seconds later.
        # Max 1pt drift — anything more means we're chasing.
        MAX_DRIFT = 5.0  # NQ moves fast — 1pt was killing valid entries
        drift = abs(self.live_price - sig["entry"])
        if drift > MAX_DRIFT:
            logger.warning(f"    SKIP: price {self.live_price:.2f} drifted {drift:.1f}pts from entry {sig['entry']:.2f} (max {MAX_DRIFT})")
            self.scanner.mark_executed(sig)
            return

        # Check price hasn't blown through stop
        if sig["side"] == "bull" and self.live_price <= sig["stop"]:
            logger.warning(f"    SKIP: price {self.live_price:.2f} already at/below stop {sig['stop']:.2f}")
            self.scanner.mark_executed(sig)
            return
        if sig["side"] == "bear" and self.live_price >= sig["stop"]:
            logger.warning(f"    SKIP: price {self.live_price:.2f} already at/above stop {sig['stop']:.2f}")
            self.scanner.mark_executed(sig)
            return

        # Check price hasn't passed target (trade already over)
        if sig["side"] == "bull" and self.live_price > sig["target"]:
            logger.warning(f"    SKIP: price {self.live_price:.2f} past target {sig['target']:.2f}")
            self.scanner.mark_executed(sig)
            return
        if sig["side"] == "bear" and self.live_price < sig["target"]:
            logger.warning(f"    SKIP: price {self.live_price:.2f} past target {sig['target']:.2f}")
            self.scanner.mark_executed(sig)
            return

        if PAPER_MODE:
            tg(f"<b>PTNUT [PAPER] — NQ {sig['side'].upper()} ({nct}ct)</b>\n"
               f"Entry: {sig['entry']:.2f} | SL: {sig['stop']:.2f} | TP: {sig['target']:.2f}\n"
               f"Risk: ${sig['risk']*20*nct:.0f} | RR: {sig['rr']} | Score: {sig['score']}\n"
               f"Zone: {sig['zone_type']} | Signal time: {sig['time'].strftime('%H:%M')}\n"
               f"Live: {self.live_price:.2f} | Drift: {drift:.1f}pts | Age: {sig_age:.0f}s")
            self.scanner.mark_executed(sig)
            return

        # ── SEPARATE ORDERS: Market entry, then SL + TP after fill ──
        # TopstepX rejects bracket orders when Position Brackets is configured.
        # Instead: place market, wait for fill, then place stop + limit separately.
        tick_size = NQ["tick_size"]
        risk_pts = sig["risk"]
        reward_pts = sig["risk"] * sig["rr"]
        d_dir = 1 if sig["side"] == "bull" else -1

        logger.info(f"    Live: {self.live_price:.2f} | Drift: {drift:.1f}pts | Age: {sig_age:.0f}s")
        logger.info(f"    SL: {risk_pts:.1f}pts | TP: {reward_pts:.1f}pts")

        # 1. Market entry (no brackets)
        oid = self.order_placer.place_market_order(
            side=side_str, size=sig["contracts"], contract_id=cid,
        )
        if oid is None:
            logger.error("    ENTRY FAILED")
            return

        # Wait for fill confirmation, then get actual fill price
        time.sleep(2)

        entry_price = sig["entry"]
        try:
            positions = self.api.search_open_positions(self.account_id)
            for p in positions:
                if p.contract_id == cid and p.average_price:
                    entry_price = float(p.average_price)
                    break
        except Exception:
            pass

        # Log execution quality
        fill_drift = abs(entry_price - sig["entry"])
        logger.info(f"    Fill: {entry_price:.2f} (signal: {sig['entry']:.2f}, drift: {fill_drift:.2f}pts)")

        # Calculate stop/target from ACTUAL fill price
        stop_px = tick_round(entry_price - risk_pts * d_dir)
        target_px = tick_round(entry_price + reward_pts * d_dir)

        # Reject if fill drift ate more than 50% of the expected reward
        if fill_drift > reward_pts * 0.5:
            logger.error(f"    Fill drifted {fill_drift:.2f}pts (>{reward_pts*0.5:.1f} = 50% of reward) — flattening")
            self._cancel_and_close(cid, exit_str)
            return

        # 2. Place SL and TP as separate orders
        # SL: stop order on the opposite side
        # TP: limit order on the opposite side
        sl_oid = self.order_placer.place_stop_market_order(
            side=exit_str, size=sig["contracts"], stop_price=stop_px,
            contract_id=cid,
        )
        tp_oid = self.order_placer.place_limit_order(
            side=exit_str, size=sig["contracts"], limit_price=target_px,
            contract_id=cid,
        )
        logger.info(f"    SL order: {sl_oid} @ {stop_px:.2f} | TP order: {tp_oid} @ {target_px:.2f}")
        if not sl_oid:
            logger.error("    SL order FAILED — flattening")
            self._cancel_and_close(cid, exit_str)
            return

        self._exit_event.clear()
        self._exit_fill_price = 0.0  # reset for new trade
        self._broker_pnl = None
        self._broker_fees = None
        self._exit_order_id = None
        self.position = Position(
            side=sig["side"], entry_price=entry_price,
            stop_price=stop_px, target_price=target_px,
            risk=sig["risk"], rr=sig["rr"], score=sig["score"],
            zone_type=sig["zone_type"], contracts=sig["contracts"],
            entry_time=datetime.now(CT),
            entry_order_id=oid, stop_order_id=sl_oid or 0, target_order_id=tp_oid or 0,
        )
        self.trades_today += 1
        self.scanner.mark_executed(sig)
        self._save_state()
        self._log_trade({
            "type": "ENTRY", "date": datetime.now(CT).strftime("%Y-%m-%d"),
            "time": datetime.now(CT).strftime("%H:%M:%S"),
            "side": sig["side"], "entry": entry_price, "stop": stop_px,
            "target": target_px, "risk": sig["risk"], "rr": sig["rr"],
            "score": sig["score"], "zone_type": sig["zone_type"],
            "contracts": sig["contracts"],
        })

        risk_dollar = risk_pts * NQ["pv"] * sig["contracts"]
        reward_dollar = reward_pts * NQ["pv"] * sig["contracts"]
        tg(f"<b>PTNUT ENTRY — NQ {sig['side'].upper()}</b>\n"
           f"Entry: {entry_price:.2f} | SL: {stop_px:.2f} | TP: {target_px:.2f}\n"
           f"Risk: ${risk_dollar:.0f} | Reward: ${reward_dollar:.0f} ({sig['rr']}R)\n"
           f"Zone: {sig['zone_type']} | Score: {sig['score']}\n"
           f"Trade #{self.trades_today} | Day PnL: ${self.daily_pnl:+,.0f}")

    def _cancel_and_close(self, cid, close_side=None):
        """Cancel all open orders and flatten position — ONLY if actually in a position."""
        try:
            open_orders = self.api.search_open_orders(self.account_id)
            for o in open_orders:
                try:
                    self.order_placer.cancel_order(o.id)
                except Exception:
                    pass
        except Exception:
            pass
        if close_side is None:
            if self.position:
                close_side = "SELL" if self.position.side == "bull" else "BUY"
            else:
                return
        # CRITICAL: Verify we actually have an open position before sending close order.
        # Without this, a bracket that already filled would cause us to OPEN a naked position.
        try:
            positions = self.api.search_open_positions(self.account_id)
            if not positions:
                logger.info("    No open position on broker — skip close order (bracket already filled)")
                return
            # Use actual position size from broker, not assumed contracts
            close_size = abs(positions[0].size) if hasattr(positions[0], 'size') and positions[0].size else (self.position.contracts if self.position else NQ["contracts"])
        except Exception as e:
            logger.warning(f"    Position check failed ({e}) — skip close to be safe")
            return
        try:
            self.order_placer.place_market_order(
                side=close_side, size=close_size, contract_id=cid)
        except Exception:
            pass

    def check_position(self):
        if not self.position:
            return
        elapsed = (datetime.now(CT) - self.position.entry_time).total_seconds() / 60
        if elapsed >= TIMEOUT:
            logger.info(f"    TIMEOUT after {elapsed:.0f}min — closing")
            with self._exit_lock:
                self._cancel_and_close(NQ["contract_id"])
                time.sleep(2)
                self._handle_exit("timeout")

    def _handle_exit(self, reason="fill"):
        if not self.position:
            return
        p = self.position

        # Cancel remaining bracket order (the one that didn't fill)
        # Use searchOpen endpoint — returns only open orders, no timestamp needed
        try:
            open_orders = self.api.search_open_orders(self.account_id)
            for o in open_orders:
                try:
                    self.order_placer.cancel_order(o.id)
                    logger.info(f"    Cancelled remaining order {o.id}")
                except Exception:
                    pass
        except Exception:
            pass

        pnl = 0.0
        exit_px = self.live_price  # default fallback
        try:
            positions = self.api.search_open_positions(self.account_id)
            if not positions:
                # Position is flat — bracket filled.
                exit_px = self._exit_fill_price if self._exit_fill_price > 0 else self.live_price
                # Prefer broker-reported PnL (includes exact fees) over manual calc
                if self._broker_pnl is not None:
                    pnl = self._broker_pnl
                    fees = self._broker_fees if self._broker_fees is not None else 0
                    src = "broker"
                    logger.info(f"    Exit price: {exit_px:.2f} | PnL: ${pnl:.2f} (broker) | fees: ${fees:.2f}")
                else:
                    # Fallback: manual calculation
                    if p.side == "bull":
                        pnl = (exit_px - p.entry_price) * NQ["pv"] * p.contracts
                    else:
                        pnl = (p.entry_price - exit_px) * NQ["pv"] * p.contracts
                    pnl -= 4.50 * p.contracts
                    src = "fill" if self._exit_fill_price > 0 else "estimate"
                    logger.info(f"    Exit price: {exit_px:.2f} ({src})")
                # Log exit reason if we know which order filled
                if self._exit_order_id and self.position:
                    if self._exit_order_id == self.position.stop_order_id:
                        logger.info(f"    Exit reason: STOP LOSS (order {self._exit_order_id})")
                    elif self._exit_order_id == self.position.target_order_id:
                        logger.info(f"    Exit reason: TAKE PROFIT (order {self._exit_order_id})")
            else:
                logger.warning(f"    Still in position after exit signal — {len(positions)} open")
        except Exception:
            pass

        self.daily_pnl += pnl
        result = "WIN" if pnl > 0 else "LOSS"

        if pnl < 0:
            if p.side == "bull":
                self.cl_bull += 1
            else:
                self.cl_bear += 1
            self.gc += 1
        else:
            if p.side == "bull":
                self.cl_bull = 0
            else:
                self.cl_bear = 0
            self.gc = 0

        self.last_exit_time = datetime.now(CT)
        self._save_state()
        self._log_trade({
            "type": "EXIT", "date": datetime.now(CT).strftime("%Y-%m-%d"),
            "time": datetime.now(CT).strftime("%H:%M:%S"),
            "side": p.side, "entry": p.entry_price, "exit": exit_px,
            "pnl": pnl, "rr": p.rr, "score": p.score,
            "zone_type": p.zone_type, "contracts": p.contracts,
            "result": result,
        })

        logger.info(f"    EXIT: {result} ${pnl:+,.0f} ({reason}) | Day: ${self.daily_pnl:+,.0f}")
        tg(f"<b>PTNUT {result}</b> ${pnl:+,.0f}\n"
           f"{'LONG' if p.side=='bull' else 'SHORT'} | {p.zone_type} | Score {p.score} | {p.rr}R\n"
           f"Day PnL: ${self.daily_pnl:+,.0f} | Trades: {self.trades_today}")

        self.position = None

    # ── MAIN LOOP ──
    def run(self):
        logger.info("=" * 60)
        logger.info("PTNUT TRADING BOT — V106 Sweep > Disp > FVG Zone")
        logger.info("Signal engine: EXACT backtest functions (v106_dynamic_rr.py)")
        logger.info(f"MODE: {'PAPER (alerts only)' if PAPER_MODE else 'LIVE TRADING'}")
        logger.info(f"NQ only | 3ct | 4T-v3 RR: bare=1.3 cisd=1.5 struct=1.7 sweep=2.0 | MR $1000")
        logger.info(f"MCL {MCL}/side | GMCL {GMCL} | DLL ${DLL}")
        logger.info(f"KZ {KZ_START[0]}:{KZ_START[1]:02d}-{KZ_END[0]}:{KZ_END[1]:02d} CT")
        logger.info(f"Scan: INSTANT on 1m close | HTF: WS-built 5m/15m bars (instant, REST fallback)")
        logger.info("=" * 60)

        # Resolve contract ID at runtime (not import time)
        if not resolve_contract(self.api):
            logger.error("Contract resolution failed — aborting")
            tg("PTNUT: Contract resolution FAILED. Bot not starting.")
            return
        # Update scanner with resolved contract
        self.scanner.cid = NQ["contract_id"]
        logger.info(f"Contract: {NQ['contract_id']}")

        # Validate REST bars before doing anything
        if not self.scanner.validate_startup():
            logger.error("Bar validation failed — aborting")
            tg("PTNUT: Bar validation FAILED. Bot not starting.")
            return

        # Pre-load bars: 5m/15m cache + overnight 1m history (blocking on startup only)
        logger.info("Loading initial bar data...")
        if not self.scanner.initial_load():
            logger.error("Failed to load initial bars — aborting")
            return

        # Diagnostic: show what dates/sessions the bot has data for
        dr5 = self.scanner._dr5_cache
        today = _bar_trading_date(int(datetime.now(CT).timestamp() * 1e9))
        dates_str = ", ".join(str(d) for d in sorted(dr5.keys())[-5:])
        has_today = today in dr5
        logger.info(f"  Data coverage: last 5 dates in cache = [{dates_str}]")
        logger.info(f"  Today ({today}) in cache: {has_today}")
        if has_today:
            ds, de = dr5[today]
            first_h = self.scanner._b5_cache[ds]["hour"]
            logger.info(f"  Today: {de - ds} 5m bars, first bar at {first_h}:00 CT")
        b1 = self.scanner._base_1m + self.scanner._live_1m
        logger.info(f"  1m bars: {len(b1)} total (base={len(self.scanner._base_1m)} live={len(self.scanner._live_1m)})")
        if b1:
            first_1m = datetime.fromtimestamp(b1[0]["time_ns"] / 1e9, tz=CT)
            last_1m = datetime.fromtimestamp(b1[-1]["time_ns"] / 1e9, tz=CT)
            logger.info(f"  1m range: {first_1m.strftime('%m/%d %H:%M')} → {last_1m.strftime('%m/%d %H:%M')} CT")

        # BUG 19 fix: Set current_date BEFORE main loop so _new_day() doesn't
        # trigger on the first iteration and wipe the data we just loaded.
        self.current_date = datetime.now(CT).strftime("%Y-%m-%d")

        self._start_streams()

        # Check for existing positions
        try:
            positions = self.api.search_open_positions(self.account_id)
            if positions:
                logger.warning("Open position on startup — waiting for flat (max 5min)")
                tg("PTNUT: Open position detected, waiting max 5min...")
                for _ in range(20):  # 20 * 15s = 5 min max
                    time.sleep(15)
                    if not self.api.search_open_positions(self.account_id):
                        break
                else:
                    logger.warning("Startup position still open after 5min — proceeding anyway")
                    tg("PTNUT: Position still open after 5min, proceeding")
                logger.info("Flat or timeout — resuming")
        except Exception:
            pass

        while True:
            try:
                self._new_day()
                self._refresh_token()
                now = datetime.now(CT)

                # Position management — MUST be first, before any sleep/skip
                # If we have a position, manage it regardless of time/day
                if self.position:
                    # Last-resort heartbeat: if auto-reconnect (via state callbacks)
                    # hasn't restored data flow, do a hard rebuild.
                    # The 30s threshold is generous — auto-reconnect should fix it in <10s.
                    if False:  # pysignalr handles reconnection — disabled
                        logger.warning(f"  No quote — hard reconnect disabled (pysignalr handles it)")
                        tg("PTNUT: Stream dead during trade, hard reconnecting...")
                        self._stop_streams()
                        time.sleep(2)
                        self._start_streams()
                        self._last_quote_time = time.monotonic()
                        self._last_reconnect = time.monotonic()
                    self.check_position()
                    if self._exit_event.wait(timeout=10):
                        with self._exit_lock:
                            self._handle_exit()
                        self._exit_event.clear()
                    continue

                # pysignalr handles its own reconnection — no heartbeat needed
                # Low-volume periods (16:00-17:00 CT) can go 5+ min with no ticks
                # DO NOT kill the stream just because no quotes arrived

                # Holiday skip
                if (now.month == 12 and now.day >= 26) or (now.month == 1 and now.day <= 2):
                    time.sleep(60); continue

                # Outside hours — use KZ_START/KZ_END (handles overnight wrap)
                if now.weekday() > 4:
                    time.sleep(30); continue
                if not _in_kz(now.hour, now.minute):
                    time.sleep(30); continue

                # Risk limits
                if self.daily_pnl <= DLL:
                    if now.minute == 0 and now.second < 15:
                        logger.info(f"DLL hit: ${self.daily_pnl:+,.0f}")
                    time.sleep(60); continue

                if self.gc >= GMCL:
                    if now.minute == 0 and now.second < 15:
                        logger.info(f"GMCL hit: {self.gc}")
                    time.sleep(60); continue

                # Bug 1+4 fix: REST sync in BACKGROUND thread — never blocks scan
                if (_in_kz(now.hour, now.minute)
                        and self.scanner.needs_rest_sync()
                        and not getattr(self, '_rest_thread_active', False)):
                    self._rest_thread_active = True
                    def _bg_rest():
                        try:
                            self.scanner.background_rest_sync()
                        finally:
                            self._rest_thread_active = False
                            # Bug 2 fix: force immediate scan after REST completes
                            self._scan_event.set()
                    threading.Thread(target=_bg_rest, daemon=True).start()
                    logger.info("  Background REST sync launched")

                # Signal scanning — triggered by 1m bar close (from WebSocket)
                # 5m boundaries fetch latest bar from REST (~1-2s, BUG 18 fix)
                should_scan = False
                is_5m = False
                if self._scan_event.is_set():
                    self._scan_event.clear()
                    should_scan = True
                    is_5m = self._is_5m_boundary
                    self._is_5m_boundary = False
                    if is_5m:
                        logger.info(f"  5m bar closed at {now.strftime('%H:%M')} — full scan")
                    if self.scanner._b5_cache:
                        lb5 = self.scanner._b5_cache[-1]
                        logger.info(f"  Built 5m: {lb5['hour']:02d}:{lb5['minute']:02d} O={lb5['open']:.2f} H={lb5['high']:.2f} L={lb5['low']:.2f} C={lb5['close']:.2f}")
                    else:
                        logger.info(f"  1m bar closed at {now.strftime('%H:%M')} — quick scan")
                    # Log last built bar for verification
                    if self.scanner._live_1m:
                        lb = self.scanner._live_1m[-1]
                        logger.info(f"  Built bar: {lb['hour']:02d}:{lb['minute']:02d} O={lb['open']:.2f} H={lb['high']:.2f} L={lb['low']:.2f} C={lb['close']:.2f}")
                elif _in_kz(now.hour, now.minute) and time.time() - self._last_scan >= SCAN_INTERVAL:
                    should_scan = True  # fallback: scan every 60s if WebSocket events missed
                    is_5m = (time.time() - self.scanner._last_5m_refresh > 360)  # only refresh if stale

                if should_scan:
                    self._last_scan = time.time()
                    try:
                        signals = self.scanner.scan(is_5m_boundary=is_5m)
                        if signals:
                            logger.info(f"  Scan: {len(signals)} new signal(s)")
                        scan_now = datetime.now(CT)  # fresh time after scan completes
                        for sig in signals:
                            # Stale = time since FIRST DISCOVERY, not since 1m bar close.
                            # The signal only becomes actionable when scan() finds it (after 5m bar
                            # closes). Measuring from 1m close killed valid signals — e.g. 10:03 touch
                            # found at 10:05 = 120s "stale" even though just discovered 0s ago.
                            # Drift check handles price validity separately.
                            sig_age = (scan_now - sig["_discovered_at"]).total_seconds()
                            if sig_age > 120:
                                logger.warning(f"  STALE signal skipped: {sig['side']} {sig['zone_type']} @ {sig['entry']:.2f} ({sig_age:.0f}s old, limit 120s)")
                                self.scanner.mark_executed(sig)
                                continue
                            cl = self.cl_bull if sig["side"] == "bull" else self.cl_bear
                            if cl >= MCL:
                                logger.info(f"  MCL hit for {sig['side']}")
                                continue
                            if self.last_exit_time and (scan_now - self.last_exit_time).total_seconds() < COOLDOWN:
                                logger.info("  Cooldown active")
                                continue
                            self.enter_trade(sig)
                            break  # one trade at a time
                    except Exception as e:
                        logger.error(f"  Scan error: {e}", exc_info=True)

                # BUG 10 fix: wait for next event but DON'T clear — let the top of
                # the loop check is_set() and process it. Old code cleared events that
                # arrived during scan execution, losing up to 59s of signal detection.
                self._scan_event.wait(timeout=10)

            except KeyboardInterrupt:
                logger.info("Shutdown")
                if self.position:
                    self._cancel_and_close(NQ["contract_id"])
                break
            except Exception as e:
                logger.error(f"Error: {e}", exc_info=True)
                time.sleep(30)

        self._stop_streams()
        tg(f"PTNUT stopped | Day PnL: ${self.daily_pnl:+,.0f}")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    # Log to both console AND dated file (never lose logs on restart)
    log_fmt = logging.Formatter('%(asctime)s | %(name)-6s | %(message)s', datefmt='%H:%M:%S')
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Console handler
    console_h = logging.StreamHandler()
    console_h.setFormatter(log_fmt)
    root.addHandler(console_h)

    # File handler — appends to dated log file
    log_date = datetime.now(CT).strftime("%Y%m%d")
    file_h = logging.FileHandler(f"ptnut_bot_{log_date}.log", mode="a")
    file_h.setFormatter(log_fmt)
    root.addHandler(file_h)

    logging.getLogger("tsxapipy").setLevel(logging.WARNING)
    logging.getLogger("signalrcore").setLevel(logging.WARNING)
    logging.getLogger("SignalRCoreClient").setLevel(logging.ERROR)

    logger.info("Authenticating with TopstepX...")
    token, token_time = authenticate()
    if not token:
        logger.error("Auth failed!")
        sys.exit(1)

    api = APIClient(initial_token=token, token_acquired_at=token_time)

    # Contract resolved in run() via resolve_contract() — just set fallback if still None
    if NQ["contract_id"] is None:
        NQ["contract_id"] = NQ_CONTRACT_FALLBACK
        logger.info(f"Using fallback contract: {NQ_CONTRACT_FALLBACK}")

    accounts = api.get_accounts()
    if not accounts:
        logger.error("No accounts!")
        sys.exit(1)

    acct = next((a for a in accounts if "PRAC" in (a.name or "")), accounts[0])
    logger.info(f"Account: {acct.id} ({acct.name})")

    mode = "PAPER" if PAPER_MODE else "LIVE"
    tg(f"<b>PTNUT Bot Started [{mode}]</b>\n"
       f"Account: {acct.name}\n"
       f"NQ 3ct | V106 4T-v3 Confluence RR\n"
       f"82.2% WR | MCL {MCL} | DLL ${DLL}")

    bot = PTNUTBot(api, acct.id)
    bot.run()


if __name__ == "__main__":
    main()
