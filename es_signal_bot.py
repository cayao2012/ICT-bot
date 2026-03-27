"""
ES Signal Bot — V106 Sweep > Displacement > FVG Zone (Alerts Only)
==================================================================
ES 1ct | 4T-v3 Confluence RR | Telegram alerts, NO orders
Same V106 engine as NQ bot. Sends signal to Telegram, you execute manually.
"""

import os
import sys
import time
import logging
import threading
import requests as _requests
from datetime import datetime, timedelta, date as dt_date
from zoneinfo import ZoneInfo

from tsxapipy import authenticate, APIClient, DataStream

from v106_dynamic_rr import (
    gen_sweep_entries,
    get_liquidity_levels,
    detect_sweep_at,
    cisd_5m,
    structure_15m,
    sweep_15m,
)

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
CT = ZoneInfo("America/Chicago")

TELEGRAM_BOT_TOKEN = "8482411404:AAGDE6EkrgEPTlkGdO-EkzdNicEqjkzJ3IU"
TELEGRAM_CHAT_ID = "8203680695"

ES_CONTRACT_FALLBACK = "CON.F.US.EP.M26"   # June 2026 (current front month)
ES_CONTRACT_PREV     = "CON.F.US.EP.H26"   # Mar 2026 (prior front month — history backfill)
ES_CONTRACT = None

ES = {
    "contract_id": ES_CONTRACT,
    "pv": 50, "contracts": 2, "slip": 0.25, "tick_size": 0.25,
}

MAX_RISK = 1000
MCL = 3
GMCL = 5
KZ_START = (7, 30)
KZ_END = (14, 30)
SCAN_INTERVAL = 60
TOKEN_REFRESH_HOURS = 4

logger = logging.getLogger("ES_SIG")


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
# HELPERS
# ═══════════════════════════════════════════════════════════════
def tick_round(price, tick=0.25):
    return round(round(price / tick) * tick, 2)


def _in_kz(h, m):
    t = h * 60 + m
    return KZ_START[0] * 60 + KZ_START[1] <= t <= KZ_END[0] * 60 + KZ_END[1]


def _to_utc(ct_dt):
    return ct_dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S")


def _verify_contract(api_client, cid):
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
    global ES_CONTRACT
    try:
        from tsxapipy.api.contract_utils import get_futures_contract_details
        from datetime import date
        result = get_futures_contract_details(api_client, date.today(), "EP")
        if result:
            str_id, int_id = result
            if str_id and str_id != "":
                if _verify_contract(api_client, str_id):
                    ES_CONTRACT = str_id
                    ES["contract_id"] = ES_CONTRACT
                    logger.info(f"ES contract resolved via API: {ES_CONTRACT}")
                    return True
                else:
                    logger.warning(f"API returned {str_id} but bar fetch failed — trying fallback")
    except Exception as e:
        logger.warning(f"ES contract API search failed: {e}")

    if _verify_contract(api_client, ES_CONTRACT_FALLBACK):
        ES_CONTRACT = ES_CONTRACT_FALLBACK
        ES["contract_id"] = ES_CONTRACT
        logger.info(f"Using ES fallback contract: {ES_CONTRACT} (verified)")
        return True

    ES_CONTRACT = ES_CONTRACT_FALLBACK
    ES["contract_id"] = ES_CONTRACT
    logger.warning(f"Using ES fallback unverified: {ES_CONTRACT}")
    return True


# ═══════════════════════════════════════════════════════════════
# V106 SCANNER (same as NQ bot, adapted for ES)
# ═══════════════════════════════════════════════════════════════
class V106Scanner:
    def __init__(self, api_client, contract_id):
        self.api = api_client
        self.cid = contract_id
        self.seen = set()
        self._b5_cache = []
        self._b15_cache = []
        self._dr5_cache = {}
        self._dr15_cache = {}
        self._all_dates_cache = set()
        self._last_5m_refresh = 0.0
        self._last_rest_sync = 0.0
        self._base_1m = []
        self._live_1m = []
        self._current_bar = None
        self._bar_minute = -1
        self._last_signal_side = None
        self._last_signal_ns = 0

    def on_tick(self, price, ct_now):
        cur_min = ct_now.hour * 60 + ct_now.minute
        bar_closed = False
        if cur_min != self._bar_minute:
            if self._current_bar is not None:
                self._live_1m.append(self._current_bar)
                bar_closed = True
            self._bar_minute = cur_min
            t_ns = int(ct_now.replace(second=0, microsecond=0).timestamp() * 1e9)
            self._current_bar = {
                "time_ns": t_ns,
                "open": price, "high": price,
                "low": price, "close": price,
                "hour": ct_now.hour, "minute": ct_now.minute,
            }
        else:
            if self._current_bar is not None:
                if price > self._current_bar["high"]:
                    self._current_bar["high"] = price
                if price < self._current_bar["low"]:
                    self._current_bar["low"] = price
                self._current_bar["close"] = price
        return bar_closed

    def load_historical_1m(self):
        now = datetime.now(CT)
        yesterday_5pm = (now.replace(hour=17, minute=0, second=0) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
        end = now.strftime("%Y-%m-%dT%H:%M:%S")
        rest_bars = self._fetch(1, yesterday_5pm, end)
        if rest_bars:
            last_rest_ns = rest_bars[-1]["time_ns"]
            newer_live = [b for b in self._live_1m if b["time_ns"] > last_rest_ns]
            self._base_1m = rest_bars
            self._live_1m = newer_live
            logger.info(f"  ES 1m history: {len(rest_bars)} REST + {len(newer_live)} live")

    def _fetch_with_rollover(self, tf_minutes, start, end):
        """Fetch bars, backfilling from prior contract if current has limited history."""
        bars = self._fetch(tf_minutes, start, end)
        if bars and len(bars) < 500 and ES_CONTRACT_PREV:
            old_cid = self.cid
            self.cid = ES_CONTRACT_PREV
            prev_bars = self._fetch(tf_minutes, start, end)
            self.cid = old_cid
            if prev_bars and len(prev_bars) > len(bars):
                first_new_ns = bars[0]["time_ns"]
                older = [b for b in prev_bars if b["time_ns"] < first_new_ns]
                if older:
                    bars = older + bars
                    logger.info(f"  ES rollover backfill: {len(older)} {tf_minutes}m bars from prior contract")
        return bars

    def refresh_htf_bars(self):
        now = datetime.now(CT)
        start_hist = (now - timedelta(days=10)).strftime("%Y-%m-%dT00:00:00")
        end = now.strftime("%Y-%m-%dT%H:%M:%S")
        b5_rest = self._fetch_with_rollover(5, start_hist, end)
        if not b5_rest:
            return False
        b15_rest = self._fetch_with_rollover(15, start_hist, end)
        self._b5_cache = b5_rest
        self._b15_cache = b15_rest if b15_rest else []
        self._dr5_cache = self._build_dr(self._b5_cache)
        self._dr15_cache = self._build_dr(self._b15_cache)
        self._all_dates_cache = set(self._dr5_cache.keys())
        self._last_5m_refresh = time.time()
        logger.info(f"  ES HTF cache: 5m={len(self._b5_cache)} 15m={len(self._b15_cache)}")
        return True

    def initial_load(self):
        if self._b5_cache:
            return True
        if not self.refresh_htf_bars():
            return False
        self.load_historical_1m()
        self._last_rest_sync = time.time()
        return True

    def background_rest_sync(self):
        try:
            logger.info("  ES background REST sync starting...")
            now = datetime.now(CT)
            start_hist = (now - timedelta(days=10)).strftime("%Y-%m-%dT00:00:00")
            end = now.strftime("%Y-%m-%dT%H:%M:%S")
            b5_rest = self._fetch_with_rollover(5, start_hist, end)
            if not b5_rest:
                return
            b15_rest = self._fetch_with_rollover(15, start_hist, end)
            b5_new = b5_rest
            b15_new = b15_rest if b15_rest else []
            dr5_new = self._build_dr(b5_new)
            dr15_new = self._build_dr(b15_new)
            all_dates_new = set(dr5_new.keys())
            yesterday_5pm = (now.replace(hour=17, minute=0, second=0) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
            rest_1m = self._fetch(1, yesterday_5pm, end)
            # Atomic swap
            self._b5_cache = b5_new
            self._b15_cache = b15_new
            self._dr5_cache = dr5_new
            self._dr15_cache = dr15_new
            self._all_dates_cache = all_dates_new
            self._last_5m_refresh = time.time()
            if rest_1m:
                last_rest_ns = rest_1m[-1]["time_ns"]
                self._base_1m = rest_1m
                self._live_1m = [b for b in self._live_1m if b["time_ns"] > last_rest_ns]
            self._last_rest_sync = time.time()
            logger.info(f"  ES REST sync done: 5m={len(b5_new)} 1m={len(self._base_1m)}+{len(self._live_1m)}")
        except Exception as e:
            logger.error(f"  ES REST sync failed: {e}")

    def needs_rest_sync(self):
        return time.time() - self._last_rest_sync > 1800

    def scan(self, is_5m_boundary=False):
        now = datetime.now(CT)
        today = now.date()
        if not self._b5_cache:
            return []
        # BUG 18 fix: Fetch from REST instead of building from WS (phantom signal fix)
        if is_5m_boundary:
            new_5m = self._fetch_latest_bar(5)
            if new_5m:
                if not self._b5_cache or new_5m["time_ns"] != self._b5_cache[-1]["time_ns"]:
                    self._b5_cache.append(new_5m)
                    self._dr5_cache = self._build_dr(self._b5_cache)
                    self._all_dates_cache = set(self._dr5_cache.keys())
                    logger.info(f"  ES REST 5m bar: {new_5m['hour']:02d}:{new_5m['minute']:02d} O={new_5m['open']:.2f} H={new_5m['high']:.2f} L={new_5m['low']:.2f} C={new_5m['close']:.2f} (total 5m={len(self._b5_cache)})")
            if now.minute % 15 == 0:
                new_15m = self._fetch_latest_bar(15)
                if new_15m:
                    if not self._b15_cache or new_15m["time_ns"] != self._b15_cache[-1]["time_ns"]:
                        self._b15_cache.append(new_15m)
                        self._dr15_cache = self._build_dr(self._b15_cache)
                        logger.info(f"  ES REST 15m bar: {new_15m['hour']:02d}:{new_15m['minute']:02d} (total 15m={len(self._b15_cache)})")
            self._last_5m_refresh = time.time()

        b5 = self._b5_cache
        b15 = self._b15_cache
        dr5 = self._dr5_cache
        dr15 = self._dr15_cache
        all_dates = self._all_dates_cache
        b1 = self._base_1m + self._live_1m

        if today not in dr5:
            return []
        ds5, de5 = dr5[today]

        # BUG 16 fix: Virtual 5m bar DISABLED — causes stop mismatch vs backtest.
        liq = get_liquidity_levels(b5, self._dr5_cache, today, self._all_dates_cache)
        entries = gen_sweep_entries(b5, b1, ds5, de5, today, liq)

        slip = ES["slip"]
        new_signals = []
        seen_bars = set()

        for e in sorted(entries, key=lambda x: (x["ns"], -{"ifvg": 2, "disp_fvg": 1}.get(x["zt"], 0))):
            if e["ns"] in self.seen:
                continue
            if e["bar_idx"] in seen_bars:
                continue
            seen_bars.add(e["bar_idx"])

            ep = e["ep"]; sp = e["sp"]; side = e["side"]
            if side == "bull":
                ep += slip
            else:
                ep -= slip
            risk = abs(ep - sp)
            if risk <= 0:
                continue

            has_rej = bool(e.get("rej"))
            has_cisd = cisd_5m(b5, e["bar_idx"], ds5) == side
            sw_d, _, _ = detect_sweep_at(b5, e["bar_idx"], liq, lookback=8)
            has_sweep = sw_d == side
            struct = structure_15m(b15, dr15, today, e["ns"])
            has_struct = struct == side
            sw15 = sweep_15m(b15, dr15, today, e["ns"], liq, side)

            score = 1 + has_rej + has_cisd + has_sweep * 2 + has_struct * 2 + sw15

            if has_sweep: rr = 2.0
            elif has_struct: rr = 1.7
            elif has_cisd: rr = 1.5
            else: rr = 1.3

            d_dir = 1 if side == "bull" else -1
            tp = ep + risk * rr * d_dir

            nct = ES["contracts"]  # 2ct
            ar = risk * ES["pv"] * nct
            if ar > MAX_RISK or ar <= 0:
                logger.info(f"  ES risk filter: {side} {e['zt']} @ {ep:.2f} — ${ar:,.0f} > ${MAX_RISK}")
                continue

            new_signals.append({
                "side": side,
                "entry": tick_round(ep),
                "stop": tick_round(sp),
                "target": tick_round(tp),
                "risk": risk,
                "rr": rr,
                "score": score,
                "zone_type": e["zt"],
                "contracts": nct,
                "time": datetime.fromtimestamp(e["ns"] / 1e9, tz=CT),
                "_ns": e["ns"],
            })

        return new_signals

    def mark_executed(self, sig):
        self.seen.add(sig["_ns"])
        self._last_signal_side = sig["side"]
        self._last_signal_ns = sig["_ns"]

    def new_day(self):
        self.seen.clear()
        self._b5_cache = []
        self._b15_cache = []
        self._dr5_cache = {}
        self._dr15_cache = {}
        self._all_dates_cache = set()
        self._last_5m_refresh = 0.0
        self._base_1m = []
        self._live_1m = []
        self._current_bar = None
        self._bar_minute = -1
        self._last_signal_side = None
        self._last_signal_ns = 0

    def _aggregate_completed_5m(self):
        now = datetime.now(CT)
        end_min = (now.minute // 5) * 5
        end_time = now.replace(minute=end_min, second=0, microsecond=0)
        start_time = end_time - timedelta(minutes=5)
        start_ns = int(start_time.timestamp() * 1e9)
        end_ns = int(end_time.timestamp() * 1e9)
        b1 = self._base_1m + self._live_1m
        bars = [b for b in b1 if start_ns <= b["time_ns"] < end_ns]
        if len(bars) < 2:
            return None
        return {
            "time_ns": start_ns,
            "open": bars[0]["open"], "high": max(b["high"] for b in bars),
            "low": min(b["low"] for b in bars), "close": bars[-1]["close"],
            "hour": start_time.hour, "minute": start_time.minute,
        }

    def _aggregate_completed_15m(self):
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
            "open": bars[0]["open"], "high": max(b["high"] for b in bars),
            "low": min(b["low"] for b in bars), "close": bars[-1]["close"],
            "hour": start_time.hour, "minute": start_time.minute,
        }

    def _fetch_latest_bar(self, tf_minutes):
        """BUG 18 fix: Fetch the latest completed bar from REST API."""
        now = datetime.now(CT)
        s_ct = now - timedelta(minutes=tf_minutes * 3)
        UTC = ZoneInfo("UTC")
        try:
            s_utc = s_ct.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
            e_utc = now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
            resp = self.api.get_historical_bars(
                contract_id=self.cid,
                start_time_iso=s_utc,
                end_time_iso=e_utc,
                unit=2, unit_number=tf_minutes, limit=10, live=False,
            )
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
            logger.warning(f"  ES REST fetch latest {tf_minutes}m bar failed: {e}")
            return None

    def _build_virtual_5m(self):
        now = datetime.now(CT)
        cur_5m_min = (now.minute // 5) * 5
        cur_5m_start = now.replace(minute=cur_5m_min, second=0, microsecond=0)
        start_ns = int(cur_5m_start.timestamp() * 1e9)
        bars = [b for b in self._base_1m if b["time_ns"] >= start_ns]
        bars += [b for b in self._live_1m if b["time_ns"] >= start_ns]
        if not bars:
            return None
        return {
            "time_ns": start_ns,
            "open": bars[0]["open"], "high": max(b["high"] for b in bars),
            "low": min(b["low"] for b in bars), "close": bars[-1]["close"],
            "hour": cur_5m_start.hour, "minute": cur_5m_min,
        }

    def _fetch(self, tf_minutes, start, end):
        all_bars = []
        UTC = ZoneInfo("UTC")
        s = datetime.fromisoformat(start).replace(tzinfo=CT)
        e = datetime.fromisoformat(end).replace(tzinfo=CT)
        if tf_minutes == 1: chunk_days = 2
        elif tf_minutes == 5: chunk_days = 2
        else: chunk_days = 5
        while s < e:
            chunk_end = min(s + timedelta(days=chunk_days), e)
            for attempt in range(3):
                try:
                    s_utc = s.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
                    e_utc = chunk_end.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
                    resp = self.api.get_historical_bars(
                        contract_id=self.cid, start_time_iso=s_utc, end_time_iso=e_utc,
                        unit=2, unit_number=tf_minutes, limit=1000, live=False,
                    )
                    if resp and resp.bars:
                        all_bars.extend(resp.bars)
                    break
                except Exception as ex:
                    if attempt == 2:
                        logger.warning(f"ES bar fetch ({tf_minutes}m) failed: {ex}")
                    else:
                        time.sleep(3)
            s = chunk_end
            if s < e:
                time.sleep(1)
        seen = set(); result = []
        for b in all_bars:
            try:
                t = b.t if hasattr(b.t, 'astimezone') else datetime.fromisoformat(str(b.t))
                t = t.astimezone(CT)
                ns = int(t.timestamp() * 1e9)
                if ns in seen: continue
                seen.add(ns)
                result.append({
                    "time_ns": ns, "open": float(b.o), "high": float(b.h),
                    "low": float(b.l), "close": float(b.c),
                    "hour": t.hour, "minute": t.minute,
                })
            except Exception:
                continue
        result.sort(key=lambda x: x["time_ns"])
        return result

    @staticmethod
    def _build_dr(bars):
        dr = {}
        for i, b in enumerate(bars):
            t = datetime.fromtimestamp(b["time_ns"] / 1e9, tz=CT)
            d = t.date()
            if d not in dr: dr[d] = (i, i + 1)
            else: dr[d] = (dr[d][0], i + 1)
        return dr


# ═══════════════════════════════════════════════════════════════
# SIGNAL BOT
# ═══════════════════════════════════════════════════════════════
class ESSignalBot:
    def __init__(self, api_client: APIClient):
        self.api = api_client
        self.scanner = V106Scanner(api_client, ES["contract_id"])
        self.live_price: float = 0.0
        self._last_quote_time: float = 0.0
        self._quote_count: int = 0
        self.data_stream = None
        self._scan_event = threading.Event()
        self._is_5m_boundary = False
        self._last_scan = 0.0
        self._token_time = time.time()
        self._last_reconnect = 0.0
        self.current_date = ""
        self.signals_today = 0
        self.cl_bull = 0
        self.cl_bear = 0
        self.gc = 0

    def _on_quote(self, quote):
        try:
            price = quote.get("lastPrice") or quote.get("LastPrice")
            if not price:
                return
            self.live_price = float(price)
            self._last_quote_time = time.monotonic()
            self._quote_count += 1
            if self._quote_count <= 3 or self._quote_count % 500 == 0:
                logger.info(f"  ES tick #{self._quote_count}: {self.live_price}")
            now = datetime.now(CT)
            bar_closed = self.scanner.on_tick(self.live_price, now)
            if bar_closed and _in_kz(now.hour, now.minute):
                if now.minute % 5 == 0:
                    self._is_5m_boundary = True
                self._scan_event.set()
        except Exception as e:
            logger.error(f"  ES _on_quote error: {e}")

    def _start_stream(self):
        cid = ES["contract_id"]
        self.data_stream = DataStream(
            api_client=self.api,
            contract_id_to_subscribe=cid,
            on_quote_callback=self._on_quote,
            auto_subscribe_quotes=True,
            auto_subscribe_depth=False,
        )
        try:
            self.data_stream.start()
            self._last_quote_time = time.monotonic()  # seed so heartbeat can detect dead stream
            logger.info("  ES DataStream started")
        except Exception as e:
            logger.error(f"  ES DataStream failed: {e}")

    def _stop_stream(self):
        try:
            if self.data_stream:
                self.data_stream.stop()
        except Exception:
            pass

    def _new_day(self):
        today = datetime.now(CT).strftime("%Y-%m-%d")
        if today != self.current_date:
            self.current_date = today
            self.signals_today = 0
            self.cl_bull = 0
            self.cl_bear = 0
            self.gc = 0
            self.scanner.new_day()
            logger.info(f"=== ES NEW DAY: {today} ===")

    def _refresh_token(self):
        if time.time() - self._token_time < TOKEN_REFRESH_HOURS * 3600:
            return
        try:
            new_token = self.api.current_token
            self._token_time = time.time()
            if self.data_stream:
                self.data_stream.update_token(new_token)
            logger.info("ES token refreshed")
        except Exception as e:
            logger.warning(f"ES token refresh failed: {e}")

    def run(self):
        logger.info("=" * 60)
        logger.info("ES SIGNAL BOT — V106 Sweep > Disp > FVG Zone")
        logger.info("ALERTS ONLY — no orders placed")
        logger.info(f"ES 1ct | 4T-v3 RR | MR $1000 | MCL {MCL}/side | GMCL {GMCL}")
        logger.info(f"KZ {KZ_START[0]}:{KZ_START[1]:02d}-{KZ_END[0]}:{KZ_END[1]:02d} CT")
        logger.info("=" * 60)

        if not resolve_contract(self.api):
            logger.error("ES contract resolution failed")
            return
        self.scanner.cid = ES["contract_id"]

        logger.info("Loading ES bar data...")
        if not self.scanner.initial_load():
            logger.error("ES initial load failed")
            return

        self._start_stream()

        tg("<b>ES Signal Bot Started</b>\n"
           "ES 1ct | V106 4T-v3 | Alerts Only\n"
           f"KZ {KZ_START[0]}:{KZ_START[1]:02d}-{KZ_END[0]}:{KZ_END[1]:02d} CT")

        while True:
            try:
                self._new_day()
                self._refresh_token()
                now = datetime.now(CT)

                # Heartbeat — runs 24/7 so stream stays alive overnight
                if (self._last_quote_time > 0
                        and time.monotonic() - self._last_quote_time > 120
                        and time.monotonic() - self._last_reconnect > 120):
                    logger.warning("  ES stream dead — reconnecting")
                    self._stop_stream()
                    time.sleep(2)
                    self._start_stream()
                    self._last_quote_time = time.monotonic()
                    self._last_reconnect = time.monotonic()

                if (now.month == 12 and now.day >= 26) or (now.month == 1 and now.day <= 2):
                    time.sleep(60); continue
                if now.weekday() > 4 or now.hour < 7 or now.hour >= 16:
                    time.sleep(30); continue

                # Background REST sync
                if (_in_kz(now.hour, now.minute)
                        and self.scanner.needs_rest_sync()
                        and not getattr(self, '_rest_active', False)):
                    self._rest_active = True
                    def _bg():
                        try:
                            self.scanner.background_rest_sync()
                        finally:
                            self._rest_active = False
                            self._scan_event.set()
                    threading.Thread(target=_bg, daemon=True).start()

                # Scan
                should_scan = False
                is_5m = False
                if self._scan_event.is_set():
                    self._scan_event.clear()
                    should_scan = True
                    is_5m = self._is_5m_boundary
                    self._is_5m_boundary = False
                elif _in_kz(now.hour, now.minute) and time.time() - self._last_scan >= SCAN_INTERVAL:
                    should_scan = True

                if should_scan:
                    self._last_scan = time.time()
                    try:
                        signals = self.scanner.scan(is_5m_boundary=is_5m)
                        if signals:
                            logger.info(f"  ES Scan: {len(signals)} signal(s)")
                        for sig in signals:
                            sig_age = (datetime.now(CT) - sig["time"]).total_seconds()
                            if sig_age > 120:
                                logger.warning(f"  ES STALE: {sig['side']} {sig['zone_type']} @ {sig['entry']:.2f} ({sig_age:.0f}s)")
                                self.scanner.mark_executed(sig)
                                continue

                            # Alert via Telegram
                            nct = sig["contracts"]
                            d_dir = 1 if sig["side"] == "bull" else -1
                            direction = "LONG" if sig["side"] == "bull" else "SHORT"
                            risk_dollar = sig["risk"] * ES["pv"] * nct

                            logger.info(f">>> ES SIGNAL: {direction} @ {sig['entry']:.2f}")
                            logger.info(f"    SL: {sig['stop']:.2f} | TP: {sig['target']:.2f} | "
                                        f"Risk: {sig['risk']:.1f}pts (${risk_dollar:.0f}) | {sig['rr']}R | Score: {sig['score']}")

                            tg(f"<b>🔔 ES SIGNAL — {direction}</b>\n"
                               f"Entry: {sig['entry']:.2f}\n"
                               f"Stop: {sig['stop']:.2f}\n"
                               f"Target: {sig['target']:.2f}\n"
                               f"Risk: {sig['risk']:.1f}pts (${risk_dollar:.0f}) | {sig['rr']}R\n"
                               f"Score: {sig['score']} | Zone: {sig['zone_type']}\n"
                               f"Live: {self.live_price:.2f} | Age: {sig_age:.0f}s")

                            self.scanner.mark_executed(sig)
                            self.signals_today += 1
                            break  # one signal at a time
                    except Exception as e:
                        logger.error(f"  ES scan error: {e}", exc_info=True)

                self._scan_event.wait(timeout=10)

            except KeyboardInterrupt:
                logger.info("ES Signal Bot shutdown")
                break
            except Exception as e:
                logger.error(f"ES error: {e}", exc_info=True)
                time.sleep(30)

        self._stop_stream()
        tg("ES Signal Bot stopped")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    # Log to both console AND dated file (never lose logs on restart)
    log_fmt = logging.Formatter('%(asctime)s | %(name)-6s | %(message)s', datefmt='%H:%M:%S')
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    console_h = logging.StreamHandler()
    console_h.setFormatter(log_fmt)
    root.addHandler(console_h)

    log_date = datetime.now(CT).strftime("%Y%m%d")
    file_h = logging.FileHandler(f"es_signal_bot_{log_date}.log", mode="a")
    file_h.setFormatter(log_fmt)
    root.addHandler(file_h)

    logging.getLogger("tsxapipy").setLevel(logging.WARNING)
    logging.getLogger("signalrcore").setLevel(logging.WARNING)
    logging.getLogger("SignalRCoreClient").setLevel(logging.ERROR)

    logger.info("ES Signal Bot — Authenticating...")
    token, token_time = authenticate()
    if not token:
        logger.error("Auth failed!")
        sys.exit(1)

    api = APIClient(initial_token=token, token_acquired_at=token_time)

    if ES["contract_id"] is None:
        ES["contract_id"] = ES_CONTRACT_FALLBACK

    bot = ESSignalBot(api)
    bot.run()


if __name__ == "__main__":
    main()
