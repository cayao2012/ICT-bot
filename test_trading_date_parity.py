"""
test_trading_date_parity.py
============================
Proves Patch 2: _bar_trading_date helper + _build_dr now use trading-date semantics,
matching bars_cache.pkl (built by build_new_bars.py) and backtest_run.py.

Bug being tested:
  ptnut_bot._build_dr used t.date() (calendar date). Bars at/after 17:00 CT were
  grouped under the current calendar day rather than the next session's date.
  This caused dr5 keys to differ between live and replay, producing different
  liquidity levels — most severely Monday PDH/PDL (built from Sunday evening only)
  and Asia levels (off by one session on Mondays).

Fix being tested:
  1. _bar_trading_date(ts_ns): canonical helper — hour >= 17 => date + 1 day
  2. _build_dr: uses _bar_trading_date instead of t.date()
  3. scan(): today = _bar_trading_date(now) so dr5[today] resolves correctly

No broker connection. No real data. No live imports beyond ptnut_bot module-level items.
Run: PYTHONPATH=tsxapi4py/src python3 test_trading_date_parity.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))

from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

CT = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")

# Import only the helper and _build_dr — no broker stack needed.
# ptnut_bot imports succeed at module level without live credentials.
from ptnut_bot import _bar_trading_date, V106Scanner


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _ns(dt: datetime) -> int:
    """Convert a CT-aware datetime to nanosecond timestamp."""
    return int(dt.timestamp() * 1_000_000_000)


def _bar(ts_ns: int) -> dict:
    """Minimal bar dict with only the time_ns field _build_dr needs."""
    return {"time_ns": ts_ns, "open": 0, "high": 0, "low": 0, "close": 0}


def _dt(y, mo, d, h, mi, *, tz=CT) -> datetime:
    return datetime(y, mo, d, h, mi, tzinfo=tz)


# ──────────────────────────────────────────────────────────────
# 1. _bar_trading_date boundary tests
# ──────────────────────────────────────────────────────────────

def test_before_17_stays_same_date():
    """Bars before 17:00 CT: trading date == calendar date."""
    cases = [
        (_dt(2025, 1, 6,  0,  0), date(2025, 1, 6)),  # midnight Monday
        (_dt(2025, 1, 6,  9, 30), date(2025, 1, 6)),  # Monday NY open
        (_dt(2025, 1, 6, 14, 30), date(2025, 1, 6)),  # Monday KZ close
        (_dt(2025, 1, 6, 16, 59), date(2025, 1, 6)),  # one minute before boundary
    ]
    for dt, expected in cases:
        result = _bar_trading_date(_ns(dt))
        assert result == expected, (
            f"{dt.strftime('%a %H:%M')}: expected {expected}, got {result}"
        )
    print("PASS  test_before_17_stays_same_date")


def test_at_17_advances_to_next_day():
    """Bars exactly at 17:00 CT belong to the next calendar day's session."""
    cases = [
        (_dt(2025, 1, 6, 17,  0), date(2025, 1, 7)),  # Mon 17:00 -> Tue session
        (_dt(2025, 1, 7, 17,  0), date(2025, 1, 8)),  # Tue 17:00 -> Wed session
        (_dt(2025, 1, 9, 17,  0), date(2025, 1, 10)), # Thu 17:00 -> Fri session
    ]
    for dt, expected in cases:
        result = _bar_trading_date(_ns(dt))
        assert result == expected, (
            f"{dt.strftime('%a %H:%M')}: expected {expected}, got {result}"
        )
    print("PASS  test_at_17_advances_to_next_day")


def test_after_17_advances_to_next_day():
    """Bars between 17:01 and 23:59 CT belong to the next calendar day's session."""
    cases = [
        (_dt(2025, 1, 6, 17,  1), date(2025, 1, 7)),
        (_dt(2025, 1, 6, 20,  0), date(2025, 1, 7)),
        (_dt(2025, 1, 6, 23, 59), date(2025, 1, 7)),
    ]
    for dt, expected in cases:
        result = _bar_trading_date(_ns(dt))
        assert result == expected, (
            f"{dt.strftime('%a %H:%M')}: expected {expected}, got {result}"
        )
    print("PASS  test_after_17_advances_to_next_day")


# ──────────────────────────────────────────────────────────────
# 2. The critical Monday / Sunday-evening edge case
# ──────────────────────────────────────────────────────────────

def test_sunday_evening_maps_to_monday():
    """CME opens Sunday 17:00 CT. Those bars must be indexed as Monday's session.

    Bug (calendar date): Sun 17:00-23:59 bars -> dr5[Sunday]
    Fix (trading date):  Sun 17:00-23:59 bars -> dr5[Monday]

    This is the most severe case: under the bug, Monday's PDH/PDL was built
    from Sunday evening only (7h), not from the full prior Friday session.
    """
    # Sunday 2025-01-05 17:00 CT = CME opens for Monday session
    sun_17 = _dt(2025, 1, 5, 17,  0)  # Sunday 5pm
    sun_20 = _dt(2025, 1, 5, 20,  0)  # Sunday 8pm (middle of Asia session)
    sun_23 = _dt(2025, 1, 5, 23, 59)  # Sunday 11:59pm

    for dt in (sun_17, sun_20, sun_23):
        result = _bar_trading_date(_ns(dt))
        assert result == date(2025, 1, 6), (   # Monday Jan 6
            f"Sunday {dt.strftime('%H:%M')} should map to Monday Jan 6, got {result}"
        )

    # Confirm Sunday 16:59 still maps to Sunday (before CME session)
    sun_1659 = _dt(2025, 1, 5, 16, 59)
    assert _bar_trading_date(_ns(sun_1659)) == date(2025, 1, 5), (
        "Sunday 16:59 should still map to Sunday (before CME open)"
    )
    print("PASS  test_sunday_evening_maps_to_monday")


def test_build_dr_groups_sunday_evening_under_monday():
    """_build_dr must index Sunday 17:00+ bars under Monday, not Sunday.

    This directly proves the PDH/PDL parity fix: with the fixed _build_dr,
    prev_d for Monday = Friday, not Sunday-evening-only.
    """
    # Build a minimal bar list: Fri daytime + Sun evening + Mon daytime
    fri_bar    = _bar(_ns(_dt(2025, 1,  3, 10,  0)))  # Friday 10am  -> Fri Jan 3
    sun_bar    = _bar(_ns(_dt(2025, 1,  5, 18,  0)))  # Sunday 6pm   -> Mon Jan 6 (fixed)
    mon_bar    = _bar(_ns(_dt(2025, 1,  6,  9, 30)))  # Monday 9:30  -> Mon Jan 6

    bars = [fri_bar, sun_bar, mon_bar]
    dr = V106Scanner._build_dr(bars)

    # Friday and Monday should each be a key; Sunday should NOT appear
    assert date(2025, 1, 3) in dr, "Friday Jan 3 must be in dr"
    assert date(2025, 1, 6) in dr, "Monday Jan 6 must be in dr"
    assert date(2025, 1, 5) not in dr, (
        f"Sunday Jan 5 must NOT be a key — got keys: {sorted(dr.keys())}"
    )

    # Both sun_bar (index 1) and mon_bar (index 2) must be under Monday
    monday_start, monday_end = dr[date(2025, 1, 6)]
    assert monday_start == 1, f"Monday session must start at bar index 1 (sun_bar), got {monday_start}"
    assert monday_end == 3,   f"Monday session must end at bar index 3 (exclusive), got {monday_end}"

    print("PASS  test_build_dr_groups_sunday_evening_under_monday")


def test_monday_prev_d_is_friday_not_sunday():
    """After the fix, sorted_dates for a Monday has no Sunday entry.

    get_liquidity_levels uses sorted_dates[d_idx - 1] as prev_d for PDH/PDL.
    Under the bug (calendar date), Sunday appears in sorted_dates and becomes
    prev_d for Monday, meaning PDH/PDL is built from Sunday evening only.
    After the fix (trading date), Sunday never appears — prev_d is Friday.
    """
    # A full week's bars: Fri daytime, Sun evening (CME open), Mon daytime
    fri_morning = _bar(_ns(_dt(2025, 1,  3,  9, 30)))   # Fri -> Jan 3
    fri_midday  = _bar(_ns(_dt(2025, 1,  3, 12,  0)))   # Fri -> Jan 3
    sun_evening = _bar(_ns(_dt(2025, 1,  5, 18,  0)))   # Sun -> Jan 6 (fixed)
    mon_morning = _bar(_ns(_dt(2025, 1,  6,  9, 30)))   # Mon -> Jan 6

    bars = [fri_morning, fri_midday, sun_evening, mon_morning]
    dr = V106Scanner._build_dr(bars)
    sorted_dates = sorted(dr.keys())

    # Only two dates should exist: Jan 3 (Friday) and Jan 6 (Monday)
    assert sorted_dates == [date(2025, 1, 3), date(2025, 1, 6)], (
        f"Expected [Jan3, Jan6], got {sorted_dates}. "
        f"Sunday must not appear as a separate key."
    )

    # For Monday (d_idx=1): prev_d = sorted_dates[0] = Friday Jan 3
    monday_idx = sorted_dates.index(date(2025, 1, 6))
    prev_d = sorted_dates[monday_idx - 1]
    assert prev_d == date(2025, 1, 3), (
        f"prev_d for Monday must be Friday Jan 3, got {prev_d}"
    )
    print("PASS  test_monday_prev_d_is_friday_not_sunday")


# ──────────────────────────────────────────────────────────────
# 3. Weekday bars (no change — must not regress)
# ──────────────────────────────────────────────────────────────

def test_weekday_bars_before_17_unchanged():
    """Bars before 17:00 on weekdays: trading date == calendar date.
    The fix must not change behavior for bars during normal trading hours.
    """
    tue_930  = _dt(2025, 1, 7,  9, 30)   # Tuesday 9:30 -> Jan 7
    wed_noon = _dt(2025, 1, 8, 12,  0)   # Wednesday noon -> Jan 8
    thu_1430 = _dt(2025, 1, 9, 14, 30)   # Thursday 2:30pm -> Jan 9

    assert _bar_trading_date(_ns(tue_930))  == date(2025, 1, 7)
    assert _bar_trading_date(_ns(wed_noon)) == date(2025, 1, 8)
    assert _bar_trading_date(_ns(thu_1430)) == date(2025, 1, 9)
    print("PASS  test_weekday_bars_before_17_unchanged")


def test_build_dr_contiguous_weekdays():
    """A normal weekday sequence (Tue-Wed) with evening bars groups correctly."""
    # Tue daytime, Tue 18:00 (goes into Wed), Wed 9:30
    tue_day = _bar(_ns(_dt(2025, 1, 7,  9, 30)))   # -> Jan 7 (Tue)
    tue_eve = _bar(_ns(_dt(2025, 1, 7, 18,  0)))   # -> Jan 8 (Wed session)
    wed_day = _bar(_ns(_dt(2025, 1, 8,  9, 30)))   # -> Jan 8 (Wed)

    dr = V106Scanner._build_dr([tue_day, tue_eve, wed_day])
    sorted_dates = sorted(dr.keys())

    assert sorted_dates == [date(2025, 1, 7), date(2025, 1, 8)], (
        f"Expected [Jan7, Jan8], got {sorted_dates}"
    )
    # Jan 8 session contains both tue_eve (index 1) and wed_day (index 2)
    jan8_start, jan8_end = dr[date(2025, 1, 8)]
    assert jan8_start == 1, f"Jan8 session should start at bar index 1, got {jan8_start}"
    assert jan8_end   == 3, f"Jan8 session should end at bar index 3, got {jan8_end}"
    print("PASS  test_build_dr_contiguous_weekdays")


# ──────────────────────────────────────────────────────────────
# 4. Parity with build_new_bars.py rule
# ──────────────────────────────────────────────────────────────

def test_matches_build_new_bars_rule():
    """_bar_trading_date must produce exactly the same dates as build_new_bars.py.

    build_new_bars.py:
        bar_date = bar_dt.date()
        if bar_dt.hour >= 17:
            bar_date = bar_date + timedelta(days=1)

    This is the authoritative rule. Both must agree on every hour boundary.
    """
    def build_new_bars_rule(dt: datetime) -> date:
        d = dt.date()
        if dt.hour >= 17:
            d = d + timedelta(days=1)
        return d

    test_times = [
        _dt(2025, 1, 6, 16, 59),  # Mon just before boundary
        _dt(2025, 1, 6, 17,  0),  # Mon at boundary
        _dt(2025, 1, 6, 23, 59),  # Mon late night
        _dt(2025, 1, 5, 17,  0),  # Sun 5pm (CME open)
        _dt(2025, 1, 5, 20,  0),  # Sun evening
        _dt(2025, 1, 7,  9, 30),  # Tue morning
        _dt(2025, 1, 9, 17,  0),  # Thu CME close (next session start)
        _dt(2025, 1, 3, 17,  0),  # Fri 5pm -> Sat (weekend)
    ]

    for dt in test_times:
        expected = build_new_bars_rule(dt)
        result   = _bar_trading_date(_ns(dt))
        assert result == expected, (
            f"{dt.strftime('%a %Y-%m-%d %H:%M')}: "
            f"build_new_bars={expected}, _bar_trading_date={result}"
        )
    print("PASS  test_matches_build_new_bars_rule")


# ──────────────────────────────────────────────────────────────
# 5. today in scan() resolves against fixed dr5
# ──────────────────────────────────────────────────────────────

def test_today_after_17_is_next_session():
    """After 17:00 CT, today must be tomorrow so scan() finds the correct dr5 key.

    After the fix, dr5 keys are trading dates. A scan at 18:30 on Monday must
    use Tuesday as 'today' — otherwise dr5[Monday] would exist (Monday's trading
    session = Sun17-Mon17) but the scan would look for Mon's calendar date which
    after 17:00 is Monday, while the active session key is Tuesday.

    Wait — actually at Mon 18:30, the next session is TUESDAY. The bars being
    built from 18:30 Mon are indexed under Tuesday. So today at scan time should
    be Tuesday. This is exactly what _bar_trading_date returns for Mon 18:30.
    """
    mon_1830 = _dt(2025, 1, 6, 18, 30)  # Monday 6:30 PM CT
    result = _bar_trading_date(_ns(mon_1830))
    assert result == date(2025, 1, 7), (  # Tuesday
        f"Scan at Mon 18:30 should use today=Tue Jan7, got {result}"
    )

    sun_1700 = _dt(2025, 1, 5, 17,  0)  # Sunday 5:00 PM CT (CME open)
    result2 = _bar_trading_date(_ns(sun_1700))
    assert result2 == date(2025, 1, 6), (  # Monday
        f"Scan at Sun 17:00 should use today=Mon Jan6, got {result2}"
    )
    print("PASS  test_today_after_17_is_next_session")


# ──────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_before_17_stays_same_date,
        test_at_17_advances_to_next_day,
        test_after_17_advances_to_next_day,
        test_sunday_evening_maps_to_monday,
        test_build_dr_groups_sunday_evening_under_monday,
        test_monday_prev_d_is_friday_not_sunday,
        test_weekday_bars_before_17_unchanged,
        test_build_dr_contiguous_weekdays,
        test_matches_build_new_bars_rule,
        test_today_after_17_is_next_session,
    ]

    print(f"\nTrading Date Parity — Patch 2 Tests\n{'='*50}")
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as e:
            print(f"FAIL  {t.__name__}:\n      {e}")
            failed += 1
        except Exception as e:
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
