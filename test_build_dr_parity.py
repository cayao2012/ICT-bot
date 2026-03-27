"""
test_build_dr_parity.py
=======================
Proves Patch 3: all build_dr implementations now return exclusive end-index
(de = last_idx + 1, Python slice style) so range(ds, de) in get_liquidity_levels
and gen_sweep_entries covers all bars in the session.

Bug being tested:
  backtest_run.build_dr and test_score_gate.build_dr returned INCLUSIVE end
  (de = last_idx). get_liquidity_levels uses range(ds, de), which with inclusive
  end silently skips the last bar of every session. ptnut_bot._build_dr returned
  EXCLUSIVE end but scan() called b5[:de5+1], producing a slice one bar too long.

Fix being tested:
  1. backtest_run.build_dr: (i, i) init -> (i, i+1); update last -> update last+1
  2. test_score_gate.build_dr: same fix
  3. ptnut_bot.scan(): b5[:de5+1] -> b5[:de5] (de5 already exclusive post Patch 2)
  4. Both cursor loops: range(ds5+1, de5+1) -> range(ds5+1, de5)

No broker connection. No real data.
Run: PYTHONPATH=tsxapi4py/src python3 test_build_dr_parity.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

CT = ZoneInfo("America/Chicago")

# Import both implementations under test.
# backtest_run.build_dr and test_score_gate.build_dr were the broken ones.
# ptnut_bot._build_dr was already exclusive (Patch 2 preserved it).
import backtest_run
import test_score_gate
from ptnut_bot import V106Scanner, _bar_trading_date


# ──────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────

def _ns(y, mo, d, h, mi):
    return int(datetime(y, mo, d, h, mi, tzinfo=CT).timestamp() * 1_000_000_000)


def _bar_with_date(ts_ns, d):
    """Bar dict with explicit date field (for backtest_run / test_score_gate)."""
    return {"time_ns": ts_ns, "date": d, "open": 0, "high": 0, "low": 0, "close": 0}


def _bar_ns_only(ts_ns):
    """Bar dict without date field (for ptnut_bot._build_dr, which derives it)."""
    return {"time_ns": ts_ns, "open": 0, "high": 0, "low": 0, "close": 0}


JAN6 = date(2025, 1, 6)   # Monday
JAN7 = date(2025, 1, 7)   # Tuesday


# ──────────────────────────────────────────────────────────────
# 1. Single-day session: end-index is last+1
# ──────────────────────────────────────────────────────────────

def test_single_day_end_is_exclusive():
    """A 3-bar session must return end_idx = 3 (exclusive), not 2 (inclusive)."""
    bars = [
        _bar_with_date(_ns(2025, 1, 6,  9, 30), JAN6),
        _bar_with_date(_ns(2025, 1, 6, 10,  0), JAN6),
        _bar_with_date(_ns(2025, 1, 6, 10, 30), JAN6),
    ]
    for impl_name, fn in [
        ("backtest_run.build_dr",    backtest_run.build_dr),
        ("test_score_gate.build_dr", test_score_gate.build_dr),
    ]:
        dr = fn(bars)
        assert JAN6 in dr, f"{impl_name}: JAN6 not in dr"
        ds, de = dr[JAN6]
        assert ds == 0, f"{impl_name}: ds should be 0, got {ds}"
        assert de == 3, (
            f"{impl_name}: de should be 3 (exclusive), got {de}. "
            f"Was {2} before fix (inclusive)."
        )
    # ptnut_bot path (derives date from time_ns)
    bot_bars = [_bar_ns_only(_ns(2025, 1, 6,  9, 30)),
                _bar_ns_only(_ns(2025, 1, 6, 10,  0)),
                _bar_ns_only(_ns(2025, 1, 6, 10, 30))]
    dr_bot = V106Scanner._build_dr(bot_bars)
    ds, de = dr_bot[JAN6]
    assert de == 3, f"ptnut_bot._build_dr: de should be 3, got {de}"
    print("PASS  test_single_day_end_is_exclusive")


def test_range_ds_de_covers_all_bars():
    """range(ds, de) with exclusive de must yield all bar indices in session."""
    bars = [
        _bar_with_date(_ns(2025, 1, 6,  9, 30), JAN6),
        _bar_with_date(_ns(2025, 1, 6, 10,  0), JAN6),
        _bar_with_date(_ns(2025, 1, 6, 10, 30), JAN6),
    ]
    dr = backtest_run.build_dr(bars)
    ds, de = dr[JAN6]
    covered = list(range(ds, de))
    assert covered == [0, 1, 2], (
        f"range(ds={ds}, de={de}) should cover [0,1,2], got {covered}"
    )
    print("PASS  test_range_ds_de_covers_all_bars")


# ──────────────────────────────────────────────────────────────
# 2. The last bar of a session is not missed
# ──────────────────────────────────────────────────────────────

def test_last_bar_included_in_range():
    """The last bar of the session must be reachable via range(ds, de).

    Before fix: de = 2 (last index, inclusive). range(0, 2) = [0, 1].
    Bar at index 2 was silently skipped — this is the session close bar.
    After fix:  de = 3 (exclusive). range(0, 3) = [0, 1, 2]. All bars covered.
    """
    bars = [
        _bar_with_date(_ns(2025, 1, 6,  9, 30), JAN6),  # index 0
        _bar_with_date(_ns(2025, 1, 6, 14,  0), JAN6),  # index 1 — midday
        _bar_with_date(_ns(2025, 1, 6, 14, 25), JAN6),  # index 2 — session close bar
    ]
    for impl_name, fn in [
        ("backtest_run.build_dr",    backtest_run.build_dr),
        ("test_score_gate.build_dr", test_score_gate.build_dr),
    ]:
        dr = fn(bars)
        ds, de = dr[JAN6]
        # The last bar (index 2) must be within range(ds, de)
        assert 2 in range(ds, de), (
            f"{impl_name}: last bar (index 2) not in range({ds}, {de}). "
            f"de should be 3 (exclusive)."
        )
    print("PASS  test_last_bar_included_in_range")


# ──────────────────────────────────────────────────────────────
# 3. Multi-day session: each day gets correct exclusive range
# ──────────────────────────────────────────────────────────────

def test_multi_day_each_session_exclusive():
    """Two consecutive days each get their own exclusive range."""
    bars = [
        _bar_with_date(_ns(2025, 1, 6,  9, 30), JAN6),  # index 0 — Mon
        _bar_with_date(_ns(2025, 1, 6, 10,  0), JAN6),  # index 1 — Mon
        _bar_with_date(_ns(2025, 1, 7,  9, 30), JAN7),  # index 2 — Tue
        _bar_with_date(_ns(2025, 1, 7, 10,  0), JAN7),  # index 3 — Tue
        _bar_with_date(_ns(2025, 1, 7, 10, 30), JAN7),  # index 4 — Tue
    ]
    for impl_name, fn in [
        ("backtest_run.build_dr",    backtest_run.build_dr),
        ("test_score_gate.build_dr", test_score_gate.build_dr),
    ]:
        dr = fn(bars)
        ds6, de6 = dr[JAN6]
        ds7, de7 = dr[JAN7]
        assert (ds6, de6) == (0, 2), f"{impl_name}: Mon should be (0,2), got ({ds6},{de6})"
        assert (ds7, de7) == (2, 5), f"{impl_name}: Tue should be (2,5), got ({ds7},{de7})"
        # Verify no overlap: Mon range ends where Tue range begins
        assert de6 == ds7, f"{impl_name}: Mon end ({de6}) must equal Tue start ({ds7})"
    print("PASS  test_multi_day_each_session_exclusive")


# ──────────────────────────────────────────────────────────────
# 4. Parity between all three implementations on the same input
# ──────────────────────────────────────────────────────────────

def test_all_implementations_agree():
    """backtest_run.build_dr, test_score_gate.build_dr, and ptnut_bot._build_dr
    must return identical (ds, de) tuples for the same session data.

    Uses trading-date bars (no bars at/after 17:00) so all three implementations
    agree on date grouping as well as end-index semantics.
    """
    # Bars strictly during normal trading hours — date derivation is identical
    ns_bars = [
        _ns(2025, 1, 6,  9, 30),
        _ns(2025, 1, 6, 10,  0),
        _ns(2025, 1, 6, 14, 25),
    ]
    bars_with_date = [_bar_with_date(ns, JAN6) for ns in ns_bars]
    bars_ns_only   = [_bar_ns_only(ns) for ns in ns_bars]

    dr_run   = backtest_run.build_dr(bars_with_date)
    dr_gate  = test_score_gate.build_dr(bars_with_date)
    dr_bot   = V106Scanner._build_dr(bars_ns_only)

    assert JAN6 in dr_run,  "backtest_run: JAN6 missing"
    assert JAN6 in dr_gate, "test_score_gate: JAN6 missing"
    assert JAN6 in dr_bot,  "ptnut_bot: JAN6 missing"

    assert dr_run[JAN6]  == (0, 3), f"backtest_run: {dr_run[JAN6]} != (0,3)"
    assert dr_gate[JAN6] == (0, 3), f"test_score_gate: {dr_gate[JAN6]} != (0,3)"
    assert dr_bot[JAN6]  == (0, 3), f"ptnut_bot: {dr_bot[JAN6]} != (0,3)"

    # All three agree
    assert dr_run[JAN6] == dr_gate[JAN6] == dr_bot[JAN6], (
        f"Implementations disagree: run={dr_run[JAN6]} gate={dr_gate[JAN6]} bot={dr_bot[JAN6]}"
    )
    print("PASS  test_all_implementations_agree")


# ──────────────────────────────────────────────────────────────
# 5. ptnut_bot scan() slice: b5[:de5] is correct with exclusive de5
# ──────────────────────────────────────────────────────────────

def test_b5_slice_uses_exclusive_end():
    """b5[:de5] with exclusive de5 = last+1 gives exactly the session bars.

    Before fix: b5[:de5+1] with exclusive de5 = b5[:last+2] — one bar too many.
    After fix:  b5[:de5]   with exclusive de5 = b5[:last+1] — exactly correct.
    """
    # 3-bar session, de5=3 (exclusive)
    bars = [_bar_ns_only(_ns(2025, 1, 6, h, 0)) for h in [9, 10, 11]]
    dr = V106Scanner._build_dr(bars)
    ds5, de5 = dr[JAN6]

    assert de5 == 3, f"de5 should be 3 (exclusive), got {de5}"

    # After fix: b5[:de5] = b5[:3] = [bars[0], bars[1], bars[2]] — all 3 session bars
    correct_slice = bars[:de5]
    assert len(correct_slice) == 3, f"b5[:de5] should have 3 bars, got {len(correct_slice)}"

    # Before fix would have been: b5[:de5+1] = b5[:4]
    # With only 3 bars in list, b5[:4] == b5[:3] (Python silently clips at list end)
    # but if there were a 4th bar from a different session it would be included — the real bug
    # Prove this with a 4-bar list where bar[3] is from the NEXT session
    bars4 = bars + [_bar_ns_only(_ns(2025, 1, 7, 9, 0))]   # 4th bar = Tuesday
    correct_slice4 = bars4[:de5]       # b5[:3] — correct: only Monday bars
    broken_slice4  = bars4[:de5 + 1]   # b5[:4] — wrong: includes Tuesday bar

    assert len(correct_slice4) == 3, "b5[:de5] should exclude the Tuesday bar"
    assert len(broken_slice4)  == 4, "b5[:de5+1] would have incorrectly included Tuesday bar"
    print("PASS  test_b5_slice_uses_exclusive_end")


# ──────────────────────────────────────────────────────────────
# 6. Cursor loop: range(ds5+1, de5) reaches last bar
# ──────────────────────────────────────────────────────────────

def test_cursor_loop_reaches_last_bar():
    """range(ds5+1, de5) with exclusive de5 must include the last bar index.

    Before fix: range(ds5+1, de5+1) with inclusive de5=last -> iterates to de5=last. Same.
    But after build_dr fix, de5=last+1. range(ds5+1, de5+1) = range(ds5+1, last+2)
    which goes ONE PAST the session. After cursor loop fix: range(ds5+1, de5) = correct.
    """
    bars = [_bar_with_date(_ns(2025, 1, 6, h, 0), JAN6) for h in [9, 10, 11, 12]]
    dr = backtest_run.build_dr(bars)
    ds5, de5 = dr[JAN6]

    # de5 must be exclusive (4 bars, last index=3, de5=4)
    assert de5 == 4, f"de5 should be 4 (exclusive), got {de5}"

    cursors = list(range(ds5 + 1, de5))  # [1, 2, 3]
    assert cursors == [1, 2, 3], f"Cursor range should be [1,2,3], got {cursors}"
    assert cursors[-1] == 3, f"Last cursor should be 3 (last bar index), got {cursors[-1]}"

    # Before fix would have been: range(ds5+1, de5+1) with OLD de5=3 (inclusive) -> [1,2,3] same
    # But with NEW de5=4 (exclusive): range(ds5+1, de5+1) = range(1, 5) = [1,2,3,4] — OUT OF BOUNDS
    broken_cursors = list(range(ds5 + 1, de5 + 1))  # [1, 2, 3, 4] — index 4 doesn't exist
    assert 4 in broken_cursors, "Without cursor fix, index 4 (out of bounds) would be attempted"
    print("PASS  test_cursor_loop_reaches_last_bar")


# ──────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_single_day_end_is_exclusive,
        test_range_ds_de_covers_all_bars,
        test_last_bar_included_in_range,
        test_multi_day_each_session_exclusive,
        test_all_implementations_agree,
        test_b5_slice_uses_exclusive_end,
        test_cursor_loop_reaches_last_bar,
    ]

    print(f"\nbuild_dr End-Index Parity — Patch 3 Tests\n{'='*50}")
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
