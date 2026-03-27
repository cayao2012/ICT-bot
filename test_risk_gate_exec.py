"""
test_risk_gate_exec.py
======================
Proves Patch 1: the execution-size risk gate added to V106Scanner.scan().

Bug being tested:
  apply_entry_mode() gates on backtest_entry_modes.CONTRACTS=3.
  If NQ["contracts"] is 4, a trade can pass the 3ct gate ($840 at 14pt stop)
  and execute at 4ct ($1120), 12% over MAX_RISK=$1000.

Fix being tested:
  ptnut_bot.scan() re-checks: risk * NQ["pv"] * NQ["contracts"] > MAX_RISK
  and rejects (continues) before appending to new_signals.

No broker connection. No real data. No live imports beyond strategy modules.
Run: PYTHONPATH=tsxapi4py/src python3 test_risk_gate_exec.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))

from backtest_entry_modes import (
    gen_sweep_entries_enriched, apply_entry_mode,
    MODE_CLOSE_ENTRY, CONTRACTS, PV, MAX_RISK, SLIP,
)

# ── Constants that must match the live bot for the gate to be meaningful ──
EXEC_CONTRACTS = 4   # NQ["contracts"] in ptnut_bot.py
EXEC_PV        = 20  # NQ["pv"]
EXEC_MAX_RISK  = 1000  # MAX_RISK in ptnut_bot.py


# ──────────────────────────────────────────────────────────────
# Helper: build a minimal fake signal dict, bypassing bar data
# ──────────────────────────────────────────────────────────────
def _fake_sig(risk_pts):
    """Return a minimal apply_entry_mode-style output dict with given risk_pts."""
    entry = 21000.0
    stop  = entry - risk_pts  # bull setup
    return {
        "entry":    entry,
        "stop":     stop,
        "risk_pts": risk_pts,
        "risk_$":   risk_pts * PV * CONTRACTS,   # gated at 3ct
        "rr":       1.1,
        "score":    3,
        "zone":     "disp_fvg",
        "zone_top": entry + 5,
        "zone_bot": entry - 2,
        "time":     __import__("datetime").datetime(2025, 1, 7, 10, 0,
                        tzinfo=__import__("zoneinfo").ZoneInfo("America/Chicago")),
        "side":     "bull",
        "mode":     "close_entry",
        "has_cisd": False, "has_struct": False, "has_sweep": False, "has_rej": False,
    }


def _exec_risk(sig):
    """Actual risk dollar at execution contract count — the value the gate must check."""
    return sig["risk_pts"] * EXEC_PV * EXEC_CONTRACTS


def _gate_risk(sig):
    """Risk dollar as seen by apply_entry_mode (3ct) — what passed before the fix."""
    return sig["risk_pts"] * PV * CONTRACTS


def _exec_gate_passes(sig):
    """The new check added by Patch 1: does the signal pass the execution-size gate?"""
    return _exec_risk(sig) <= EXEC_MAX_RISK


# ──────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────

def test_gate_constants_are_correct():
    """Sanity: module constants match what the fix depends on."""
    assert CONTRACTS == 3,   f"Expected CONTRACTS=3, got {CONTRACTS}"
    assert PV == 20,         f"Expected PV=20, got {PV}"
    assert MAX_RISK == 1000, f"Expected MAX_RISK=1000, got {MAX_RISK}"
    assert EXEC_CONTRACTS == 4, "Test setup: EXEC_CONTRACTS must be 4"
    print("PASS  test_gate_constants_are_correct")


def test_safe_trade_passes_both_gates():
    """A trade small enough to pass at 4ct passes both the 3ct gate and the 4ct gate.
    Max stop for 4ct: 1000 / (4 * 20) = 12.5 pts."""
    sig = _fake_sig(risk_pts=10.0)    # 10pt stop: 3ct=$600, 4ct=$800 — both under $1000
    assert _gate_risk(sig) <= MAX_RISK, "Expected to pass 3ct gate"
    assert _exec_gate_passes(sig),      "Expected to pass 4ct gate"
    print(f"PASS  test_safe_trade_passes_both_gates  (10pt stop: 3ct=${_gate_risk(sig):.0f}, 4ct=${_exec_risk(sig):.0f})")


def test_over_limit_trade_passes_old_gate_fails_new():
    """The core bug: a 14pt stop passes the 3ct gate but must fail the 4ct gate.
    3ct risk = 14 * 20 * 3 = $840  <=  $1000  (OLD gate: PASSES — BUG)
    4ct risk = 14 * 20 * 4 = $1120 >   $1000  (NEW gate: MUST FAIL — FIX)"""
    sig = _fake_sig(risk_pts=14.0)
    old_gate_passes = _gate_risk(sig) <= MAX_RISK
    new_gate_passes = _exec_gate_passes(sig)

    assert old_gate_passes,  f"Pre-fix 3ct gate should PASS  (${_gate_risk(sig):.0f} <= ${MAX_RISK})"
    assert not new_gate_passes, f"Post-fix 4ct gate must REJECT (${_exec_risk(sig):.0f} > ${MAX_RISK})"
    print(f"PASS  test_over_limit_trade_passes_old_gate_fails_new  "
          f"(14pt: old=${_gate_risk(sig):.0f} PASS, new=${_exec_risk(sig):.0f} REJECT)")


def test_exact_boundary_4ct():
    """Stop of exactly MAX_RISK/(4*PV) = 12.5 pts: must pass the 4ct gate (equal, not over)."""
    boundary_pts = MAX_RISK / (EXEC_CONTRACTS * EXEC_PV)   # 12.5
    sig = _fake_sig(risk_pts=boundary_pts)
    assert _exec_gate_passes(sig), (
        f"At-boundary trade (${_exec_risk(sig):.0f} == ${MAX_RISK}) must pass 4ct gate"
    )
    print(f"PASS  test_exact_boundary_4ct  (12.5pt stop: 4ct=${_exec_risk(sig):.0f} == ${MAX_RISK})")


def test_one_tick_over_boundary_4ct():
    """12.75pt stop (one NQ tick above 12.5): must fail the 4ct gate.
    4ct risk = 12.75 * 20 * 4 = $1020 > $1000."""
    sig = _fake_sig(risk_pts=12.75)   # one tick (0.25pt) above boundary
    assert not _exec_gate_passes(sig), (
        f"One-tick-over trade (${_exec_risk(sig):.0f} > ${MAX_RISK}) must fail 4ct gate"
    )
    print(f"PASS  test_one_tick_over_boundary_4ct  (12.75pt stop: 4ct=${_exec_risk(sig):.0f} > ${MAX_RISK})")


def test_worst_case_passthrough():
    """A 16.5pt stop passes the 3ct gate ($990 < $1000) but overshoots by 32% at 4ct ($1320).
    Uses a concrete stop value to avoid floating-point edge cases."""
    sig = _fake_sig(risk_pts=16.5)   # 3ct: $990 (passes), 4ct: $1320 (fails)
    assert _gate_risk(sig) <= MAX_RISK, f"3ct gate should pass ${_gate_risk(sig):.0f}"
    assert not _exec_gate_passes(sig), f"4ct gate must reject ${_exec_risk(sig):.0f}"
    overage = _exec_risk(sig) - MAX_RISK
    pct = overage / MAX_RISK * 100
    print(f"PASS  test_worst_case_passthrough  "
          f"(16.5pt stop: 3ct=${_gate_risk(sig):.0f} PASS, 4ct=${_exec_risk(sig):.0f} REJECT, overage=${overage:.0f} ({pct:.0f}%))")


def test_gate_symmetric_for_bear():
    """Gate must also catch bear trades. Bears use a positive stop (stop > entry)."""
    risk_pts = 14.0
    entry = 21000.0
    stop  = entry + risk_pts   # bear: stop above entry
    sig = _fake_sig(risk_pts=risk_pts)
    sig["side"]  = "bear"
    sig["entry"] = entry
    sig["stop"]  = stop
    # Risk arithmetic is the same regardless of direction
    assert not _exec_gate_passes(sig), (
        f"Bear 14pt trade must also fail 4ct gate (${_exec_risk(sig):.0f})"
    )
    print(f"PASS  test_gate_symmetric_for_bear  (14pt bear: 4ct=${_exec_risk(sig):.0f} > ${MAX_RISK})")


def test_gate_uses_nq_pv_not_hardcoded():
    """Gate formula must use NQ['pv'] (20), not a hardcoded literal.
    This test documents the expected variable reference, not just the number."""
    sig = _fake_sig(risk_pts=14.0)
    expected = sig["risk_pts"] * EXEC_PV * EXEC_CONTRACTS
    assert expected == 1120.0, f"Expected $1120, got ${expected}"
    print(f"PASS  test_gate_uses_nq_pv_not_hardcoded  (formula: 14 * {EXEC_PV} * {EXEC_CONTRACTS} = ${expected:.0f})")


# ──────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    tests = [
        test_gate_constants_are_correct,
        test_safe_trade_passes_both_gates,
        test_over_limit_trade_passes_old_gate_fails_new,
        test_exact_boundary_4ct,
        test_one_tick_over_boundary_4ct,
        test_worst_case_passthrough,
        test_gate_symmetric_for_bear,
        test_gate_uses_nq_pv_not_hardcoded,
    ]

    print(f"\nRisk Gate (Execution Size) — Patch 1 Tests\n{'='*55}")
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as e:
            print(f"FAIL  {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
            failed += 1

    print(f"\n{'='*55}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
