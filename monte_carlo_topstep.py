"""
Monte Carlo — TopstepX Pass/Blow Probability
============================================
Simulates the trailing MLL (Maximum Loss Limit) mechanic:
 - MLL floor starts at starting_balance - mll_buffer
 - After each trading day, if EOD equity is a new all-time high,
   MLL floor trails up by the same amount (never goes down)
 - MLL locks at starting_balance once EOD high - mll_buffer >= starting_balance
 - MLL is enforced intraday: if running equity hits floor mid-day → blown

Strategy stats from 4yr backtest at 1.0 RR:
  WR: 74.1%, avg win $622, avg loss $-697, ~7.4 trades/day
"""
import numpy as np

np.random.seed(42)

# ── Strategy stats (from 4yr backtest at 1.0 RR) ──
WIN_RATE      = 0.741
AVG_WIN       = 622.0
AVG_LOSS      = -697.0
TRADES_PER_DAY = 7587 / 1022  # 7.42 avg

# Win/loss std dev (estimated from distribution — wins/losses vary)
WIN_STD  = 250.0
LOSS_STD = 200.0

# ── Account parameters ──
STARTING  = 50_000
MLL_BUF   = 2_000   # MLL is this many $ below EOD high
PASS_TARGET = 3_000  # profit needed to pass combine ($53K)

N_SIMS   = 100_000
MAX_DAYS = 120       # max days before we call it "stuck"


def simulate_one(starting=STARTING, mll_buf=MLL_BUF, pass_target=PASS_TARGET,
                  max_days=MAX_DAYS):
    """
    Returns: ('pass', days) or ('blow', days)
    Tracks trailing MLL enforced intraday (after each trade).
    """
    equity    = starting
    eod_high  = starting
    mll_floor = starting - mll_buf
    locked    = False

    for day in range(max_days):
        # Randomize number of trades today (Poisson around daily mean)
        n_trades = max(1, np.random.poisson(TRADES_PER_DAY))

        for _ in range(n_trades):
            if np.random.random() < WIN_RATE:
                pnl = max(1, np.random.normal(AVG_WIN, WIN_STD))
            else:
                pnl = min(-1, np.random.normal(AVG_LOSS, LOSS_STD))

            equity += pnl

            # Intraday MLL check
            if equity <= mll_floor:
                return ('blow', day + 1)

            # Pass check (hit profit target)
            if equity >= starting + pass_target:
                return ('pass', day + 1)

        # EOD: update trailing MLL
        if not locked:
            if equity > eod_high:
                eod_high = equity
                new_floor = eod_high - mll_buf
                if new_floor >= starting:
                    mll_floor = starting
                    locked = True
                else:
                    mll_floor = new_floor

    return ('timeout', max_days)


def run_scenario(label, starting=STARTING, mll_buf=MLL_BUF,
                 pass_target=PASS_TARGET):
    results = [simulate_one(starting, mll_buf, pass_target) for _ in range(N_SIMS)]
    passed  = [r for r in results if r[0] == 'pass']
    blown   = [r for r in results if r[0] == 'blow']
    timeout = [r for r in results if r[0] == 'timeout']

    pass_pct    = len(passed)  / N_SIMS * 100
    blow_pct    = len(blown)   / N_SIMS * 100
    timeout_pct = len(timeout) / N_SIMS * 100

    avg_pass_days = np.mean([r[1] for r in passed])  if passed  else 0
    avg_blow_days = np.mean([r[1] for r in blown])   if blown   else 0

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  Account: ${starting:,} | MLL: ${mll_buf:,} | Target: +${pass_target:,}")
    print(f"{'='*60}")
    print(f"  PASS:    {pass_pct:6.1f}%   avg {avg_pass_days:.1f} days to pass")
    print(f"  BLOW:    {blow_pct:6.1f}%   avg {avg_blow_days:.1f} days until blown")
    print(f"  TIMEOUT: {timeout_pct:6.1f}%   (>{MAX_DAYS} days, no result)")

    # Percentiles for days-to-pass
    if passed:
        days = sorted(r[1] for r in passed)
        p25 = days[int(len(days)*0.25)]
        p50 = days[int(len(days)*0.50)]
        p75 = days[int(len(days)*0.75)]
        print(f"  Days to pass (of passing sims): p25={p25} p50={p50} p75={p75}")


if __name__ == "__main__":
    print(f"Monte Carlo — TopstepX Pass/Blow Probability")
    print(f"Strategy: WR={WIN_RATE*100:.1f}% | AvgW=${AVG_WIN:.0f} | AvgL=${AVG_LOSS:.0f} | {TRADES_PER_DAY:.1f} trades/day")
    print(f"Simulations: {N_SIMS:,}")

    # Scenario 1: Pass the $50K Combine ($3K profit target, $2K MLL)
    run_scenario("$50K COMBINE — Pass $3K target, $2K MLL",
                 starting=50_000, mll_buf=2_000, pass_target=3_000)

    # Scenario 2: Build $5K buffer on funded (MLL already locked at $50K)
    # Now MLL is static at $50K, need to reach $55K
    run_scenario("$50K FUNDED — Build $5K buffer (MLL locked at $50K)",
                 starting=52_000, mll_buf=2_000, pass_target=5_000)

    # Scenario 3: $100K account ($6K target, $3K MLL)
    run_scenario("$100K COMBINE — Pass $6K target, $3K MLL",
                 starting=100_000, mll_buf=3_000, pass_target=6_000)
