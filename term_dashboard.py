"""
PTNUT Terminal Dashboard — TradeZella style
python3 term_dashboard.py
"""
import json, os, time, math, subprocess, signal
from datetime import datetime, timedelta
from collections import defaultdict

from rich.console import Console, Group
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich.columns import Columns
from rich import box

STATE_FILE = "ptnut_state.json"
TRADE_LOG = "ptnut_trades.json"
LOG_DIR = "logs"
console = Console()

# ── helpers ──────────────────────────────────────────────────
def rj(path, default=None):
    try:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    except Exception:
        pass
    return default if default is not None else {}


def log_tail(n=10):
    today = datetime.now().strftime("%Y-%m-%d")
    # Try both log naming conventions
    lf = os.path.join(LOG_DIR, f"bot_{today}.log")
    if not os.path.exists(lf):
        lf = os.path.join(LOG_DIR, f"ptnut_{today}.log")
    if not os.path.exists(lf):
        return []
    try:
        with open(lf) as f:
            return f.readlines()[-n:]
    except Exception:
        return []


def log_tail_es(n=10):
    today = datetime.now().strftime("%Y-%m-%d")
    lf = os.path.join(LOG_DIR, f"es_bot_{today}.log")
    if not os.path.exists(lf):
        return []
    try:
        with open(lf) as f:
            return f.readlines()[-n:]
    except Exception:
        return []


def pnl_color(v):
    return "green" if v > 0 else "red" if v < 0 else "dim"


def sparkline(values, width=30, height=6):
    """Render a sparkline chart using block characters."""
    if not values:
        return "[dim]no data[/]"
    blocks = " ▁▂▃▄▅▆▇█"
    mn, mx = min(values), max(values)
    rng = mx - mn if mx != mn else 1
    # Resample to width
    if len(values) > width:
        step = len(values) / width
        sampled = [values[int(i * step)] for i in range(width)]
    else:
        sampled = values
    out = ""
    for v in sampled:
        idx = int((v - mn) / rng * (len(blocks) - 1))
        out += blocks[idx]
    return out


def bar_chart_v(values, labels, width=40, height=8):
    """Vertical bar chart using block characters."""
    if not values:
        return "[dim]no trades[/]"
    mx = max(abs(v) for v in values) if values else 1
    lines = []
    bw = max(1, min(3, width // max(len(values), 1)))
    # Build rows from top to bottom
    for row in range(height, 0, -1):
        threshold_pos = (row / height) * mx
        threshold_neg = ((height - row + 1) / height) * mx
        line = ""
        for v in values:
            if v >= 0 and v >= threshold_pos:
                line += f"[green]{'█' * bw}[/] "
            elif v < 0 and abs(v) >= threshold_neg:
                line += f"[red]{'█' * bw}[/] "
            else:
                line += " " * bw + " "
        lines.append(line)
    # Zero line
    lines.append("[dim]" + "─" * (len(values) * (bw + 1)) + "[/]")
    # Negative bars
    for row in range(1, height + 1):
        threshold = (row / height) * mx
        line = ""
        for v in values:
            if v < 0 and abs(v) >= threshold:
                line += f"[red]{'█' * bw}[/] "
            else:
                line += " " * bw + " "
        lines.append(line)
    return "\n".join(lines)


def win_ring(pct, size=5):
    """ASCII win rate ring using Unicode."""
    filled = int(pct / 100 * 8)
    empty = 8 - filled
    ring_chars = "●" * filled + "○" * empty
    color = "green" if pct >= 50 else "red" if pct > 0 else "dim"
    return f"[{color}]{ring_chars}[/]\n[bold {color}]{pct:.0f}%[/]"


def wl_circles(wins, losses):
    """Colored W/L count circles like TradeZella."""
    w_str = f"[bold green]({wins})[/]" if wins > 0 else f"[dim]({wins})[/]"
    l_str = f"[bold red]({losses})[/]" if losses > 0 else f"[dim]({losses})[/]"
    return f"{w_str}  {l_str}"


def period_stats(trades, label):
    """Calculate stats for a period's trades."""
    exits = [t for t in trades if t.get("type") == "EXIT"]
    wins = [e for e in exits if (e.get("pnl") or 0) > 0]
    losses = [e for e in exits if (e.get("pnl") or 0) <= 0]
    total_pnl = sum(e.get("pnl", 0) for e in exits)
    total_rr = sum((e.get("rr", 0) if e.get("pnl", 0) > 0 else -1) for e in exits) if exits else 0
    wr = len(wins) / (len(wins) + len(losses)) * 100 if (len(wins) + len(losses)) > 0 else 0
    nw, nl = len(wins), len(losses)

    pc = pnl_color(total_rr)
    rr_sign = "+" if total_rr >= 0 else ""
    pnl_sign = "+" if total_pnl >= 0 else ""

    ring = win_ring(wr)

    return Panel(
        f"[bold {pc}]{rr_sign}{total_rr:.2f}[/] [dim]R:R[/]      {ring}\n"
        f"[{pc}]{pnl_sign}{total_pnl / 100:.2f} %[/]\n"
        f"[bold {pc}]{pnl_sign}${total_pnl:,.0f}[/]\n\n"
        f"{wl_circles(nw, nl)}",
        title=f"[bold]{label}[/]",
        border_style="bright_black",
        box=box.ROUNDED,
        width=28,
        height=10,
    )


# ── main build ───────────────────────────────────────────────
def build():
    s = rj(STATE_FILE)
    all_trades = rj(TRADE_LOG, [])
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")

    # Period filters
    def in_period(t, days=None, month=False, year=False, all_time=False):
        d = t.get("date", "")
        if all_time:
            return True
        try:
            td = datetime.strptime(d, "%Y-%m-%d")
        except Exception:
            return False
        if days is not None:
            return td >= now - timedelta(days=days)
        if month:
            return td.year == now.year and td.month == now.month
        if year:
            return td.year == now.year
        return d == today

    today_trades = [t for t in all_trades if t.get("date") == today]
    week_trades = [t for t in all_trades if in_period(t, days=7)]
    month_trades = [t for t in all_trades if in_period(t, month=True)]
    year_trades = [t for t in all_trades if in_period(t, year=True)]
    all_time_trades = all_trades

    exits_today = [t for t in today_trades if t.get("type") == "EXIT"]
    sigs_today = [t for t in today_trades if t.get("type") in ("PAPER_SIGNAL", "ENTRY")]
    total_pnl = sum(e.get("pnl", 0) for e in exits_today)
    price = s.get("live_price", 0)

    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="stat_row", size=11),
        Layout(name="charts", size=13),
        Layout(name="trades", size=14),
        Layout(name="es_section", size=8),
        Layout(name="monthly", size=10),
    )

    # ── HEADER ───────────────────────────────────────────────
    mode = s.get("mode", "??")
    status = s.get("status", "?")
    dot = "[bold green]⬤[/]" if status == "running" else "[bold red]⬤[/]"
    mode_b = f"[bold white on red] {mode} [/]" if mode != "PAPER" else f"[bold black on yellow] {mode} [/]"
    ticks = s.get("quote_count", 0)
    kz = "[bold green]KZ ACTIVE[/]" if s.get("kz_active") else "[dim]KZ CLOSED[/]"

    header = Text.from_markup(
        f"  [bold cyan]PTNUT COMMAND CENTER[/]  {mode_b}  {dot}  "
        f"[bold cyan]NQ {price:,.2f}[/]  {kz}  "
        f"[dim]{s.get('time', '')}  {ticks:,} ticks[/]"
    )
    layout["header"].update(Panel(header, box=box.HEAVY, style=""))

    # ── STAT CARDS ROW ───────────────────────────────────────
    stat_cards = Layout(name="stats_inner")
    stat_cards.split_row(
        Layout(period_stats(today_trades, "TODAY"), name="s1"),
        Layout(period_stats(week_trades, "THIS WEEK"), name="s2"),
        Layout(period_stats(month_trades, "THIS MONTH"), name="s3"),
        Layout(period_stats(year_trades, "THIS YEAR"), name="s4"),
        Layout(name="acct", ratio=1),
    )

    # Account info + risk
    cl_bull = s.get("cl_bull", 0)
    cl_bear = s.get("cl_bear", 0)
    gc = s.get("gc", 0)
    dll = abs(min(s.get("pnl", 0), 0))

    def risk_bar(val, mx, w=12):
        filled = int(min(val / mx, 1.0) * w) if mx > 0 else 0
        pct = min(val / mx * 100, 100) if mx > 0 else 0
        c = "red" if pct >= 80 else "yellow" if pct >= 50 else "cyan"
        return f"[{c}]{'█' * filled}[/]{'░' * (w - filled)}"

    acct_text = (
        f"[bold cyan]Account[/]\n"
        f"[dim]CL Bull[/]  {risk_bar(cl_bull, 3)} [bold]{cl_bull}/3[/]\n"
        f"[dim]CL Bear[/]  {risk_bar(cl_bear, 3)} [bold]{cl_bear}/3[/]\n"
        f"[dim]GMCL  [/]  {risk_bar(gc, 5)} [bold]{gc}/5[/]\n"
        f"[dim]DLL   [/]  {risk_bar(dll, 2000)} [bold]${dll:,.0f}[/]\n"
        f"\n[dim]Signals:[/] [bold yellow]{len(sigs_today)}[/]"
    )
    stat_cards["acct"].update(Panel(acct_text, border_style="bright_black", box=box.ROUNDED))
    layout["stat_row"].update(stat_cards)

    # ── CHARTS ROW ───────────────────────────────────────────
    charts_layout = Layout(name="charts_inner")
    charts_layout.split_row(
        Layout(name="eq_chart"),
        Layout(name="rr_chart"),
        Layout(name="trade_cards", ratio=1),
    )

    # Equity curve sparkline
    all_exits = [t for t in all_trades if t.get("type") == "EXIT"]
    cum = 0
    eq_points = [0]
    for e in all_exits:
        cum += e.get("pnl", 0)
        eq_points.append(cum)

    spark = sparkline(eq_points, width=40)
    eq_color = pnl_color(cum)
    eq_text = (
        f"[dim]Equity[/]  [bold {eq_color}]${cum:,.0f}[/]\n\n"
        f"  [{eq_color}]{spark}[/]\n"
        f"  [dim]{'─' * 40}[/]\n"
        f"  [dim]{'oldest':<20}{'latest':>20}[/]"
    )
    charts_layout["eq_chart"].update(Panel(eq_text, title="[bold]ACCOUNT BALANCE[/]", border_style="bright_black", box=box.ROUNDED))

    # RR bar chart
    recent_exits = all_exits[-20:]  # last 20 trades
    rr_vals = []
    for e in recent_exits:
        pnl = e.get("pnl", 0)
        rr = e.get("rr", 0)
        rr_vals.append(rr if pnl > 0 else -1)

    if rr_vals:
        mx_rr = max(abs(v) for v in rr_vals) if rr_vals else 1
        rr_lines = []
        for row in range(4, 0, -1):
            line = ""
            for v in rr_vals:
                thresh = (row / 4) * mx_rr
                if v > 0 and v >= thresh:
                    line += "[green]█[/]"
                else:
                    line += " "
            rr_lines.append(f"  {line}")
        rr_lines.append(f"  [dim]{'─' * len(rr_vals)}[/]")
        for row in range(1, 3):
            line = ""
            for v in rr_vals:
                thresh = (row / 2) * mx_rr
                if v < 0 and abs(v) >= thresh:
                    line += "[red]█[/]"
                else:
                    line += " "
            rr_lines.append(f"  {line}")
        total_rr = sum(rr_vals)
        rr_text = f"[dim]Total:[/] [bold {pnl_color(total_rr)}]{total_rr:+.2f} R:R[/]\n" + "\n".join(rr_lines)
    else:
        rr_text = "[dim]no trades yet[/]"

    charts_layout["rr_chart"].update(Panel(rr_text, title="[bold]REWARD : RISK[/]", border_style="bright_black", box=box.ROUNDED))

    # Trade cards (right sidebar)
    recent = list(reversed(today_trades))[:6]
    card_parts = []
    for t in recent:
        tp = t.get("type", "")
        side = t.get("side", "?")
        side_s = "[green]LONG[/]" if side == "bull" else "[red]SHORT[/]"
        sc = t.get("score", 0)
        zone = t.get("zone_type", "")
        tm = (t.get("time", "") or "")[-8:]

        if tp == "PAPER_SIGNAL":
            tag = "[yellow]SIG[/]"
            val = f"[magenta]{t.get('rr', '?')}R[/]"
        elif tp == "ENTRY":
            tag = "[cyan]FILL[/]"
            val = f"[magenta]{t.get('rr', '?')}R[/]"
        elif tp == "EXIT":
            p = t.get("pnl", 0)
            tag = "[green]WIN[/]" if p > 0 else "[red]LOSS[/]"
            val = f"[{pnl_color(p)}]${p:+,.0f}[/]"
        else:
            continue

        card_parts.append(f"  {side_s} {tag}  {val}  [dim]{zone} {tm}[/]")

    if not card_parts:
        card_parts = ["  [dim]waiting for signals...[/]"]

    # Position info at top of cards if active
    pos_text = ""
    if s.get("in_position") and s.get("position"):
        p = s["position"]
        side = p.get("side", "?")
        ss = "[bold green]▲ LONG[/]" if side == "bull" else "[bold red]▼ SHORT[/]"
        entry = p.get("entry", 0)
        stop = p.get("stop", 0)
        target = p.get("target", 0)
        rp = abs(entry - stop)
        rwp = abs(target - entry)
        unreal = 0
        if price > 0:
            unreal = (price - entry) * 60 if side == "bull" else (entry - price) * 60
        uc = pnl_color(unreal)
        pos_text = (
            f"  {ss} [dim]@[/][cyan]{entry:,.2f}[/]\n"
            f"  [red]SL {stop:,.2f}[/] [green]TP {target:,.2f}[/]\n"
            f"  [dim]Risk[/] {rp:.1f}p  [dim]Rwd[/] {rwp:.1f}p  [magenta]{p.get('rr','?')}R[/]\n"
            f"  [bold {uc}]Unreal: ${unreal:+,.0f}[/]\n"
            f"  [dim]{'─' * 30}[/]\n"
        )

    cards_text = pos_text + "\n".join(card_parts)
    charts_layout["trade_cards"].update(Panel(cards_text, title="[bold]TRADES[/]", border_style="bright_black", box=box.ROUNDED))

    layout["charts"].update(charts_layout)

    # ── TRADES TABLE ─────────────────────────────────────────
    table = Table(box=box.SIMPLE_HEAVY, expand=True, show_edge=False, pad_edge=True)
    table.add_column("Time", style="dim", width=8)
    table.add_column("Type", width=5)
    table.add_column("Side", width=5)
    table.add_column("Entry", justify="right", width=10)
    table.add_column("Stop", justify="right", style="red", width=10)
    table.add_column("Target", justify="right", style="green", width=10)
    table.add_column("Risk", justify="right", width=14)
    table.add_column("Reward", justify="right", width=14)
    table.add_column("RR", justify="center", style="magenta bold", width=5)
    table.add_column("Sc", justify="center", width=4)
    table.add_column("Zone", style="yellow", width=8)
    table.add_column("P&L", justify="right", width=10)

    for t in list(reversed(today_trades))[:10]:
        tp = t.get("type", "")
        side = t.get("side", "?")
        side_s = "[green]LONG[/]" if side == "bull" else "[red]SHRT[/]"
        sc = t.get("score", 0)
        sc_c = "green" if sc >= 5 else "cyan" if sc >= 3 else "yellow"

        if tp == "PAPER_SIGNAL":
            badge = "[yellow]SIG[/]"
            rk = f"{t.get('risk_pts', 0):.1f}p ${t.get('risk_dollar', 0)}"
            rw = f"{t.get('reward_pts', 0):.1f}p ${t.get('reward_dollar', 0)}"
            pl = "[dim]--[/]"
        elif tp == "ENTRY":
            badge = "[cyan]FILL[/]"
            rk = f"{t.get('risk_pts', 0):.1f}p ${t.get('risk_dollar', 0)}"
            rw = f"{t.get('reward_pts', 0):.1f}p ${t.get('reward_dollar', 0)}"
            pl = "[dim]--[/]"
        elif tp == "EXIT":
            pv = t.get("pnl", 0)
            badge = "[green]WIN[/]" if pv > 0 else "[red]LOSS[/]"
            rk = "--"
            rw = "--"
            pl = f"[bold {pnl_color(pv)}]${pv:+,.0f}[/]"
        else:
            continue

        table.add_row(
            (t.get("time", "") or "")[-8:], badge, side_s,
            f"{t.get('entry', 0):,.2f}", f"{t.get('stop', 0):,.2f}", f"{t.get('target', 0):,.2f}",
            rk, rw, str(t.get("rr", "")), f"[{sc_c}]{sc}[/]",
            t.get("zone_type", ""), pl,
        )

    if not today_trades:
        table.add_row(*["[dim]waiting...[/]"] + [""] * 11)

    layout["trades"].update(Panel(table, title="[bold]TODAY'S TRADES — NQ[/]", border_style="bright_black", box=box.ROUNDED))

    # ── ES SIGNAL BOT ─────────────────────────────────────────
    es_lines = log_tail_es(6)
    es_text = ""
    es_signals = 0
    for line in es_lines:
        line = line.strip()
        if "BEAR" in line or "SELL" in line:
            es_text += f"[bold red]▼ {line}[/]\n"
            es_signals += 1
        elif "BULL" in line or "BUY" in line:
            es_text += f"[bold green]▲ {line}[/]\n"
            es_signals += 1
        elif "SIGNAL" in line or "ENTRY" in line:
            es_text += f"[yellow]{line}[/]\n"
            es_signals += 1
        elif "ERROR" in line:
            es_text += f"[red]{line}[/]\n"
        elif line:
            es_text += f"[dim]{line}[/]\n"
    if not es_text:
        es_text = "[dim]waiting for ES signals...[/]"

    # Check if ES bot is alive
    es_alive = False
    try:
        import subprocess as _sp
        es_alive = _sp.run(["pgrep", "-f", "es_signal_bot.py"], capture_output=True).returncode == 0
    except Exception:
        pass
    es_dot = "[bold green]⬤[/]" if es_alive else "[bold red]⬤[/]"
    es_header = f"  {es_dot} [bold yellow]ES SIGNAL BOT[/]  [dim]Alerts Only → Telegram[/]  [dim]Signals: {es_signals}[/]"

    layout["es_section"].update(Panel(
        f"{es_header}\n{es_text.rstrip()}",
        title="[bold]ES — ALERTS[/]",
        border_style="yellow",
        box=box.ROUNDED,
    ))

    # ── MONTHLY STATS ────────────────────────────────────────
    mtable = Table(box=box.SIMPLE_HEAVY, expand=True, show_edge=False)
    mtable.add_column("Month", style="bold", width=8)
    mtable.add_column("R:R", justify="right", width=10)
    mtable.add_column("P&L", justify="right", width=12)
    mtable.add_column("Win%", justify="right", width=8)
    mtable.add_column("Trades", justify="right", width=7)

    monthly = defaultdict(list)
    for t in all_trades:
        if t.get("type") == "EXIT":
            d = t.get("date", "")[:7]  # YYYY-MM
            if d:
                monthly[d].append(t)

    for month_key in sorted(monthly.keys(), reverse=True)[:6]:
        me = monthly[month_key]
        mw = [e for e in me if (e.get("pnl") or 0) > 0]
        ml = [e for e in me if (e.get("pnl") or 0) <= 0]
        mp = sum(e.get("pnl", 0) for e in me)
        mr = sum((e.get("rr", 0) if e.get("pnl", 0) > 0 else -1) for e in me)
        mwr = len(mw) / (len(mw) + len(ml)) * 100 if (len(mw) + len(ml)) > 0 else 0
        pc = pnl_color(mp)
        mtable.add_row(
            month_key,
            f"[{pc}]{mr:+.2f}[/]",
            f"[{pc}]${mp:+,.0f}[/]",
            f"[{pc}]{mwr:.0f}%[/]",
            str(len(me)),
        )

    if not monthly:
        mtable.add_row(*["[dim]no history[/]"] + [""] * 4)

    # NQ log lines
    lines = log_tail(4)
    log_text = "[bold cyan]NQ BOT[/]\n"
    for line in lines:
        line = line.strip()
        if "SIGNAL" in line or "ENTRY" in line:
            log_text += f"[cyan]{line}[/]\n"
        elif "ERROR" in line:
            log_text += f"[red]{line}[/]\n"
        elif "WIN" in line or "EXIT" in line:
            log_text += f"[green]{line}[/]\n"
        else:
            log_text += f"[dim]{line}[/]\n"

    # ES signal bot log lines
    es_lines = log_tail_es(4)
    log_text += "\n[bold yellow]ES SIGNALS[/]\n"
    for line in es_lines:
        line = line.strip()
        if "SIGNAL" in line or "ENTRY" in line or "BEAR" in line or "BULL" in line:
            log_text += f"[yellow]{line}[/]\n"
        elif "ERROR" in line:
            log_text += f"[red]{line}[/]\n"
        else:
            log_text += f"[dim]{line}[/]\n"
    if not lines and not es_lines:
        log_text = "[dim]no logs[/]"

    bottom = Layout(name="bottom")
    bottom.split_row(
        Layout(Panel(mtable, title="[bold]MONTHLY STATS[/]", border_style="bright_black", box=box.ROUNDED), ratio=2),
        Layout(Panel(log_text.rstrip(), title="[bold]LOG[/]", border_style="bright_black", box=box.ROUNDED), ratio=1),
    )
    layout["monthly"].update(bottom)

    return layout


def _start_bot(bot_dir, script_name, log_prefix):
    """Start a bot process if it's not already running."""
    try:
        result = subprocess.run(["pgrep", "-f", script_name], capture_output=True)
        if result.returncode == 0:
            return  # already running
    except Exception:
        pass
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.join(bot_dir, "tsxapi4py", "src") + ":" + env.get("PYTHONPATH", "")
    log_dir = os.path.join(bot_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{log_prefix}_{datetime.now().strftime('%Y-%m-%d')}.log")
    with open(log_file, "a") as lf:
        subprocess.Popen(
            ["/usr/bin/python3", "-u", os.path.join(bot_dir, script_name)],
            stdout=lf, stderr=lf, cwd=bot_dir, env=env,
        )


def ensure_bots_running():
    """Start NQ trading bot + ES signal bot if not already running."""
    bot_dir = os.path.dirname(os.path.abspath(__file__))
    # NQ bot (auto-trades)
    start_script = os.path.expanduser("~/.tradingbot/start_bot.sh")
    try:
        result = subprocess.run(["pgrep", "-f", "ptnut_bot.py"], capture_output=True)
        if result.returncode != 0:
            if os.path.exists(start_script):
                subprocess.Popen(["bash", start_script], cwd=bot_dir)
            else:
                _start_bot(bot_dir, "ptnut_bot.py", "bot")
    except Exception:
        pass
    # ES signal bot (Telegram alerts)
    _start_bot(bot_dir, "es_signal_bot.py", "es_bot")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    ensure_bots_running()
    console.clear()

    bot_proc = None  # track so we can clean up

    def shutdown(sig, frame):
        console.clear()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, shutdown)

    with Live(build(), console=console, refresh_per_second=1, screen=True) as live:
        while True:
            time.sleep(3)
            live.update(build())
