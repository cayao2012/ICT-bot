#!/usr/bin/env python3
"""
Build 1m, 5m, and 15m bars from tick data for the period after Jan 29, 2026,
and merge them into the existing bars_cache.pkl.
"""

import numpy as np
import pickle
import shutil
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
import os
import sys
import time as time_module

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_new")
CT = ZoneInfo("America/Chicago")

def main():
    t0 = time_module.time()

    # ── Step 0: Backup existing bars_cache.pkl ──
    cache_path = os.path.join(DATA_DIR, "bars_cache.pkl")
    backup_path = os.path.join(DATA_DIR, "bars_cache_backup.pkl")
    print(f"Backing up {cache_path} -> {backup_path}")
    shutil.copy2(cache_path, backup_path)
    print("  Backup done.")

    # ── Step 1: Load existing cache and inspect ──
    print("\nLoading existing bars_cache.pkl...")
    with open(cache_path, "rb") as f:
        cache = pickle.load(f)

    print(f"  Keys: {list(cache.keys())}")
    for k in cache:
        v = cache[k]
        print(f"  {k}: {len(v)} bars")
        if len(v) > 0:
            print(f"    first: time_ns={v[0]['time_ns']}, date={v[0]['date']}, {v[0]['hour']:02d}:{v[0]['minute']:02d}")
            print(f"    last:  time_ns={v[-1]['time_ns']}, date={v[-1]['date']}, {v[-1]['hour']:02d}:{v[-1]['minute']:02d}")

    # Determine cutoff: after Jan 29, 2026 17:59 CT
    # That means we want ticks from Jan 29, 2026 18:00:00 CT onward
    cutoff_dt = datetime(2026, 1, 29, 18, 0, 0, tzinfo=CT)
    cutoff_ns = int(cutoff_dt.timestamp() * 1_000_000_000)
    print(f"\nCutoff: {cutoff_dt} = {cutoff_ns} ns")

    # ── Step 2: Load tick data and filter ──
    print("\nLoading tick arrays (memory-mapped)...")
    prices_mmap = np.load(os.path.join(DATA_DIR, "NQ_prices.npy"), mmap_mode="r")
    times_mmap = np.load(os.path.join(DATA_DIR, "NQ_times.npy"), mmap_mode="r")
    total_ticks = len(prices_mmap)
    print(f"  Total ticks: {total_ticks:,}")

    # Binary search for the cutoff point (times are sorted)
    print("  Finding cutoff index via binary search...")
    cutoff_idx = np.searchsorted(times_mmap, cutoff_ns, side="left")
    n_new = total_ticks - cutoff_idx
    print(f"  Cutoff index: {cutoff_idx:,}")
    print(f"  New ticks to process: {n_new:,}")

    if n_new == 0:
        print("No new ticks found after cutoff. Exiting.")
        return

    # Load only the new ticks into memory
    print("  Loading new tick slice into memory...")
    prices = np.array(prices_mmap[cutoff_idx:], dtype=np.float64)
    times_ns = np.array(times_mmap[cutoff_idx:], dtype=np.int64)
    del prices_mmap, times_mmap
    print(f"  Loaded {len(prices):,} ticks")

    # Verify first/last tick
    first_ts = datetime.fromtimestamp(times_ns[0] / 1e9, tz=CT)
    last_ts = datetime.fromtimestamp(times_ns[-1] / 1e9, tz=CT)
    print(f"  First tick: {first_ts}")
    print(f"  Last tick:  {last_ts}")

    # ── Step 3: Build bars ──

    def build_bars(tick_prices, tick_times_ns, interval_minutes):
        """
        Build OHLC bars from ticks at the given interval.
        Uses vectorized numpy operations for speed.
        """
        # Convert tick times from ns to seconds (float64)
        tick_times_sec = tick_times_ns / 1e9

        # We need to compute the floored bar start time for each tick in CT.
        # Strategy: convert to CT-aware datetimes, floor to interval boundary.
        # For efficiency, we'll compute the UTC offset for each tick and work in
        # epoch seconds. CT offset changes with DST, so we need to handle that.

        # Get unique dates to determine DST transitions
        # First, get rough day boundaries
        min_sec = tick_times_sec[0]
        max_sec = tick_times_sec[-1]

        # Build a mapping of UTC offset transitions in the time range
        # Check every day boundary
        from datetime import timezone
        start_day = datetime.fromtimestamp(min_sec, tz=CT).replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = datetime.fromtimestamp(max_sec, tz=CT).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=2)

        # Build transition table: list of (epoch_sec, utc_offset_seconds)
        transitions = []
        current = start_day
        prev_offset = None
        while current <= end_day:
            offset_sec = current.utcoffset().total_seconds()
            if offset_sec != prev_offset:
                transitions.append((current.timestamp(), offset_sec))
                prev_offset = offset_sec
            current += timedelta(hours=1)  # check hourly for DST

        # For each tick, determine its CT offset
        # Build array of CT-adjusted epoch seconds
        ct_adjusted_sec = np.empty_like(tick_times_sec)

        # Apply offsets in segments
        for i in range(len(transitions)):
            start_ts = transitions[i][0]
            offset = transitions[i][1]
            if i + 1 < len(transitions):
                end_ts = transitions[i + 1][0]
                mask = (tick_times_sec >= start_ts) & (tick_times_sec < end_ts)
            else:
                mask = tick_times_sec >= start_ts
            ct_adjusted_sec[mask] = tick_times_sec[mask] + offset

        # Now floor to interval boundary
        # ct_adjusted_sec represents "local CT time as if it were UTC"
        # Floor: subtract (seconds_since_midnight % (interval * 60)), keeping date part

        # Seconds since midnight in CT
        day_start_sec = (ct_adjusted_sec // 86400) * 86400  # floor to day
        time_of_day_sec = ct_adjusted_sec - day_start_sec
        interval_sec = interval_minutes * 60
        floored_time_of_day = (time_of_day_sec // interval_sec) * interval_sec
        floored_ct_sec = day_start_sec + floored_time_of_day

        # Convert back to actual UTC epoch (subtract offset)
        # We need to map back. The floored CT time corresponds to a bar start in CT.
        # bar_start_utc = floored_ct_sec - offset
        # We stored offset per tick already, so:
        bar_start_utc_sec = np.empty_like(tick_times_sec)
        for i in range(len(transitions)):
            start_ts = transitions[i][0]
            offset = transitions[i][1]
            if i + 1 < len(transitions):
                end_ts = transitions[i + 1][0]
                mask = (tick_times_sec >= start_ts) & (tick_times_sec < end_ts)
            else:
                mask = tick_times_sec >= start_ts
            bar_start_utc_sec[mask] = floored_ct_sec[mask] - offset

        # Convert bar start times to ns for the bar dict
        bar_start_ns = (bar_start_utc_sec * 1e9).astype(np.int64)

        # Group by bar_start_ns and compute OHLC
        # Find unique bar keys and their boundaries
        unique_keys, inverse, counts = np.unique(bar_start_ns, return_inverse=True, return_counts=True)

        bars = []
        # Use a cumulative approach for OHLC
        # Sort is already done by time (ticks are chronological)
        # For each unique bar key, find the slice of ticks

        # Build index boundaries
        # Since ticks are chronological and bar keys are mostly monotonic (except DST),
        # we can use the inverse mapping

        # Group indices by bar key
        # More efficient: iterate through unique keys with start/end positions
        idx_sorted = np.argsort(inverse, kind='stable')
        key_starts = np.zeros(len(unique_keys) + 1, dtype=np.int64)
        np.cumsum(counts, out=key_starts[1:])

        for i in range(len(unique_keys)):
            s = key_starts[i]
            e = key_starts[i + 1]
            idxs = idx_sorted[s:e]

            # Since ticks are chronological, get min/max idx for open/close
            # But idxs might not be sorted if argsort reordered within same key
            # Actually with stable sort and chronological data mapping to same key, they should be in order
            bar_prices = tick_prices[idxs]

            bar_ns = int(unique_keys[i])
            bar_dt = datetime.fromtimestamp(bar_ns / 1e9, tz=CT)

            # Trading date: if hour >= 17, date = next calendar day
            bar_date = bar_dt.date()
            if bar_dt.hour >= 17:
                bar_date = bar_date + timedelta(days=1)

            bars.append({
                "time_ns": bar_ns,
                "open": float(bar_prices[0]),
                "high": float(np.max(bar_prices)),
                "low": float(np.min(bar_prices)),
                "close": float(bar_prices[-1]),
                "date": bar_date,
                "hour": bar_dt.hour,
                "minute": bar_dt.minute,
            })

        # Sort by time_ns just in case
        bars.sort(key=lambda b: b["time_ns"])
        return bars

    print("\nBuilding 1m bars...")
    t1 = time_module.time()
    bars_1m = build_bars(prices, times_ns, 1)
    print(f"  Built {len(bars_1m)} bars in {time_module.time()-t1:.1f}s")

    print("Building 5m bars...")
    t1 = time_module.time()
    bars_5m = build_bars(prices, times_ns, 5)
    print(f"  Built {len(bars_5m)} bars in {time_module.time()-t1:.1f}s")

    print("Building 15m bars...")
    t1 = time_module.time()
    bars_15m = build_bars(prices, times_ns, 15)
    print(f"  Built {len(bars_15m)} bars in {time_module.time()-t1:.1f}s")

    # Free tick memory
    del prices, times_ns

    # Print sample bars
    for label, bars_list in [("1m", bars_1m), ("5m", bars_5m), ("15m", bars_15m)]:
        print(f"\n  {label} first 3 bars:")
        for b in bars_list[:3]:
            print(f"    {b['date']} {b['hour']:02d}:{b['minute']:02d} O={b['open']:.2f} H={b['high']:.2f} L={b['low']:.2f} C={b['close']:.2f}")
        print(f"  {label} last 3 bars:")
        for b in bars_list[-3:]:
            print(f"    {b['date']} {b['hour']:02d}:{b['minute']:02d} O={b['open']:.2f} H={b['high']:.2f} L={b['low']:.2f} C={b['close']:.2f}")

    # ── Step 4: Merge with existing cache ──
    print("\nMerging with existing cache...")

    # Get the last time_ns from existing bars to avoid duplicates
    key_map = {"b1": bars_1m, "b5": bars_5m, "b15": bars_15m}

    for key, new_bars in key_map.items():
        existing = cache[key]
        if len(existing) > 0:
            last_existing_ns = existing[-1]["time_ns"]
            # Filter new bars to only those AFTER the last existing bar
            new_bars_filtered = [b for b in new_bars if b["time_ns"] > last_existing_ns]
            print(f"  {key}: {len(existing)} existing + {len(new_bars_filtered)} new = {len(existing) + len(new_bars_filtered)} total")
            if len(new_bars_filtered) > 0:
                first_new = new_bars_filtered[0]
                last_new = new_bars_filtered[-1]
                print(f"    New range: {first_new['date']} {first_new['hour']:02d}:{first_new['minute']:02d} -> {last_new['date']} {last_new['hour']:02d}:{last_new['minute']:02d}")
            cache[key] = existing + new_bars_filtered
        else:
            print(f"  {key}: 0 existing + {len(new_bars)} new = {len(new_bars)} total")
            cache[key] = new_bars

    # ── Step 5: Save ──
    print(f"\nSaving merged cache to {cache_path}...")
    with open(cache_path, "wb") as f:
        pickle.dump(cache, f, protocol=pickle.HIGHEST_PROTOCOL)

    new_size = os.path.getsize(cache_path)
    backup_size = os.path.getsize(backup_path)
    print(f"  Old size: {backup_size / 1e6:.1f} MB")
    print(f"  New size: {new_size / 1e6:.1f} MB")

    print(f"\nDone in {time_module.time()-t0:.1f}s total.")


if __name__ == "__main__":
    main()
