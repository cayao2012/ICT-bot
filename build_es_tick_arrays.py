#!/usr/bin/env python3
"""
Build ES tick arrays (ES_prices.npy, ES_times.npy) from dbn.zst files.

Two types of source files:
1. Per-contract: glbx-mdp3-YYYYMMDD.trades.ESH5.dbn.zst (already ES-specific)
2. Parent symbol: glbx-mdp3-YYYYMMDD.trades.dbn.zst (contains all instruments, filter to ES)

For each day, picks the highest-volume ES outright contract (front month).

Output:
- ES_prices.npy (float64) - tick prices sorted by ts_recv
- ES_times.npy  (int64)   - ts_recv in nanoseconds
"""

import os
import re
import glob
import time
import numpy as np
import databento as db

DATA_DIR = "/Users/gtrades/ptnut-bot/data_new_raw"
OUT_DIR  = "/Users/gtrades/ptnut-bot/data_es"

# Valid ES price range (2022-2026)
ES_PRICE_MIN = 3000.0
ES_PRICE_MAX = 8000.0

# Create output directory
os.makedirs(OUT_DIR, exist_ok=True)

# ── Step 1: Discover files ──────────────────────────────────────────────────

# Per-contract ES files (ESH5, ESM5, etc. — no spreads)
per_contract_pattern = os.path.join(DATA_DIR, "glbx-mdp3-*.trades.ES??.dbn.zst")
per_contract_files = sorted(glob.glob(per_contract_pattern))
# Filter out spread files (contain dash like ESH5-ESM5)
per_contract_files = [f for f in per_contract_files if "-ES" not in os.path.basename(f).split(".trades.")[1]]
print(f"Per-contract ES files found: {len(per_contract_files)}")

# Parent symbol files
parent_pattern = os.path.join(DATA_DIR, "glbx-mdp3-*.trades.dbn.zst")
parent_files = sorted(glob.glob(parent_pattern))
print(f"Parent symbol files found: {len(parent_files)}")

# Group per-contract files by date, pick highest-volume contract per date
date_to_per_contract = {}
for f in per_contract_files:
    m = re.search(r"glbx-mdp3-(\d{8})\.trades\.(ES[A-Z]\d)\.dbn\.zst$", os.path.basename(f))
    if m:
        date_str = m.group(1)
        if date_str not in date_to_per_contract:
            date_to_per_contract[date_str] = []
        date_to_per_contract[date_str].append(f)

per_contract_dates = set(date_to_per_contract.keys())
print(f"Unique dates in per-contract files: {len(per_contract_dates)}")
if per_contract_dates:
    print(f"  Range: {min(per_contract_dates)} - {max(per_contract_dates)}")

parent_dates = set()
for f in parent_files:
    m = re.search(r"glbx-mdp3-(\d{8})\.trades\.dbn\.zst$", os.path.basename(f))
    if m:
        parent_dates.add(m.group(1))

print(f"Unique dates in parent files: {len(parent_dates)}")
if parent_dates:
    print(f"  Range: {min(parent_dates)} - {max(parent_dates)}")

# Only use parent files for dates NOT covered by per-contract
new_parent_dates = parent_dates - per_contract_dates
print(f"Parent-only dates (not in per-contract): {len(new_parent_dates)}")

parent_files_to_use = []
for f in parent_files:
    m = re.search(r"glbx-mdp3-(\d{8})\.trades\.dbn\.zst$", os.path.basename(f))
    if m and m.group(1) in new_parent_dates:
        parent_files_to_use.append(f)
parent_files_to_use.sort()
print(f"Parent files to process: {len(parent_files_to_use)}")

# Total unique dates
all_dates = sorted(per_contract_dates | new_parent_dates)
print(f"\nTotal unique dates: {len(all_dates)}")
print(f"Overall range: {all_dates[0]} - {all_dates[-1]}")
print("=" * 60)

# ── Step 2: Process files ───────────────────────────────────────────────────

all_prices = []
all_times = []
total_ticks = 0
filtered_ticks = 0
processed = 0
errors = []
t0 = time.time()

# Process per-contract dates (pick highest-volume contract per date)
print("\n── Processing per-contract ES files ──")
for date_str in sorted(date_to_per_contract.keys()):
    fpaths = date_to_per_contract[date_str]
    best_prices = None
    best_times = None
    best_count = 0
    best_sym = ""

    for fpath in fpaths:
        fname = os.path.basename(fpath)
        m = re.search(r"\.trades\.(ES[A-Z]\d)\.dbn\.zst$", fname)
        sym = m.group(1) if m else "?"
        try:
            store = db.DBNStore.from_file(fpath)
            df = store.to_df()

            if len(df) == 0:
                continue

            prices = df["price"].values.astype(np.float64)
            times = df.index.view(np.int64)

            # Filter to valid ES price range
            mask = (prices >= ES_PRICE_MIN) & (prices <= ES_PRICE_MAX)
            n_bad = (~mask).sum()
            if n_bad > 0:
                prices = prices[mask]
                times = times[mask]
                filtered_ticks += n_bad

            if len(prices) > best_count:
                best_count = len(prices)
                best_prices = prices
                best_times = times
                best_sym = sym

        except Exception as e:
            errors.append((fname, str(e)))

    if best_prices is not None:
        all_prices.append(best_prices)
        all_times.append(best_times)
        total_ticks += best_count

    processed += 1
    if processed % 50 == 0:
        elapsed = time.time() - t0
        print(f"  [{processed}/{len(all_dates)}] {elapsed:.1f}s, {total_ticks:,} ticks  (date={date_str} sym={best_sym})")

print(f"  Per-contract done: {len(date_to_per_contract)} dates, {total_ticks:,} ticks")

# Process parent symbol files (for new dates only)
print("\n── Processing parent symbol files (new dates only) ──")
parent_ticks_start = total_ticks
for fpath in parent_files_to_use:
    fname = os.path.basename(fpath)
    try:
        store = db.DBNStore.from_file(fpath)
        df = store.to_df()

        if len(df) == 0:
            processed += 1
            continue

        # Filter to ES outright symbols (no spreads)
        if "symbol" in df.columns:
            # ES outrights: start with 'ES', 4-5 chars, no dash
            es_syms = [s for s in df["symbol"].unique()
                       if s.startswith("ES") and "-" not in s and 4 <= len(s) <= 5]
            if not es_syms:
                processed += 1
                continue

            # Pick highest-volume ES contract
            best = max(es_syms, key=lambda s: len(df[df["symbol"] == s]))
            sub = df[df["symbol"] == best]
        else:
            processed += 1
            continue

        if len(sub) == 0:
            processed += 1
            continue

        prices = sub["price"].values.astype(np.float64)
        times = sub.index.view(np.int64)

        # Filter to valid ES price range
        mask = (prices >= ES_PRICE_MIN) & (prices <= ES_PRICE_MAX)
        n_bad = (~mask).sum()
        if n_bad > 0:
            prices = prices[mask]
            times = times[mask]
            filtered_ticks += n_bad

        if len(prices) > 0:
            all_prices.append(prices)
            all_times.append(times)
            total_ticks += len(prices)

    except Exception as e:
        errors.append((fname, str(e)))

    processed += 1
    if processed % 50 == 0:
        elapsed = time.time() - t0
        print(f"  [{processed}/{len(all_dates)}] {elapsed:.1f}s, {total_ticks:,} ticks  ({fname})")

parent_ticks = total_ticks - parent_ticks_start
print(f"  Parent files done: {len(parent_files_to_use)} files, {parent_ticks:,} new ticks")

if filtered_ticks > 0:
    print(f"\n  WARNING: Filtered {filtered_ticks:,} ticks outside ES price range ({ES_PRICE_MIN}-{ES_PRICE_MAX})")

# ── Step 3: Concatenate and sort ────────────────────────────────────────────

print(f"\n── Concatenating {total_ticks:,} ticks ──")
prices = np.concatenate(all_prices)
times = np.concatenate(all_times)

# Free memory
del all_prices, all_times

print(f"  Raw ticks: {len(prices):,}")

# Sort by timestamp
print("  Sorting by ts_recv...")
sort_idx = np.argsort(times, kind="mergesort")
prices = prices[sort_idx]
times = times[sort_idx]
del sort_idx

print(f"  Final ticks: {len(prices):,}")

# ── Step 4: Save ────────────────────────────────────────────────────────────

prices_path = os.path.join(OUT_DIR, "ES_prices.npy")
times_path  = os.path.join(OUT_DIR, "ES_times.npy")

print(f"\n  Saving {prices_path} ...")
np.save(prices_path, prices)
print(f"  Saving {times_path} ...")
np.save(times_path, times)

# ── Step 5: Summary ────────────────────────────────────────────────────────

import pandas as pd

elapsed = time.time() - t0
t_min = pd.Timestamp(times[0], unit="ns", tz="UTC")
t_max = pd.Timestamp(times[-1], unit="ns", tz="UTC")

print("\n" + "=" * 60)
print("DONE!")
print(f"  Total ticks:  {len(prices):,}")
print(f"  Date range:   {t_min} -> {t_max}")
print(f"  Price range:  {prices.min():.2f} - {prices.max():.2f}")
print(f"  Prices file:  {prices_path} ({os.path.getsize(prices_path) / 1e9:.2f} GB)")
print(f"  Times file:   {times_path} ({os.path.getsize(times_path) / 1e9:.2f} GB)")
print(f"  Elapsed:      {elapsed:.1f}s ({elapsed/60:.1f} min)")
if filtered_ticks > 0:
    print(f"  Filtered out: {filtered_ticks:,} ticks (outside {ES_PRICE_MIN}-{ES_PRICE_MAX} range)")

if errors:
    print(f"\n  ERRORS ({len(errors)}):")
    for fname, err in errors[:20]:
        print(f"    {fname}: {err}")
    if len(errors) > 20:
        print(f"    ... and {len(errors) - 20} more")
