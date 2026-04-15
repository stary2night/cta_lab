"""
CTA Data Pipeline — Data Agent
Runs all 5 tasks: load data, compute returns, stats, autocorr, asset list, report.
"""

import sys
import os
import json
import numpy as np
import pandas as pd

# ── Ensure project root is on path ───────────────────────────────────────────
PROJECT_ROOT = "/home/ubuntu/dengl/my_projects/cta"
sys.path.insert(0, PROJECT_ROOT)

from module1_data_overseas import (
    load_all_futures_overseas,
    compute_returns,
    descriptive_stats,
    SECTOR_MAP_OVERSEAS,
)

OUT_DIR = "/home/ubuntu/dengl/my_projects/cta/JP Cross Asset/research_outputs/data"
os.makedirs(OUT_DIR, exist_ok=True)

DATA_DIR = "/home/ubuntu/dengl/my_projects/cta/FutureData/overseas_new"

# ─────────────────────────────────────────────────────────────────────────────
# Task 1: Load data and compute returns
# ─────────────────────────────────────────────────────────────────────────────
print("=" * 70)
print("TASK 1: Load futures data and compute returns")
print("=" * 70)

prices = load_all_futures_overseas(DATA_DIR, min_listing_days=252)
print(f"\nSymbols loaded: {sorted(prices.keys())}")

returns_raw = compute_returns(prices)
print(f"\nFull returns shape: {returns_raw.shape}")
print(f"Date range (raw): {returns_raw.index.min().date()} ~ {returns_raw.index.max().date()}")

# Filter to 2010-01-01 ~ 2024-12-31
returns = returns_raw.loc["2010-01-01":"2024-12-31"].copy()
print(f"\nFiltered returns shape: {returns.shape}")
print(f"Date range (filtered): {returns.index.min().date()} ~ {returns.index.max().date()}")

returns_path = os.path.join(OUT_DIR, "returns.parquet")
returns.to_parquet(returns_path)
print(f"\n[SAVED] returns.parquet -> {returns_path}")

# ─────────────────────────────────────────────────────────────────────────────
# Task 2: Descriptive statistics
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("TASK 2: Descriptive statistics")
print("=" * 70)

stats = descriptive_stats(returns, SECTOR_MAP_OVERSEAS)

stats_path = os.path.join(OUT_DIR, "stats.csv")
stats.to_csv(stats_path)
print(f"\n[SAVED] stats.csv -> {stats_path}")

# ─────────────────────────────────────────────────────────────────────────────
# Task 3: Autocorrelation
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("TASK 3: Autocorrelation at lags 1, 5, 22")
print("=" * 70)

# Build reverse mapping: symbol -> sector
symbol_to_sector = {}
for sector, symbols in SECTOR_MAP_OVERSEAS.items():
    for sym in symbols:
        symbol_to_sector[sym] = sector

autocorr_records = []
for sym in returns.columns:
    s = returns[sym].dropna()
    if len(s) < 50:
        print(f"  [Skip] {sym}: only {len(s)} obs, too few for autocorr")
        continue
    ac1  = s.autocorr(lag=1)
    ac5  = s.autocorr(lag=5)
    ac22 = s.autocorr(lag=22)
    autocorr_records.append({
        "symbol": sym,
        "lag_1":  round(ac1,  6),
        "lag_5":  round(ac5,  6),
        "lag_22": round(ac22, 6),
        "sector": symbol_to_sector.get(sym, "Unclassified"),
    })
    print(f"  {sym:10s}  lag1={ac1:+.4f}  lag5={ac5:+.4f}  lag22={ac22:+.4f}")

autocorr_df = pd.DataFrame(autocorr_records)
autocorr_path = os.path.join(OUT_DIR, "autocorr.csv")
autocorr_df.to_csv(autocorr_path, index=False)
print(f"\n[SAVED] autocorr.csv -> {autocorr_path}")

# ─────────────────────────────────────────────────────────────────────────────
# Task 4: Asset list JSON
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("TASK 4: Asset list JSON")
print("=" * 70)

symbols_in_returns = sorted(returns.columns.tolist())
sector_map_for_symbols = {sym: symbol_to_sector.get(sym, "Unclassified")
                          for sym in symbols_in_returns}

asset_list = {
    "symbols": symbols_in_returns,
    "sector_map": sector_map_for_symbols,
}

asset_list_path = os.path.join(OUT_DIR, "asset_list.json")
with open(asset_list_path, "w") as f:
    json.dump(asset_list, f, indent=2)
print(f"[SAVED] asset_list.json -> {asset_list_path}")
print(f"  Total symbols: {len(symbols_in_returns)}")

# ─────────────────────────────────────────────────────────────────────────────
# Task 5: DATA_REPORT.md
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("TASK 5: Writing DATA_REPORT.md")
print("=" * 70)

# --- Helpers for the report ---

# Coverage stats
coverage = returns.notna().mean() * 100  # % non-NaN per symbol
avg_coverage = coverage.mean()

# Symbols per sector
sector_counts = {}
for sector, syms in SECTOR_MAP_OVERSEAS.items():
    in_universe = [s for s in syms if s in returns.columns]
    if in_universe:
        sector_counts[sector] = in_universe
unclassified = [s for s in returns.columns
                if symbol_to_sector.get(s, "Unclassified") == "Unclassified"]
if unclassified:
    sector_counts["Unclassified"] = unclassified

# Top 5 by lag-1 autocorr (most positive = most trending)
top5_ac = (autocorr_df
           .sort_values("lag_1", ascending=False)
           .head(5)[["symbol", "sector", "lag_1", "lag_5", "lag_22"]])

# Top 5 by annualised Sharpe
top5_sharpe = (stats
               .reset_index()
               .rename(columns={"Symbol": "symbol"})
               .sort_values("Sharpe", ascending=False)
               .head(5)[["symbol", "Sector", "Ann. Return", "Ann. Vol", "Sharpe"]])

# Date range
date_start = returns.index.min().date()
date_end   = returns.index.max().date()

# Coverage per symbol table
coverage_df = coverage.reset_index()
coverage_df.columns = ["Symbol", "Coverage (%)"]
coverage_df["Coverage (%)"] = coverage_df["Coverage (%)"].round(1)
avg_cov_pct = round(avg_coverage, 1)

# ── Build markdown ────────────────────────────────────────────────────────────
md_lines = []

md_lines.append("# CTA Research — Data Report")
md_lines.append("")
md_lines.append(f"_Generated: 2026-03-29_")
md_lines.append("")

md_lines.append("## 1. Asset Universe Summary")
md_lines.append("")
md_lines.append(f"| Parameter | Value |")
md_lines.append(f"|-----------|-------|")
md_lines.append(f"| Total symbols | {len(symbols_in_returns)} |")
md_lines.append(f"| Date range | {date_start} to {date_end} |")
md_lines.append(f"| Calendar days | {(returns.index.max() - returns.index.min()).days:,} |")
md_lines.append(f"| Trading rows | {len(returns):,} |")
md_lines.append(f"| Sectors | {len(sector_counts)} |")
md_lines.append(f"| Average data coverage | {avg_cov_pct}% |")
md_lines.append("")

md_lines.append("## 2. Symbols per Sector")
md_lines.append("")
md_lines.append("| Sector | Count | Symbols |")
md_lines.append("|--------|-------|---------|")
for sector, syms in sector_counts.items():
    md_lines.append(f"| {sector} | {len(syms)} | {', '.join(sorted(syms))} |")
md_lines.append("")

md_lines.append("## 3. Data Coverage per Symbol")
md_lines.append("")
md_lines.append(f"Average coverage across all symbols: **{avg_cov_pct}%**")
md_lines.append("")
md_lines.append("| Symbol | Sector | Coverage (%) | Valid Days |")
md_lines.append("|--------|--------|-------------|------------|")
for sym in sorted(returns.columns):
    cov_pct = round(coverage[sym], 1)
    valid_d = returns[sym].notna().sum()
    sec = symbol_to_sector.get(sym, "Unclassified")
    md_lines.append(f"| {sym} | {sec} | {cov_pct} | {valid_d} |")
md_lines.append("")

md_lines.append("## 4. Top 5 Assets by Autocorrelation Lag-1 (Most Trending)")
md_lines.append("")
md_lines.append("Positive lag-1 autocorrelation indicates momentum / trending behaviour.")
md_lines.append("")
md_lines.append("| Symbol | Sector | Lag-1 | Lag-5 | Lag-22 |")
md_lines.append("|--------|--------|-------|-------|--------|")
for _, row in top5_ac.iterrows():
    md_lines.append(
        f"| {row['symbol']} | {row['sector']} | {row['lag_1']:+.4f} | "
        f"{row['lag_5']:+.4f} | {row['lag_22']:+.4f} |"
    )
md_lines.append("")

md_lines.append("## 5. Top 5 Assets by Annualised Sharpe (Buy & Hold)")
md_lines.append("")
md_lines.append("| Symbol | Sector | Ann. Return | Ann. Vol | Sharpe |")
md_lines.append("|--------|--------|------------|----------|--------|")
for _, row in top5_sharpe.iterrows():
    md_lines.append(
        f"| {row['symbol']} | {row['Sector']} | {row['Ann. Return']:+.4f} | "
        f"{row['Ann. Vol']:.4f} | {row['Sharpe']:+.4f} |"
    )
md_lines.append("")

md_lines.append("## 6. Data Quality Notes")
md_lines.append("")
md_lines.append("- **Continuous contract method**: OI-weighted roll with a 3-day stability filter. ")
md_lines.append("  NAV series are normalised to 1.0 at first available date.")
md_lines.append("- **Roll treatment**: par-value roll (no basis P&L at roll point); ")
md_lines.append("  returns are computed on the within-contract price change only.")
md_lines.append("- **H00300** (CSI 300 cash index) was excluded — different schema (Tushare), ")
md_lines.append("  not a tradable futures contract.")
md_lines.append("- **min_listing_days = 252**: symbols with fewer than 252 valid trading days ")
md_lines.append("  across the full raw history are dropped before the 2010-2024 filter.")
md_lines.append("- **Date filter**: returns are restricted to 2010-01-01 to 2024-12-31. ")
md_lines.append("  Symbols with data outside this window will have NaN rows at the edges.")

# Coverage warning
low_cov = coverage_df[coverage_df["Coverage (%)"] < 50]
if not low_cov.empty:
    md_lines.append(f"- **Low coverage symbols** (< 50% non-NaN in filtered window): "
                    f"{', '.join(low_cov['Symbol'].tolist())}. "
                    f"These symbols have limited history within 2010–2024.")

md_lines.append("")
md_lines.append("---")
md_lines.append("_Report generated by CTA Data Agent pipeline._")

report_text = "\n".join(md_lines)

report_path = os.path.join(OUT_DIR, "DATA_REPORT.md")
with open(report_path, "w") as f:
    f.write(report_text)
print(f"[SAVED] DATA_REPORT.md -> {report_path}")

# ── Final summary ─────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("ALL TASKS COMPLETE — Output files:")
print("=" * 70)
for fname in ["returns.parquet", "stats.csv", "autocorr.csv",
              "asset_list.json", "DATA_REPORT.md"]:
    fpath = os.path.join(OUT_DIR, fname)
    size = os.path.getsize(fpath)
    print(f"  {fname:30s}  {size:>10,} bytes")
