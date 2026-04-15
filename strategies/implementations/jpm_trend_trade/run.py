"""JPM 国内期货趋势策略回测入口。

用法（在 cta_lab/ 目录下执行）：
    python -m strategies.implementations.jpm_trend_trade.run \\
        --data-dir /path/to/market_data/kline/china_daily_full \\
        --out-dir  /path/to/output

默认输出目录：<cta_lab 上级目录>/research_outputs/jpm_trend_china/

输出文件：
  reports/baseline_annual.csv
  reports/corrcap025_annual.csv
  reports/decade_breakdown.csv
  reports/sector_contribution.csv
  reports/asset_standalone_sr.csv
  reports/full_sample_summary.csv
  reports/annual_sharpe_comparison.csv
  reports/baseline_monthly.csv
  reports/corrcap025_monthly.csv
  charts/nav_china.png
  charts/annual_returns_bar.png
  charts/monthly_heatmap_baseline.png
  charts/monthly_heatmap_corrcap025.png
  charts/rolling_sharpe.png
  charts/sector_nav.png
  signals/multiperiod_positions.parquet
  advanced/corrcap_positions_cap025.parquet
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ── 路径设置：允许作为 __main__ 或通过 -m 运行 ────────────────────────────────
_HERE = Path(__file__).resolve().parent
_CTA_LAB = _HERE.parent.parent.parent   # cta_lab/
if str(_CTA_LAB) not in sys.path:
    sys.path.insert(0, str(_CTA_LAB))

from analysis.metrics import performance_summary
from strategies.implementations.jpm_trend_trade.strategy import JPMTrendStrategy
from strategies.implementations.jpm_trend_trade.config import TRADING_DAYS


# ── CLI 参数 ──────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="JPM 国内期货趋势策略回测")
    p.add_argument(
        "--data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "china_daily_full"),
        help="china_daily_full/ 数据目录",
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "jpm_trend_china"),
        help="输出根目录",
    )
    p.add_argument(
        "--no-corrcap",
        action="store_true",
        help="跳过 CorrCap 计算（节省时间）",
    )
    p.add_argument("--verbose", action="store_true", default=True)
    return p.parse_args()


# ── 绩效统计辅助 ──────────────────────────────────────────────────────────────

def _pnl_stats(pnl: pd.Series) -> dict:
    """计算 PnL 系列的完整绩效统计。"""
    ann_r = pnl.mean() * TRADING_DAYS
    ann_v = pnl.std() * np.sqrt(TRADING_DAYS)
    sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
    nav = (1 + pnl).cumprod()
    mdd = float(((nav - nav.cummax()) / nav.cummax()).min())
    calmar = ann_r / abs(mdd) if mdd != 0 else float("nan")
    hit = float((pnl > 0).mean())
    return {
        "Return(%)": round(ann_r * 100, 2),
        "Vol(%)": round(ann_v * 100, 2),
        "Sharpe": round(sharpe, 3),
        "MaxDD(%)": round(mdd * 100, 2),
        "Calmar": round(calmar, 3),
        "HitRate(%)": round(hit * 100, 1),
    }


def _annual_stats(pnl: pd.Series) -> pd.DataFrame:
    rows = []
    for year, grp in pnl.groupby(pnl.index.year):
        ann_r = grp.mean() * TRADING_DAYS
        ann_v = grp.std() * np.sqrt(TRADING_DAYS)
        sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
        nav = (1 + grp).cumprod()
        mdd = float(((nav - nav.cummax()) / nav.cummax()).min())
        rows.append({
            "Year": year,
            "Return(%)": round(ann_r * 100, 2),
            "Vol(%)": round(ann_v * 100, 2),
            "Sharpe": round(sharpe, 3),
            "MaxDD(%)": round(mdd * 100, 2),
            "Days": len(grp),
        })
    return pd.DataFrame(rows).set_index("Year")


def _decade_stats(pnl: pd.Series) -> pd.DataFrame:
    rows = []
    for ds in [1995, 2000, 2005, 2010, 2015, 2020]:
        mask = (pnl.index.year >= ds) & (pnl.index.year <= ds + 9)
        grp = pnl[mask]
        if len(grp) < 63:
            continue
        ann_r = grp.mean() * TRADING_DAYS
        ann_v = grp.std() * np.sqrt(TRADING_DAYS)
        sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
        nav = (1 + grp).cumprod()
        mdd = float(((nav - nav.cummax()) / nav.cummax()).min())
        rows.append({
            "Period": f"{ds}s",
            "Start": grp.index[0].date(),
            "End": grp.index[-1].date(),
            "Return(%)": round(ann_r * 100, 2),
            "Vol(%)": round(ann_v * 100, 2),
            "Sharpe": round(sharpe, 3),
            "MaxDD(%)": round(mdd * 100, 2),
        })
    return pd.DataFrame(rows).set_index("Period")


def _monthly_pivot(pnl: pd.Series) -> pd.DataFrame:
    mr = pnl.groupby([pnl.index.year, pnl.index.month]).apply(
        lambda g: (1 + g).prod() - 1
    )
    mr.index = pd.MultiIndex.from_tuples(mr.index, names=["Year", "Month"])
    piv = mr.unstack("Month") * 100
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    piv.columns = month_names[: len(piv.columns)]
    piv["Annual(%)"] = (
        (piv / 100 + 1).prod(axis=1, skipna=True).subtract(1).mul(100).round(2)
    )
    return piv.round(2)


def _sector_pnl(
    pos_df: pd.DataFrame,
    ret_df: pd.DataFrame,
    sector_map: dict[str, str],
    strategy: JPMTrendStrategy,
) -> pd.DataFrame:
    """计算各板块独立 PnL 绩效（通过 VectorizedBacktest）。"""
    from backtest.vectorized import VectorizedBacktest

    bt = strategy._make_backtest()
    sectors: dict[str, list[str]] = {}
    for sym, sec in sector_map.items():
        sectors.setdefault(sec, []).append(sym)

    rows = []
    for sec, syms in sorted(sectors.items()):
        syms_avail = [s for s in syms if s in ret_df.columns]
        if not syms_avail:
            continue
        result_s = bt.run(pos_df[syms_avail], ret_df[syms_avail])
        pnl_s = result_s.returns.iloc[1:]
        stats = _pnl_stats(pnl_s)
        rows.append({"Sector": sec, "Symbols": len(syms_avail), **stats})

    return pd.DataFrame(rows).set_index("Sector")


def _asset_standalone(
    pos_df: pd.DataFrame,
    ret_df: pd.DataFrame,
    sector_map: dict[str, str],
    strategy: JPMTrendStrategy,
) -> pd.DataFrame:
    bt = strategy._make_backtest()
    rows = []
    for sym in ret_df.columns:
        result_a = bt.run(pos_df[[sym]], ret_df[[sym]])
        pnl_a = result_a.returns.iloc[1:]
        stats = _pnl_stats(pnl_a)
        rows.append({
            "Symbol": sym,
            "Sector": sector_map.get(sym, "Other"),
            "StandaloneSR": stats["Sharpe"],
            "MaxDD(%)": stats["MaxDD(%)"],
            "Start": ret_df[sym].first_valid_index().date(),
        })
    return (
        pd.DataFrame(rows).set_index("Symbol").sort_values("StandaloneSR", ascending=False)
    )


# ── 图表函数 ──────────────────────────────────────────────────────────────────

def _plot_nav(
    pnl_base: pd.Series,
    pnl_cc: pd.Series,
    sharpe_base: float,
    mdd_base: float,
    sharpe_cc: float,
    mdd_cc: float,
    out_path: str,
) -> None:
    nav_base = (1 + pnl_base).cumprod()
    nav_cc = (1 + pnl_cc).cumprod()
    fig, axes = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})
    ax = axes[0]
    ax.semilogy(nav_base.index, nav_base.values, "steelblue", linewidth=1.5,
                label=f"Baseline  (SR={sharpe_base:.3f}, MDD={mdd_base*100:.1f}%)")
    ax.semilogy(nav_cc.index, nav_cc.values, "darkorange", linewidth=1.5,
                label=f"CorrCap-0.25  (SR={sharpe_cc:.3f}, MDD={mdd_cc*100:.1f}%)")
    ax.set_ylabel("NAV (log scale)", fontsize=11)
    ax.set_title("China Domestic Futures Trend — Full Period NAV\n"
                 "JPM t-stat signal, EWMA vol targeting 10% ann.", fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, which="both")

    dd_base = (nav_base - nav_base.cummax()) / nav_base.cummax() * 100
    dd_cc = (nav_cc - nav_cc.cummax()) / nav_cc.cummax() * 100
    ax2 = axes[1]
    ax2.fill_between(dd_base.index, dd_base.values, 0, color="steelblue", alpha=0.4)
    ax2.fill_between(dd_cc.index, dd_cc.values, 0, color="darkorange", alpha=0.4)
    ax2.set_ylabel("Drawdown (%)", fontsize=10)
    ax2.set_xlabel("Date", fontsize=10)
    ax2.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _plot_annual_bar(
    tbl_base: pd.DataFrame,
    tbl_cc: pd.DataFrame,
    out_path: str,
) -> None:
    years = tbl_base.index
    x = np.arange(len(years))
    w = 0.38
    fig, ax = plt.subplots(figsize=(16, 5))
    ax.bar(x - w / 2, tbl_base["Return(%)"], width=w, color="steelblue", alpha=0.85, label="Baseline")
    ax.bar(x + w / 2, tbl_cc["Return(%)"], width=w, color="darkorange", alpha=0.85, label="CorrCap-0.25")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, fontsize=8)
    ax.set_ylabel("Annual Return (%)", fontsize=11)
    ax.set_title("China Futures Trend — Annual Returns: Baseline vs CorrCap-0.25", fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.2, axis="y")
    plt.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _plot_monthly_heatmap(mpiv: pd.DataFrame, title: str, out_path: str) -> None:
    data = mpiv.drop(columns=["Annual(%)"]).values.astype(float)
    row_labels = mpiv.index.astype(str).tolist()
    col_labels = list(mpiv.drop(columns=["Annual(%)"]).columns)
    fig, ax = plt.subplots(figsize=(13, max(6, len(row_labels) * 0.28)))
    vmax = min(np.nanpercentile(np.abs(data), 95), 15)
    im = ax.imshow(data, cmap="RdYlGn", aspect="auto", vmin=-vmax, vmax=vmax)
    ax.set_xticks(range(len(col_labels)))
    ax.set_xticklabels(col_labels, fontsize=9)
    ax.set_yticks(range(len(row_labels)))
    ax.set_yticklabels(row_labels, fontsize=8)
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            v = data[i, j]
            if not np.isnan(v):
                ax.text(j, i, f"{v:.1f}", ha="center", va="center",
                        fontsize=6.5, color="black" if abs(v) < vmax * 0.6 else "white")
    ann_vals = mpiv["Annual(%)"].values
    for i, v in enumerate(ann_vals):
        if not np.isnan(v):
            color = "#2a6c2a" if v > 0 else "#8b1a1a"
            ax.text(len(col_labels) + 0.6, i, f"{v:.1f}%", ha="left", va="center",
                    fontsize=8, color=color, fontweight="bold")
    ax.text(len(col_labels) + 0.6, -0.8, "Annual", ha="left", va="center",
            fontsize=8, fontweight="bold")
    plt.colorbar(im, ax=ax, label="Monthly Return (%)", shrink=0.6)
    ax.set_title(title, fontsize=12)
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _plot_rolling_sharpe(
    pnl_base: pd.Series,
    pnl_cc: pd.Series,
    sharpe_base: float,
    sharpe_cc: float,
    out_path: str,
) -> None:
    roll_sh_base = (pnl_base.rolling(252).mean() * TRADING_DAYS
                    / (pnl_base.rolling(252).std() * np.sqrt(TRADING_DAYS)))
    roll_sh_cc = (pnl_cc.rolling(252).mean() * TRADING_DAYS
                  / (pnl_cc.rolling(252).std() * np.sqrt(TRADING_DAYS)))
    fig, ax = plt.subplots(figsize=(14, 4))
    ax.plot(roll_sh_base.index, roll_sh_base.values, "steelblue", linewidth=1.2, label="Baseline")
    ax.plot(roll_sh_cc.index, roll_sh_cc.values, "darkorange", linewidth=1.2, label="CorrCap-0.25")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.axhline(sharpe_base, color="steelblue", linestyle="--", linewidth=0.8, alpha=0.6)
    ax.axhline(sharpe_cc, color="darkorange", linestyle="--", linewidth=0.8, alpha=0.6)
    ax.set_ylabel("Rolling 1-Year Sharpe", fontsize=11)
    ax.set_title("China Futures Trend — Rolling 1-Year Sharpe", fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _plot_sector_nav(
    baseline_pos: pd.DataFrame,
    returns: pd.DataFrame,
    sector_map: dict[str, str],
    strategy: JPMTrendStrategy,
    out_path: str,
) -> None:
    sectors: dict[str, list[str]] = {}
    for sym, sec in sector_map.items():
        sectors.setdefault(sec, []).append(sym)

    fig, ax = plt.subplots(figsize=(14, 6))
    colors = plt.cm.tab10.colors  # type: ignore[attr-defined]
    for i, sec in enumerate(sorted(sectors)):
        syms_avail = [s for s in sectors[sec] if s in returns.columns]
        if not syms_avail:
            continue
        result_s = strategy._make_backtest().run(baseline_pos[syms_avail], returns[syms_avail])
        pnl_s = result_s.returns.iloc[1:]
        nav_s = (1 + pnl_s).cumprod()
        sh = _pnl_stats(pnl_s)["Sharpe"]
        ax.semilogy(nav_s.index, nav_s.values, color=colors[i % 10],
                    linewidth=1.2, label=f"{sec} (SR={sh:.2f})")

    ax.set_ylabel("NAV (log scale)", fontsize=11)
    ax.set_title("China Futures — Sector NAV Comparison (Baseline)", fontsize=12)
    ax.legend(fontsize=8, ncol=2)
    ax.grid(True, alpha=0.3, which="both")
    plt.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


# ── 主流程 ────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()
    out_dir = Path(args.out_dir)
    compute_corrcap = not args.no_corrcap

    # 创建输出子目录
    dirs = {
        "reports":  out_dir / "reports",
        "charts":   out_dir / "charts",
        "signals":  out_dir / "signals",
        "advanced": out_dir / "advanced",
        "data":     out_dir / "data",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)

    # ── 运行策略流水线 ─────────────────────────────────────────────────────────
    strategy = JPMTrendStrategy()
    result = strategy.run_pipeline(
        data_dir=args.data_dir,
        compute_corrcap=compute_corrcap,
        verbose=args.verbose,
    )

    returns      = result.returns
    signal       = result.signal
    sigma        = result.sigma
    baseline_pos = result.baseline_pos
    corrcap_pos  = result.corrcap_pos
    pnl_base     = result.pnl_baseline
    pnl_cc       = result.pnl_corrcap
    sector_map   = result.sector_map

    # ── 保存原始数据 ───────────────────────────────────────────────────────────
    returns.to_parquet(dirs["data"] / "returns.parquet")
    with open(dirs["data"] / "asset_list.json", "w") as f:
        json.dump(
            {"symbols": returns.columns.tolist(), "sector_map": sector_map}, f, indent=2
        )
    baseline_pos.to_parquet(dirs["signals"] / "multiperiod_positions.parquet")
    if compute_corrcap:
        corrcap_pos.to_parquet(dirs["advanced"] / "corrcap_positions_cap025.parquet")

    # ── Section 1: Baseline 年度绩效 ──────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Section 1: Baseline Annual Performance")
    print("=" * 65)
    tbl_base = _annual_stats(pnl_base)
    print(tbl_base.drop(columns=["Days"]).to_string())

    # ── Section 2: Decade 分段 ────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Section 2: Decade-level Sharpe")
    print("=" * 65)
    decade_df = _decade_stats(pnl_base)
    print(decade_df.to_string())

    # ── Section 3: 板块贡献 ────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Section 3: Sector Contribution (Baseline)")
    print("=" * 65)
    sector_df = _sector_pnl(baseline_pos, returns, sector_map, strategy)
    print(sector_df.to_string())

    # ── Section 4: 个股独立 Sharpe ────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Section 4: Per-asset Standalone Sharpe")
    print("=" * 65)
    asset_df = _asset_standalone(baseline_pos, returns, sector_map, strategy)
    print(asset_df.to_string())

    # ── Section 5: CorrCap 年度绩效 ────────────────────────────────────────────
    if compute_corrcap:
        print("\n" + "=" * 65)
        print("Section 5: CorrCap-0.25 Annual Performance")
        print("=" * 65)
        tbl_cc = _annual_stats(pnl_cc)
        print(tbl_cc.drop(columns=["Days"]).to_string())
    else:
        tbl_cc = tbl_base.copy()   # placeholder

    # ── Section 6: 全样本汇总 ─────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Section 6: Full-Sample Summary")
    print("=" * 65)
    base_stats = _pnl_stats(pnl_base)
    base_stats["Label"] = "Baseline"
    full_rows = [base_stats]
    if compute_corrcap:
        cc_stats = _pnl_stats(pnl_cc)
        cc_stats["Label"] = "CorrCap-0.25"
        full_rows.append(cc_stats)
    full_summary = pd.DataFrame(full_rows).set_index("Label")
    print(full_summary.to_string())

    # ── Section 7: 月度收益 ───────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Section 7: Monthly Returns")
    print("=" * 65)
    mpiv_base = _monthly_pivot(pnl_base)
    print("\nBaseline — Monthly Returns (%):")
    print(mpiv_base.to_string())
    if compute_corrcap:
        mpiv_cc = _monthly_pivot(pnl_cc)
        print("\nCorrCap-0.25 — Monthly Returns (%):")
        print(mpiv_cc.to_string())
    else:
        mpiv_cc = mpiv_base.copy()

    # ── 保存 CSV 报告 ─────────────────────────────────────────────────────────
    tbl_base.to_csv(dirs["reports"] / "baseline_annual.csv")
    tbl_cc.to_csv(dirs["reports"] / "corrcap025_annual.csv")
    decade_df.to_csv(dirs["reports"] / "decade_breakdown.csv")
    sector_df.to_csv(dirs["reports"] / "sector_contribution.csv")
    asset_df.to_csv(dirs["reports"] / "asset_standalone_sr.csv")
    full_summary.to_csv(dirs["reports"] / "full_sample_summary.csv")
    pd.DataFrame({
        "Baseline": tbl_base["Sharpe"],
        "CorrCap-025": tbl_cc["Sharpe"],
    }).to_csv(dirs["reports"] / "annual_sharpe_comparison.csv")
    mpiv_base.to_csv(dirs["reports"] / "baseline_monthly.csv")
    mpiv_cc.to_csv(dirs["reports"] / "corrcap025_monthly.csv")

    # ── Section 8: 图表 ────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Section 8: Charts")
    print("=" * 65)

    s_base = _pnl_stats(pnl_base)
    sharpe_base = s_base["Sharpe"]
    mdd_base = s_base["MaxDD(%)"] / 100

    if compute_corrcap:
        s_cc = _pnl_stats(pnl_cc)
        sharpe_cc = s_cc["Sharpe"]
        mdd_cc = s_cc["MaxDD(%)"] / 100
    else:
        sharpe_cc = mdd_cc = float("nan")
        pnl_cc = pnl_base  # fallback for chart functions

    _plot_nav(pnl_base, pnl_cc, sharpe_base, mdd_base, sharpe_cc, mdd_cc,
              str(dirs["charts"] / "nav_china.png"))
    print("  Saved nav_china.png")

    _plot_annual_bar(tbl_base, tbl_cc, str(dirs["charts"] / "annual_returns_bar.png"))
    print("  Saved annual_returns_bar.png")

    _plot_monthly_heatmap(
        mpiv_base,
        "China Futures Trend — Baseline Monthly Returns (%)",
        str(dirs["charts"] / "monthly_heatmap_baseline.png"),
    )
    print("  Saved monthly_heatmap_baseline.png")

    _plot_monthly_heatmap(
        mpiv_cc,
        "China Futures Trend — CorrCap-0.25 Monthly Returns (%)",
        str(dirs["charts"] / "monthly_heatmap_corrcap025.png"),
    )
    print("  Saved monthly_heatmap_corrcap025.png")

    _plot_rolling_sharpe(pnl_base, pnl_cc, sharpe_base, sharpe_cc,
                         str(dirs["charts"] / "rolling_sharpe.png"))
    print("  Saved rolling_sharpe.png")

    _plot_sector_nav(baseline_pos, returns, sector_map, strategy,
                     str(dirs["charts"] / "sector_nav.png"))
    print("  Saved sector_nav.png")

    print(f"\nAll reports : {dirs['reports']}/")
    print(f"All charts  : {dirs['charts']}/")
    print(
        f"\nSummary: Baseline SR={sharpe_base:.3f}"
        + (f"  CorrCap-0.25 SR={sharpe_cc:.3f}" if compute_corrcap else "")
    )
    print(
        f"         Universe: {returns.shape[1]} instruments  "
        f"Period: {returns.index[0].date()} - {returns.index[-1].date()}"
    )


if __name__ == "__main__":
    main()
