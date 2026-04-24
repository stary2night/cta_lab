"""JPM 国内期货趋势策略回测入口。

用法（在 cta_lab/ 目录下执行）：
    python scripts/run_jpm.py \\
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
import sys
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import pandas as pd

warnings.filterwarnings("ignore")

# ── 路径设置 ──────────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
_CTA_LAB = _HERE.parent   # scripts/ -> cta_lab/
if str(_CTA_LAB) not in sys.path:
    sys.path.insert(0, str(_CTA_LAB))

from analysis.metrics import pnl_stats, annual_stats, decade_stats, monthly_pivot, sector_stats, asset_stats
from analysis.report.charts import (
    plot_nav_with_drawdown,
    plot_annual_bar,
    plot_rolling_sharpe,
    plot_monthly_heatmap,
    plot_sector_nav,
)
from analysis.report.output import BacktestOutput
from backtest import ProportionalCostModel
from backtest.vectorized import VectorizedBacktest
from data.loader import DataLoader, KlineSchema
from data.sources.parquet_source import ParquetSource
from strategies.context import StrategyContext
from strategies.implementations.jpm_trend_trade.strategy import JPMTrendStrategy


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
    p.add_argument("--cost-bps", type=float, default=None, help="单边换手成本，单位 bps；默认使用 JPMConfig")
    p.add_argument("--max-abs-weight", type=float, default=None, help="单品种持仓上限（绝对值），如 0.10")
    p.add_argument("--max-gross-exposure", type=float, default=None, help="组合 gross 上限，如 1.50")
    p.add_argument("--verbose", action="store_true", default=True)
    return p.parse_args()


# ── 主流程 ────────────────────────────────────────────────────────────────────

def _turnover_cost_frame(turnover: pd.Series | None, cost_rate: float) -> pd.DataFrame:
    """Build a turnover/cost report from effective backtest turnover."""
    if turnover is None:
        return pd.DataFrame(columns=["turnover", "transaction_cost"])
    frame = turnover.to_frame("turnover")
    frame["transaction_cost"] = frame["turnover"] * cost_rate
    return frame


def _turnover_cost_summary(frame: pd.DataFrame, trading_days: int) -> dict[str, float]:
    """Summarize effective turnover and transaction-cost drag."""
    if frame.empty:
        return {
            "AvgTurnover(%)": 0.0,
            "AnnTurnover(x)": 0.0,
            "TotalCost(%)": 0.0,
            "AnnCost(%)": 0.0,
        }
    return {
        "AvgTurnover(%)": round(float(frame["turnover"].mean() * 100.0), 2),
        "AnnTurnover(x)": round(float(frame["turnover"].mean() * trading_days), 2),
        "TotalCost(%)": round(float(frame["transaction_cost"].sum() * 100.0), 2),
        "AnnCost(%)": round(float(frame["transaction_cost"].mean() * trading_days * 100.0), 2),
    }


def main() -> None:
    args = _parse_args()
    compute_corrcap = not args.no_corrcap

    out = BacktestOutput(
        args.out_dir,
        subdirs=["reports", "charts", "signals", "advanced", "data"],
    )

    strategy = JPMTrendStrategy()
    cost_bps = strategy.transaction_cost_bps if args.cost_bps is None else args.cost_bps
    loader = DataLoader(
        kline_source=ParquetSource(args.data_dir),
        kline_schema=KlineSchema.tushare(),
    )
    backtest = VectorizedBacktest(
        lag=1,
        vol_target=strategy.target_vol,
        vol_halflife=strategy.vol_halflife,
        trading_days=strategy.trading_days,
        cost_model=ProportionalCostModel(cost_bps / 10_000.0),
        max_abs_weight=args.max_abs_weight,
        max_gross_exposure=args.max_gross_exposure,
    )
    context = StrategyContext(
        loader=loader,
        sector_map=strategy.sector_map,
        backtest=backtest,
    )
    result = strategy.run_pipeline(
        context=context,
        compute_corrcap=compute_corrcap,
        verbose=args.verbose,
    )

    returns      = result.returns
    baseline_pos = result.baseline_pos
    corrcap_pos  = result.corrcap_pos
    pnl_base     = result.pnl_baseline
    pnl_cc       = result.pnl_corrcap
    sector_map   = result.sector_map
    bt           = context.backtest
    assert bt is not None

    # ── Raw data ──────────────────────────────────────────────────────────────
    out.save_parquet(returns, "data", "returns.parquet")
    out.save_json(
        {"symbols": returns.columns.tolist(), "sector_map": sector_map},
        "data", "asset_list.json",
    )
    out.save_parquet(baseline_pos, "signals", "multiperiod_positions.parquet")
    turnover_base = _turnover_cost_frame(
        result.result_baseline.turnover_series if result.result_baseline is not None else None,
        cost_bps / 10_000.0,
    )
    out.save_csv(turnover_base, "reports", "baseline_turnover_cost.csv")
    if compute_corrcap:
        out.save_parquet(corrcap_pos, "advanced", "corrcap_positions_cap025.parquet")
        turnover_cc = _turnover_cost_frame(
            result.result_corrcap.turnover_series if result.result_corrcap is not None else None,
            cost_bps / 10_000.0,
        )
        out.save_csv(turnover_cc, "reports", "corrcap025_turnover_cost.csv")

    # ── Stats ─────────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Section 1: Baseline Annual Performance")
    print("=" * 65)
    tbl_base = annual_stats(pnl_base)
    print(tbl_base.drop(columns=["Days"]).to_string())

    print("\n" + "=" * 65)
    print("Section 2: Decade-level Sharpe")
    print("=" * 65)
    decade_df = decade_stats(pnl_base)
    print(decade_df.to_string())

    print("\n" + "=" * 65)
    print("Section 3: Sector Contribution (Baseline)")
    print("=" * 65)
    sector_df = sector_stats(baseline_pos, returns, sector_map, bt)
    print(sector_df.to_string())

    print("\n" + "=" * 65)
    print("Section 4: Per-asset Standalone Sharpe")
    print("=" * 65)
    asset_df = asset_stats(baseline_pos, returns, sector_map, bt)
    print(asset_df.to_string())

    if compute_corrcap:
        print("\n" + "=" * 65)
        print("Section 5: CorrCap-0.25 Annual Performance")
        print("=" * 65)
        tbl_cc = annual_stats(pnl_cc)
        print(tbl_cc.drop(columns=["Days"]).to_string())
    else:
        tbl_cc = tbl_base.copy()

    print("\n" + "=" * 65)
    print("Section 6: Full-Sample Summary")
    print("=" * 65)
    base_stats = pnl_stats(pnl_base)
    base_stats.update(_turnover_cost_summary(turnover_base, strategy.trading_days))
    base_stats["Label"] = "Baseline"
    full_rows = [base_stats]
    if compute_corrcap:
        cc_stats = pnl_stats(pnl_cc)
        cc_stats.update(_turnover_cost_summary(turnover_cc, strategy.trading_days))
        cc_stats["Label"] = "CorrCap-0.25"
        full_rows.append(cc_stats)
    full_summary = pd.DataFrame(full_rows).set_index("Label")
    print(full_summary.to_string())

    print("\n" + "=" * 65)
    print("Section 7: Monthly Returns")
    print("=" * 65)
    mpiv_base = monthly_pivot(pnl_base)
    print("\nBaseline — Monthly Returns (%):")
    print(mpiv_base.to_string())
    if compute_corrcap:
        mpiv_cc = monthly_pivot(pnl_cc)
        print("\nCorrCap-0.25 — Monthly Returns (%):")
        print(mpiv_cc.to_string())
    else:
        mpiv_cc = mpiv_base.copy()

    # ── CSV reports ───────────────────────────────────────────────────────────
    out.save_csv(tbl_base,     "reports", "baseline_annual.csv")
    out.save_csv(tbl_cc,       "reports", "corrcap025_annual.csv")
    out.save_csv(decade_df,    "reports", "decade_breakdown.csv")
    out.save_csv(sector_df,    "reports", "sector_contribution.csv")
    out.save_csv(asset_df,     "reports", "asset_standalone_sr.csv")
    out.save_csv(full_summary, "reports", "full_sample_summary.csv")
    out.save_csv(
        pd.DataFrame({"Baseline": tbl_base["Sharpe"], "CorrCap-025": tbl_cc["Sharpe"]}),
        "reports", "annual_sharpe_comparison.csv",
    )
    out.save_csv(mpiv_base, "reports", "baseline_monthly.csv")
    out.save_csv(mpiv_cc,   "reports", "corrcap025_monthly.csv")

    # ── Charts ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Section 8: Charts")
    print("=" * 65)

    if not compute_corrcap:
        pnl_cc = pnl_base

    _nav_colors = {"Baseline": "steelblue", "CorrCap-0.25": "darkorange"}
    out.save_fig(
        plot_nav_with_drawdown(
            {"Baseline": pnl_base, "CorrCap-0.25": pnl_cc},
            title="China Domestic Futures Trend — Full Period NAV\n"
                  "JPM t-stat signal, EWMA vol targeting 10% ann.",
            colors=_nav_colors,
        ),
        "charts", "nav_china.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar(
            {"Baseline": tbl_base, "CorrCap-0.25": tbl_cc},
            title="China Futures Trend — Annual Returns: Baseline vs CorrCap-0.25",
            colors=_nav_colors,
        ),
        "charts", "annual_returns_bar.png", dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(
            mpiv_base,
            title="China Futures Trend — Baseline Monthly Returns (%)",
        ),
        "charts", "monthly_heatmap_baseline.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_monthly_heatmap(
            mpiv_cc,
            title="China Futures Trend — CorrCap-0.25 Monthly Returns (%)",
        ),
        "charts", "monthly_heatmap_corrcap025.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_rolling_sharpe(
            {"Baseline": pnl_base, "CorrCap-0.25": pnl_cc},
            title="China Futures Trend — Rolling 1-Year Sharpe",
            colors=_nav_colors,
        ),
        "charts", "rolling_sharpe.png", dpi=150,
    )
    out.save_fig(
        plot_sector_nav(
            baseline_pos, returns, sector_map, bt,
            title="China Futures — Sector NAV Comparison (Baseline)",
        ),
        "charts", "sector_nav.png", dpi=150,
    )

    print(f"\nAll outputs:")
    out.summary()
    _summary_base = pnl_stats(pnl_base)
    print(
        f"\nSummary: Baseline SR={_summary_base['Sharpe']:.3f}"
        + (f"  CorrCap-0.25 SR={pnl_stats(pnl_cc)['Sharpe']:.3f}" if compute_corrcap else "")
    )
    print(
        f"         Universe: {returns.shape[1]} instruments  "
        f"Period: {returns.index[0].date()} - {returns.index[-1].date()}"
    )


if __name__ == "__main__":
    main()
