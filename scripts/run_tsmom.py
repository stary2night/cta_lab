"""TSMOM 时序动量策略回测入口。

参考：Moskowitz, Ooi & Pedersen — "Time Series Momentum" (JFE, 2012)
信号：sign(累计对数收益，lookback=252 交易日)
定仓：signal / sigma_ewma（基准）；可选 CorrCap 变体

用法（在 cta_lab/ 目录下执行）：
    python scripts/run_tsmom.py \\
        --data-dir /path/to/market_data/kline/china_daily_full \\
        --out-dir  /path/to/output

默认输出目录：<cta_lab 上级目录>/research_outputs/tsmom_china/

输出文件：
  reports/baseline_annual.csv
  reports/decade_breakdown.csv
  reports/sector_contribution.csv
  reports/asset_standalone_sr.csv
  reports/full_sample_summary.csv
  reports/baseline_monthly.csv
  charts/nav_tsmom.png
  charts/annual_returns_bar.png
  charts/monthly_heatmap_baseline.png
  charts/rolling_sharpe.png
  charts/sector_nav.png
  signals/tsmom_positions.parquet

可选开启 StrategyReport：
    python scripts/run_tsmom.py --analysis --fee-rate 0.0003
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
from backtest import ProportionalCostModel, turnover_cost_frame, turnover_cost_summary
from backtest.vectorized import VectorizedBacktest
from data.loader import DataLoader, KlineSchema
from data.sources.parquet_source import ParquetSource
from strategies.context import StrategyContext
from strategies.implementations.tsmom_backtest.strategy import TSMOMStrategy


# ── CLI 参数 ──────────────────────────────────────────────────────────────────

def _parse_args(
    default_out_dir: str | Path | None = None,
    default_fee_rate: float = 0.0,
    default_analysis: bool = False,
    description: str = "TSMOM 国内期货时序动量策略回测",
) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "--data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "china_daily_full"),
        help="china_daily_full/ 数据目录",
    )
    p.add_argument(
        "--out-dir",
        default=str(default_out_dir or _CTA_LAB.parent / "research_outputs" / "tsmom_china"),
        help="输出根目录",
    )
    p.add_argument(
        "--lookback",
        type=int,
        default=252,
        help="TSMOM 回望期（交易日），默认 252（≈12 个月）",
    )
    p.add_argument(
        "--mode",
        type=str,
        default="binary",
        choices=["binary", "linear", "nonlinear"],
        help="信号模式，默认 binary",
    )
    p.add_argument(
        "--corrcap",
        action="store_true",
        default=False,
        help="计算 CorrCap 变体（较慢，默认关闭）",
    )
    p.add_argument(
        "--fee-rate",
        type=float,
        default=default_fee_rate,
        help="单边换手成本率，默认 0 = 不收费",
    )
    p.add_argument(
        "--cost-bps",
        type=float,
        default=None,
        help="单边换手成本，单位 bps；若提供则优先于 --fee-rate",
    )
    p.add_argument(
        "--analysis",
        dest="run_analysis",
        action="store_true",
        default=default_analysis,
        help="运行 StrategyReport 分析层",
    )
    p.add_argument(
        "--no-analysis",
        dest="run_analysis",
        action="store_false",
        help="不运行 StrategyReport 分析层",
    )
    p.add_argument("--verbose", action="store_true", default=True)
    return p.parse_args()


# ── 主流程 ────────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    compute_corrcap = args.corrcap
    cost_rate = args.cost_bps / 10_000.0 if args.cost_bps is not None else args.fee_rate

    out = BacktestOutput(
        args.out_dir,
        subdirs=["reports", "charts", "signals", "data", "analysis"],
    )

    strategy = TSMOMStrategy(config={"lookback": args.lookback}, fee_rate=cost_rate)
    loader = DataLoader(
        kline_source=ParquetSource(args.data_dir),
        kline_schema=KlineSchema.tushare(),
    )
    backtest = VectorizedBacktest(
        lag=1,
        vol_target=strategy.target_vol,
        vol_halflife=strategy.vol_halflife,
        trading_days=strategy.trading_days,
        cost_model=ProportionalCostModel(cost_rate),
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
        mode=args.mode,
        run_analysis=args.run_analysis,
        benchmark_returns=None,
        output_dir=out["analysis"] if args.run_analysis else None,
    )

    returns      = result.returns
    baseline_pos = result.baseline_pos
    pnl_base     = result.pnl_baseline
    pnl_cc       = result.pnl_corrcap if compute_corrcap else None
    sector_map   = result.sector_map
    bt           = context.backtest
    assert bt is not None

    # ── Raw data ──────────────────────────────────────────────────────────────
    out.save_parquet(returns, "data", "returns.parquet")
    out.save_json(
        {"symbols": returns.columns.tolist(), "sector_map": sector_map},
        "data", "asset_list.json",
    )
    out.save_parquet(result.signal,  "signals", "tsmom_signal.parquet")
    out.save_parquet(baseline_pos,   "signals", "tsmom_positions.parquet")
    turnover_base = turnover_cost_frame(
        baseline_pos,
        cost_rate,
        lag=bt.lag,
    )
    out.save_csv(turnover_base, "reports", "baseline_turnover_cost.csv")

    if args.run_analysis and result.report:
        print("\n" + "=" * 65)
        print("StrategyReport")
        print("=" * 65)
        print("  result.report.keys() ->", sorted(result.report.keys()))
        print(f"  analysis outputs: {out['analysis']}/")

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

    print("\n" + "=" * 65)
    print("Section 5: Full-Sample Summary")
    print("=" * 65)
    base_stats = pnl_stats(pnl_base)
    base_stats.update(
        turnover_cost_summary(
            baseline_pos,
            cost_rate,
            lag=bt.lag,
            trading_days=strategy.trading_days,
        )
    )
    base_stats["Label"] = "TSMOM Baseline"
    full_rows = [base_stats]
    if compute_corrcap and pnl_cc is not None and not pnl_cc.empty:
        cc_stats = pnl_stats(pnl_cc)
        cc_stats.update(
            turnover_cost_summary(
                result.corrcap_pos,
                cost_rate,
                lag=bt.lag,
                trading_days=strategy.trading_days,
            )
        )
        cc_stats["Label"] = f"CorrCap-{strategy.corr_cap}"
        full_rows.append(cc_stats)
    full_summary = pd.DataFrame(full_rows).set_index("Label")
    print(full_summary.to_string())

    print("\n" + "=" * 65)
    print("Section 6: Monthly Returns (Baseline)")
    print("=" * 65)
    mpiv_base = monthly_pivot(pnl_base)
    print(mpiv_base.to_string())

    # ── CSV reports ───────────────────────────────────────────────────────────
    out.save_csv(tbl_base,    "reports", "baseline_annual.csv")
    out.save_csv(decade_df,   "reports", "decade_breakdown.csv")
    out.save_csv(sector_df,   "reports", "sector_contribution.csv")
    out.save_csv(asset_df,    "reports", "asset_standalone_sr.csv")
    out.save_csv(full_summary,"reports", "full_sample_summary.csv")
    out.save_csv(mpiv_base,   "reports", "baseline_monthly.csv")
    if compute_corrcap and pnl_cc is not None and not pnl_cc.empty:
        out.save_csv(
            turnover_cost_frame(result.corrcap_pos, cost_rate, lag=bt.lag),
            "reports",
            "corrcap_turnover_cost.csv",
        )

    # ── Charts ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Section 7: Charts")
    print("=" * 65)

    _nav_pnls = {"Baseline TSMOM": pnl_base}
    if pnl_cc is not None and not pnl_cc.empty:
        _nav_pnls["CorrCap"] = pnl_cc

    out.save_fig(
        plot_nav_with_drawdown(
            _nav_pnls,
            title=f"China Domestic Futures — TSMOM (lookback={args.lookback}d)\n"
                  f"mode={args.mode}, EWMA vol targeting 10% ann.",
        ),
        "charts", "nav_tsmom.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar(
            {"Baseline TSMOM": tbl_base},
            title="China Futures TSMOM — Annual Returns (Baseline)",
        ),
        "charts", "annual_returns_bar.png", dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(
            mpiv_base,
            title="China Futures TSMOM — Baseline Monthly Returns (%)",
        ),
        "charts", "monthly_heatmap_baseline.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_rolling_sharpe(
            {"Baseline TSMOM": pnl_base},
            title="China Futures TSMOM — Rolling 1-Year Sharpe",
        ),
        "charts", "rolling_sharpe.png", dpi=150,
    )
    out.save_fig(
        plot_sector_nav(
            baseline_pos, returns, sector_map, bt,
            title="China Futures TSMOM — Sector NAV (Baseline)",
        ),
        "charts", "sector_nav.png", dpi=150,
    )

    print(f"\nAll outputs:")
    out.summary()
    s_base = pnl_stats(pnl_base)
    print(
        f"\nSummary: TSMOM Baseline SR={s_base['Sharpe']:.3f}  "
        f"Return={s_base['Return(%)']:.1f}%  MaxDD={s_base['MaxDD(%)']:.1f}%"
    )
    print(
        f"         Universe: {returns.shape[1]} instruments  "
        f"Period: {returns.index[0].date()} - {returns.index[-1].date()}"
    )


def main() -> None:
    run(_parse_args())


if __name__ == "__main__":
    main()
