"""MultiFactor CTA 国内期货组合策略回测入口。"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import pandas as pd

warnings.filterwarnings("ignore")

_HERE = Path(__file__).resolve().parent
_CTA_LAB = _HERE.parent
if str(_CTA_LAB) not in sys.path:
    sys.path.insert(0, str(_CTA_LAB))

from analysis.metrics import annual_stats, monthly_pivot, pnl_stats, sector_stats
from analysis.report.charts import (
    plot_annual_bar,
    plot_monthly_heatmap,
    plot_nav_with_drawdown,
    plot_sector_nav,
)
from analysis.report.output import BacktestOutput
from backtest import ProportionalCostModel
from backtest.vectorized import VectorizedBacktest
from data.loader import DataLoader, KlineSchema
from data.sources.parquet_source import ParquetSource
from strategies.context import StrategyContext
from strategies.implementations.multifactor_cta_backtest import MultiFactorCTAStrategy


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MultiFactor CTA 国内期货组合策略回测")
    p.add_argument(
        "--data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "china_daily_full"),
        help="china_daily_full/ 数据目录",
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "multifactor_cta_china"),
        help="输出根目录",
    )
    p.add_argument("--start", default=None, help="回测开始日期，例如 2005-01-01")
    p.add_argument("--end", default=None, help="回测结束日期，例如 2025-12-31")
    p.add_argument(
        "--short-filter-mode",
        choices=["none", "momentum_vote", "donchian"],
        default="momentum_vote",
        help="短周期过滤器模式，默认 momentum_vote",
    )
    p.add_argument("--trend-weight", type=float, default=2.0, help="趋势 sleeve 权重，默认2")
    p.add_argument("--cross-weight", type=float, default=1.0, help="截面动量 sleeve 权重，默认1")
    p.add_argument("--cross-lookback", type=int, default=240, help="截面动量回望期，默认240")
    p.add_argument("--cross-short-mean-window", type=int, default=120, help="截面动量短均值窗口，默认120")
    p.add_argument("--cross-vol-window", type=int, default=20, help="截面动量波动率窗口，默认20")
    p.add_argument(
        "--cross-weighting",
        choices=["global_equal", "global_inv_vol", "sector_inverse_vol"],
        default="global_equal",
        help="截面动量 sleeve 加权方式，默认 global_equal",
    )
    p.add_argument("--cross-sector-vol-halflife", type=int, default=21, help="行业中性 sleeve 波动率半衰期，默认21")
    p.add_argument("--top-pct", type=float, default=0.20, help="行业内截面动量做多比例，默认0.20")
    p.add_argument("--bottom-pct", type=float, default=0.20, help="行业内截面动量做空比例，默认0.20")
    p.add_argument("--smoothing-window", type=int, default=20, help="权重平滑窗口，默认20")
    p.add_argument("--max-abs-weight", type=float, default=0.10, help="单品种权重上限，默认0.10")
    p.add_argument("--max-gross-exposure", type=float, default=1.50, help="组合 gross 上限，默认1.50")
    p.add_argument("--target-vol", type=float, default=0.10, help="组合目标年化波动率，默认0.10")
    p.add_argument("--cost-bps", type=float, default=5.0, help="单边换手成本，单位bps，默认5")
    p.add_argument("--verbose", action="store_true", default=True)
    return p.parse_args()


def _turnover_cost_frame(turnover: pd.Series | None, cost_rate: float) -> pd.DataFrame:
    if turnover is None:
        return pd.DataFrame(columns=["turnover", "transaction_cost"])
    frame = turnover.to_frame("turnover")
    frame["transaction_cost"] = frame["turnover"] * cost_rate
    return frame


def _turnover_cost_summary(frame: pd.DataFrame, trading_days: int) -> dict[str, float]:
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

    out = BacktestOutput(
        args.out_dir,
        subdirs=["reports", "charts", "signals", "data"],
    )

    strategy = MultiFactorCTAStrategy(
        config={
            "short_filter_mode": args.short_filter_mode,
            "trend_weight": args.trend_weight,
            "cross_weight": args.cross_weight,
            "cross_lookback": args.cross_lookback,
            "cross_short_mean_window": args.cross_short_mean_window,
            "cross_vol_window": args.cross_vol_window,
            "cross_weighting": args.cross_weighting,
            "cross_sector_vol_halflife": args.cross_sector_vol_halflife,
            "top_pct": args.top_pct,
            "bottom_pct": args.bottom_pct,
            "smoothing_window": args.smoothing_window,
            "max_abs_weight": args.max_abs_weight,
            "max_gross_exposure": args.max_gross_exposure,
            "target_vol": args.target_vol,
            "transaction_cost_bps": args.cost_bps,
        }
    )

    loader = DataLoader(
        kline_source=ParquetSource(args.data_dir),
        kline_schema=KlineSchema.tushare(),
    )
    backtest = VectorizedBacktest(
        lag=1,
        vol_target=strategy.target_vol,
        vol_halflife=strategy.vol_halflife,
        trading_days=strategy.trading_days,
        cost_model=ProportionalCostModel(args.cost_bps / 10_000.0),
    )
    context = StrategyContext(
        loader=loader,
        sector_map=strategy.sector_map,
        backtest=backtest,
    )

    result = strategy.run_pipeline(
        context=context,
        start=args.start,
        end=args.end,
        verbose=args.verbose,
    )

    returns = result.returns
    positions = result.positions
    pnl = result.pnl
    sector_map = result.sector_map
    bt_result = result.backtest_result
    bt = context.backtest
    assert bt is not None

    out.save_parquet(returns, "data", "returns.parquet")
    out.save_json(
        {"symbols": returns.columns.tolist(), "sector_map": sector_map},
        "data", "asset_list.json",
    )
    out.save_parquet(result.trend_signal, "signals", "trend_signal.parquet")
    out.save_parquet(result.cross_signal, "signals", "cross_signal.parquet")
    out.save_parquet(result.blended_signal, "signals", "blended_signal.parquet")
    out.save_parquet(result.short_filter, "signals", "short_filter.parquet")
    out.save_parquet(result.filtered_signal, "signals", "filtered_signal.parquet")
    out.save_parquet(result.raw_positions, "signals", "raw_positions.parquet")
    out.save_parquet(result.trend_positions, "signals", "trend_positions.parquet")
    out.save_parquet(result.cross_positions, "signals", "cross_positions.parquet")
    out.save_parquet(positions, "signals", "positions.parquet")

    annual_df = annual_stats(pnl)
    monthly_df = monthly_pivot(pnl)
    sector_df = sector_stats(positions, returns, sector_map, bt)
    turnover_df = _turnover_cost_frame(
        bt_result.turnover_series if bt_result is not None else None,
        args.cost_bps / 10_000.0,
    )
    summary = pnl_stats(pnl)
    summary.update(_turnover_cost_summary(turnover_df, strategy.trading_days))
    summary_df = pd.DataFrame([summary]).rename(index={0: "MultiFactorCTA"})

    out.save_csv(annual_df, "reports", "annual.csv")
    out.save_csv(monthly_df, "reports", "monthly.csv")
    out.save_csv(sector_df, "reports", "sector_contribution.csv")
    out.save_csv(summary_df, "reports", "full_sample_summary.csv")
    out.save_csv(turnover_df, "reports", "turnover_cost.csv")

    out.save_fig(
        plot_nav_with_drawdown(
            {"MultiFactorCTA": pnl},
            title="China Futures MultiFactor CTA",
        ),
        "charts", "nav_multifactor_cta.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar(
            {"MultiFactorCTA": annual_df},
            title="China Futures MultiFactor CTA — Annual Returns",
        ),
        "charts", "annual_returns_bar.png", dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(
            monthly_df,
            title="China Futures MultiFactor CTA — Monthly Returns (%)",
        ),
        "charts", "monthly_heatmap.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_sector_nav(
            positions,
            returns,
            sector_map,
            bt,
            title="China Futures MultiFactor CTA — Sector NAV",
        ),
        "charts", "sector_nav.png", dpi=150,
    )

    print("\nFull sample summary:")
    print(summary_df.to_string())
    print("\nAll outputs:")
    out.summary()


if __name__ == "__main__":
    main()
