"""CrossMOM 国内期货相对动量策略回测入口。"""

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
from backtest import ProportionalCostModel, turnover_cost_frame, turnover_cost_summary
from backtest.vectorized import VectorizedBacktest
from data.loader import DataLoader, KlineSchema
from data.sources.parquet_source import ParquetSource
from strategies.context import StrategyContext
from strategies.implementations.crossmom_backtest import CrossMOMStrategy


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="CrossMOM 国内期货相对动量策略回测")
    p.add_argument(
        "--data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "china_daily_full"),
        help="china_daily_full/ 数据目录",
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "crossmom_china"),
        help="输出根目录",
    )
    p.add_argument("--lookback", type=int, default=252, help="回望期，默认252")
    p.add_argument("--top-pct", type=float, default=0.30, help="多头截面分位，默认0.30")
    p.add_argument("--bottom-pct", type=float, default=0.30, help="空头截面分位，默认0.30")
    p.add_argument("--cost-bps", type=float, default=0.0, help="单边换手成本，单位 bps，默认0")
    p.add_argument("--verbose", action="store_true", default=True)
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    out = BacktestOutput(
        args.out_dir,
        subdirs=["reports", "charts", "signals", "data"],
    )

    strategy = CrossMOMStrategy(
        config={
            "lookback": args.lookback,
            "top_pct": args.top_pct,
            "bottom_pct": args.bottom_pct,
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

    result = strategy.run_pipeline(context=context, verbose=args.verbose)

    returns = result.returns
    sector_map = result.sector_map
    signal = result.signal
    positions = result.positions
    pnl = result.pnl
    bt = context.backtest
    assert bt is not None

    out.save_parquet(returns, "data", "returns.parquet")
    out.save_json(
        {"symbols": returns.columns.tolist(), "sector_map": sector_map},
        "data", "asset_list.json",
    )
    out.save_parquet(signal, "signals", "crossmom_signal.parquet")
    out.save_parquet(positions, "signals", "crossmom_positions.parquet")

    annual_df = annual_stats(pnl)
    monthly_df = monthly_pivot(pnl)
    sector_df = sector_stats(positions, returns, sector_map, bt)
    turnover_df = turnover_cost_frame(
        positions,
        args.cost_bps / 10_000.0,
        lag=bt.lag,
    )
    summary = pnl_stats(pnl)
    summary.update(
        turnover_cost_summary(
            positions,
            args.cost_bps / 10_000.0,
            lag=bt.lag,
            trading_days=strategy.trading_days,
        )
    )
    summary_df = pd.DataFrame([summary]).rename(index={0: "CrossMOM"})

    out.save_csv(annual_df, "reports", "annual.csv")
    out.save_csv(monthly_df, "reports", "monthly.csv")
    out.save_csv(sector_df, "reports", "sector_contribution.csv")
    out.save_csv(summary_df, "reports", "full_sample_summary.csv")
    out.save_csv(turnover_df, "reports", "turnover_cost.csv")

    out.save_fig(
        plot_nav_with_drawdown({"CrossMOM": pnl}, title="China Futures CrossMOM"),
        "charts", "nav_crossmom.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar({"CrossMOM": annual_df}, title="China Futures CrossMOM — Annual Returns"),
        "charts", "annual_returns_bar.png", dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(monthly_df, title="China Futures CrossMOM — Monthly Returns (%)"),
        "charts", "monthly_heatmap.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_sector_nav(positions, returns, sector_map, bt, title="China Futures CrossMOM — Sector NAV"),
        "charts", "sector_nav.png", dpi=150,
    )

    print("\nAll outputs:")
    out.summary()


if __name__ == "__main__":
    main()
