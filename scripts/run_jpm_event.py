"""JPM 国内期货趋势策略事件驱动回测入口。

用法（在 cta_lab/ 目录下执行）：
    python scripts/run_jpm_event.py \\
        --data-dir /path/to/market_data/kline/china_daily_full \\
        --out-dir  /path/to/output \\
        --mode all

默认输出目录：<cta_lab 上级目录>/research_outputs/jpm_trend_event_china/
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

_HERE = Path(__file__).resolve().parent
_CTA_LAB = _HERE.parent
if str(_CTA_LAB) not in sys.path:
    sys.path.insert(0, str(_CTA_LAB))

from analysis.metrics import annual_stats, monthly_pivot, pnl_stats
from analysis.report.charts import plot_nav_with_drawdown, plot_annual_bar, plot_monthly_heatmap
from analysis.report.output import BacktestOutput
from backtest import FixedBpsSlippage, ProportionalCostModel
from backtest.event import MarketDataPortal
from data.loader import DataLoader, KlineSchema
from data.sources.parquet_source import ParquetSource
from strategies.context import StrategyContext
from strategies.implementations.jpm_trend_trade import (
    JPMEventDrivenStrategy,
    JPMTrendStrategy,
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="JPM 国内期货趋势策略事件驱动回测")
    p.add_argument(
        "--data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "china_daily_full"),
        help="china_daily_full/ 数据目录",
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "jpm_trend_event_china"),
        help="输出根目录",
    )
    p.add_argument(
        "--mode",
        choices=["baseline", "corrcap", "all"],
        default="all",
        help="运行 baseline、corrcap 或两者，默认 all",
    )
    p.add_argument("--start", default=None, help="回测开始日期，例如 2020-01-01")
    p.add_argument("--end", default=None, help="回测结束日期，例如 2025-12-31")
    p.add_argument("--rebalance-every", type=int, default=1, help="每 N 个交易日调仓，默认每日")
    p.add_argument("--initial-cash", type=float, default=1.0, help="初始资金/NAV，默认 1.0")
    p.add_argument("--commission-rate", type=float, default=0.0, help="成交佣金率，默认 0")
    p.add_argument("--commission-bps", type=float, default=None, help="成交费率，单位 bps；优先于 --commission-rate，未指定时使用 JPMConfig")
    p.add_argument("--slippage-bps", type=float, default=0.0, help="固定滑点，单位 bps，默认 0")
    p.add_argument(
        "--no-vol-target",
        action="store_true",
        help="baseline 模式关闭事件驱动版 ex-ante vol-targeting",
    )
    p.add_argument("--verbose", action="store_true", default=True)
    return p.parse_args()


def _load_returns(
    strategy: JPMTrendStrategy,
    data_dir: str | Path,
    *,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    loader = DataLoader(
        kline_source=ParquetSource(data_dir),
        kline_schema=KlineSchema.tushare(),
    )
    context = StrategyContext(
        loader=loader,
        sector_map=strategy.sector_map,
    )
    return context.load_returns_matrix(
        start=start,
        end=end,
        min_obs=strategy.min_obs,
        exclude=strategy.exclude,
    )


def _run_event_backtest(
    *,
    base_strategy: JPMTrendStrategy,
    returns: pd.DataFrame,
    mode: str,
    rebalance_every: int,
    apply_vol_target: bool,
    initial_cash: float,
    commission_rate: float,
    commission_bps: float | None,
    slippage_bps: float,
):
    strategy = JPMEventDrivenStrategy(
        strategy=base_strategy,
        event_config={
            "mode": mode,
            "rebalance_every": rebalance_every,
            "apply_vol_target": apply_vol_target,
        },
    )
    portal = MarketDataPortal.from_returns(returns)
    config_commission_bps = base_strategy.transaction_cost_bps
    effective_commission_rate = (
        commission_bps / 10_000.0
        if commission_bps is not None
        else commission_rate if commission_rate > 0
        else config_commission_bps / 10_000.0
    )
    return strategy.run_event_backtest(
        data_portal=portal,
        initial_cash=initial_cash,
        cost_model=ProportionalCostModel(effective_commission_rate),
        slippage_model=FixedBpsSlippage(slippage_bps),
    )


def main() -> None:
    args = _parse_args()
    modes = ["baseline", "corrcap"] if args.mode == "all" else [args.mode]

    out = BacktestOutput(
        args.out_dir,
        subdirs=["reports", "charts", "signals", "data"],
    )

    base_strategy = JPMTrendStrategy()

    if args.verbose:
        print("=" * 65)
        print("Step 1: Load china_daily_full returns")
        print("=" * 65)
    returns = _load_returns(base_strategy, args.data_dir, start=args.start, end=args.end)
    if returns.empty:
        raise RuntimeError("No returns loaded. Check data_dir and symbols.")

    if args.verbose:
        print(
            f"\nReturns matrix: {returns.shape}  "
            f"({returns.index[0].date()} - {returns.index[-1].date()})"
        )

    out.save_parquet(returns, "data", "returns.parquet")
    out.save_json(
        {
            "symbols": returns.columns.tolist(),
            "mode": args.mode,
            "start": args.start,
            "end": args.end,
            "commission_rate": args.commission_rate,
            "commission_bps": args.commission_bps,
            "slippage_bps": args.slippage_bps,
        },
        "data",
        "asset_list.json",
    )

    result_by_mode = {}
    annual_by_mode = {}
    summary_rows = []

    for mode in modes:
        if args.verbose:
            print("\n" + "=" * 65)
            print(f"Step 2: Run event-driven JPM strategy [{mode}]")
            print("=" * 65)

        result = _run_event_backtest(
            base_strategy=base_strategy,
            returns=returns,
            mode=mode,
            rebalance_every=args.rebalance_every,
            apply_vol_target=not args.no_vol_target,
            initial_cash=args.initial_cash,
            commission_rate=args.commission_rate,
            commission_bps=args.commission_bps,
            slippage_bps=args.slippage_bps,
        )
        result_by_mode[mode] = result

        pnl = result.returns.iloc[1:]
        annual_df = annual_stats(pnl)
        monthly_df = monthly_pivot(pnl)
        summary = pnl_stats(pnl)
        if result.turnover_series is not None and not result.turnover_series.empty:
            summary["AvgTurnover(%)"] = round(float(result.turnover_series.mean() * 100.0), 2)
            summary["AnnTurnover(x)"] = round(float(result.turnover_series.mean() * base_strategy.trading_days), 2)
        if result.fee_log is not None and not result.fee_log.empty:
            if "total_cost" in result.fee_log:
                total_cost = result.fee_log["total_cost"]
            else:
                total_cost = result.fee_log.sum(axis=1)
            summary["TotalCost(%)"] = round(float(total_cost.sum() / args.initial_cash * 100.0), 2)
            summary["AnnCost(%)"] = round(float(total_cost.mean() / args.initial_cash * base_strategy.trading_days * 100.0), 2)
        summary["Label"] = mode
        summary_rows.append(summary)
        annual_by_mode[mode] = annual_df

        out.save_csv(annual_df, "reports", f"{mode}_annual.csv")
        out.save_csv(monthly_df, "reports", f"{mode}_monthly.csv")
        if result.positions_df is not None:
            out.save_parquet(result.positions_df, "signals", f"{mode}_positions.parquet")
        if result.turnover_series is not None:
            out.save_csv(
                result.turnover_series.to_frame("turnover"),
                "reports",
                f"{mode}_turnover.csv",
            )
        if result.fee_log is not None:
            out.save_csv(result.fee_log, "reports", f"{mode}_fees.csv")

        print(f"\n{mode} annual performance:")
        print(annual_df.drop(columns=["Days"], errors="ignore").to_string())

    summary_df = pd.DataFrame(summary_rows).set_index("Label")
    out.save_csv(summary_df, "reports", "full_sample_summary.csv")

    pnl_map = {
        mode: result.returns.iloc[1:]
        for mode, result in result_by_mode.items()
    }
    out.save_fig(
        plot_nav_with_drawdown(
            pnl_map,
            title="China Domestic Futures Trend - Event-Driven JPM",
        ),
        "charts",
        "nav_jpm_event.png",
        dpi=150,
        bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar(
            annual_by_mode,
            title="Event-Driven JPM - Annual Returns",
        ),
        "charts",
        "annual_returns_bar.png",
        dpi=150,
    )
    for mode, result in result_by_mode.items():
        out.save_fig(
            plot_monthly_heatmap(
                monthly_pivot(result.returns.iloc[1:]),
                title=f"Event-Driven JPM {mode} - Monthly Returns (%)",
            ),
            "charts",
            f"monthly_heatmap_{mode}.png",
            dpi=150,
            bbox_inches="tight",
        )

    print("\n" + "=" * 65)
    print("Full-sample summary")
    print("=" * 65)
    print(summary_df.to_string())

    print("\nAll outputs:")
    out.summary()


if __name__ == "__main__":
    main()
