"""Skew reversal CTA 海外期货回测入口。

使用 overseas_daily_full/ 多合约原始数据，通过 DataLoader OI-max 规则构建连续合约，
对 46 个海外期货品种运行偏度反转策略回测（可记录 2010 年以来的表现）。

与国内版的主要差异：
  - KlineSchema.overseas() + ContractSchema.overseas()
  - 无 CNY 流动性阈值（设为 0），依赖挂牌天数过滤和 OI 持仓量门控
  - close/settle 无区分（overseas 数据均为 settle），禁用 close-settle 融合修正
  - 默认成本 3 bps（海外主流品种流动性更好）
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
from analysis.report.charts import (
    plot_annual_bar,
    plot_monthly_heatmap,
    plot_nav_with_drawdown,
)
from analysis.report.output import BacktestOutput
from backtest import ProportionalCostModel
from backtest.vectorized import VectorizedBacktest
from data.loader import ContractSchema, DataLoader, InstrumentSchema, KlineSchema
from data.sources.parquet_source import ParquetSource
from strategies.context import StrategyContext
from strategies.implementations.skew_reversal_backtest import SkewReversalStrategy

# 排除加密货币、波动率指数和近期才上市的品种（历史数据不足，行为特殊）
_OVERSEAS_EXCLUDE = {"BTC", "VX", "HTI", "A01"}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Skew reversal 海外期货回测")
    p.add_argument(
        "--kline-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "overseas_daily_full"),
        help="overseas_daily_full/ 数据目录",
    )
    p.add_argument(
        "--contract-dir",
        default=str(_CTA_LAB.parent / "market_data" / "contracts" / "overseas"),
        help="海外合约元数据目录",
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "skew_reversal_overseas"),
        help="输出根目录",
    )
    p.add_argument("--start", default=None, help="回测开始日期，例如 2010-01-01")
    p.add_argument("--end", default=None, help="回测结束日期，例如 2025-12-31")
    p.add_argument("--target-vol", type=float, default=0.05, help="组合目标年化波动率，默认0.05")
    p.add_argument("--top-pct", type=float, default=0.25, help="截面做空比例")
    p.add_argument("--bottom-pct", type=float, default=0.25, help="截面做多比例")
    p.add_argument("--oi-lookback", type=int, default=10, help="持仓量变化窗口")
    p.add_argument("--rebalance-buckets", type=int, default=20, help="轮动桶数")
    p.add_argument("--smoothing-window", type=int, default=20, help="目标权重平滑窗口")
    p.add_argument("--cost-bps", type=float, default=3.0, help="单边换手成本bps，默认3")
    p.add_argument(
        "--momentum-filter-window", type=int, default=0,
        help="动量确认过滤窗口(交易日)，0=禁用",
    )
    p.add_argument(
        "--momentum-filter-threshold", type=float, default=0.05,
        help="动量过滤阈值，默认0.05",
    )
    p.add_argument("--verbose", action="store_true", default=True)
    return p.parse_args()


def _turnover_cost_frame(turnover: pd.Series | None, cost_rate: float) -> pd.DataFrame:
    if turnover is None:
        return pd.DataFrame(columns=["turnover", "transaction_cost"])
    frame = turnover.to_frame("turnover")
    frame["transaction_cost"] = frame["turnover"] * cost_rate
    return frame


def _turnover_cost_summary(frame: pd.DataFrame, trading_days: int) -> dict:
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

    # 海外版配置：禁用 CNY 流动性阈值，关闭 close-settle 混合修正
    strategy = SkewReversalStrategy(
        config={
            "top_pct": args.top_pct,
            "bottom_pct": args.bottom_pct,
            "oi_lookback": args.oi_lookback,
            "rebalance_buckets": args.rebalance_buckets,
            "smoothing_window": args.smoothing_window,
            # overseas 数据 close == settle，blend 无意义
            "close_settle_blend_alpha": 0.0,
            "use_close_settle_correction": False,
            # 海外用合约数量 × 价格，不做 CNY 阈值过滤
            "liquidity_threshold_pre2017": 0.0,
            "liquidity_threshold_post2017": 0.0,
            "target_vol": args.target_vol,
            "transaction_cost_bps": args.cost_bps,
            "momentum_filter_window": args.momentum_filter_window,
            "momentum_filter_threshold": args.momentum_filter_threshold,
            "exclude": sorted(_OVERSEAS_EXCLUDE),
        }
    )

    loader = DataLoader(
        kline_source=ParquetSource(args.kline_dir),
        kline_schema=KlineSchema.overseas(),
        contract_source=ParquetSource(args.contract_dir),
        contract_schema=ContractSchema.overseas(),
        instrument_schema=InstrumentSchema.overseas_from_contracts(),
    )
    backtest = VectorizedBacktest(
        lag=1,
        vol_target=strategy.target_vol,
        vol_halflife=strategy.vol_halflife,
        trading_days=strategy.trading_days,
        max_gross_exposure=strategy.max_gross_exposure,
        cost_model=ProportionalCostModel(args.cost_bps / 10_000.0),
    )
    context = StrategyContext(loader=loader, sector_map={}, backtest=backtest)

    # lot_size_map={} → 所有品种 multiplier=1.0；阈值为 0 时乘数无影响
    result = strategy.run_pipeline(
        context=context,
        start=args.start,
        end=args.end,
        verbose=args.verbose,
        lot_size_map={},
    )

    returns = result.returns
    pnl = result.pnl
    positions = result.positions
    bt_result = result.backtest_result

    out.save_parquet(returns, "data", "returns.parquet")
    out.save_json({"symbols": returns.columns.tolist()}, "data", "asset_list.json")
    out.save_parquet(result.settle_skew, "signals", "settle_skew.parquet")
    out.save_parquet(result.skew_factor, "signals", "skew_factor.parquet")
    out.save_parquet(result.oi_change, "signals", "oi_change.parquet")
    out.save_parquet(result.raw_positions, "signals", "raw_positions.parquet")
    out.save_parquet(result.smoothed_positions, "signals", "smoothed_positions.parquet")
    out.save_parquet(result.vol_scale, "signals", "vol_scale.parquet")
    out.save_parquet(positions, "signals", "positions.parquet")

    annual_df = annual_stats(pnl)
    monthly_df = monthly_pivot(pnl)
    turnover_df = _turnover_cost_frame(
        bt_result.turnover_series if bt_result is not None else None,
        args.cost_bps / 10_000.0,
    )
    summary = pnl_stats(pnl)
    summary.update(_turnover_cost_summary(turnover_df, strategy.trading_days))
    summary_df = pd.DataFrame([summary]).rename(index={0: "SkewReversal_Overseas"})

    out.save_csv(annual_df, "reports", "annual.csv")
    out.save_csv(monthly_df, "reports", "monthly.csv")
    out.save_csv(summary_df, "reports", "full_sample_summary.csv")
    out.save_csv(turnover_df, "reports", "turnover_cost.csv")

    out.save_fig(
        plot_nav_with_drawdown(
            {"SkewReversal_Overseas": pnl},
            title="Overseas Futures Skew Reversal",
        ),
        "charts", "nav_skew_reversal.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar(
            {"SkewReversal_Overseas": annual_df},
            title="Overseas Futures Skew Reversal — Annual Returns",
        ),
        "charts", "annual_returns_bar.png", dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(
            monthly_df,
            title="Overseas Futures Skew Reversal — Monthly Returns (%)",
        ),
        "charts", "monthly_heatmap.png", dpi=150, bbox_inches="tight",
    )

    if not annual_df.empty:
        print("\nAnnual returns (%):")
        print(annual_df.round(2).to_string())
    print(f"\nSaved outputs to: {Path(args.out_dir).resolve()}")


if __name__ == "__main__":
    main()
