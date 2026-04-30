"""Short-term reversal CTA 中国期货回测入口。

策略逻辑：
  signal = -log(price_{t-skip} / price_{t-skip-window})
  做空近期涨幅最大品种，做多近期跌幅最大品种（逆势反转）。
  无需 far-leg 合约数据，仅使用主力连续合约收益率和持仓量。
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
from data.sources.column_keyed_source import ColumnKeyedSource
from data.sources.parquet_source import ParquetSource
from strategies.context import StrategyContext
from strategies.implementations.short_reversal_backtest import ShortReversalStrategy


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="短期反转 中国期货回测")
    p.add_argument(
        "--data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "china_daily_full"),
        help="china_daily_full/ 数据目录",
    )
    p.add_argument(
        "--contract-info",
        default=str(_CTA_LAB.parent / "market_data" / "contracts" / "china" / "contract_info.parquet"),
        help="合约元数据 parquet 路径",
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "short_reversal_china"),
        help="输出根目录",
    )
    p.add_argument("--start", default=None, help="回测开始日期，例如 2012-01-01")
    p.add_argument("--end", default=None, help="回测结束日期，例如 2025-12-31")
    p.add_argument(
        "--reversal-window", type=int, default=21,
        help="反转窗口（交易日），默认21（约1个月）",
    )
    p.add_argument(
        "--skip-days", type=int, default=1,
        help="信号窗口前跳过天数（微观结构缓冲），默认1",
    )
    p.add_argument(
        "--signal-clip", type=float, default=0.30,
        help="信号截断阈值（log单位），默认0.30",
    )
    p.add_argument("--min-obs", type=int, default=65, help="最少观测天数，默认65")
    p.add_argument("--min-listing-days", type=int, default=65, help="最少上市天数，默认65")
    p.add_argument(
        "--vol-scale-windows",
        default="20,60,120",
        help="波动率窗口，逗号分隔，默认20,60,120",
    )
    p.add_argument("--rebalance-buckets", type=int, default=20, help="轮动桶数，默认20")
    p.add_argument(
        "--selection-weighting",
        choices=["equal", "inv_vol"],
        default="inv_vol",
        help="截面权重方式，默认 inv_vol",
    )
    p.add_argument("--max-abs-weight", type=float, default=0.06, help="单品种权重上限，默认0.06")
    p.add_argument("--max-gross-exposure", type=float, default=1.0, help="组合 gross 上限，默认1.0")
    p.add_argument("--target-vol", type=float, default=0.05, help="组合目标年化波动率，默认0.05")
    p.add_argument("--cost-bps", type=float, default=5.0, help="单边换手成本，单位bps，默认5")
    p.add_argument("--verbose", action="store_true", default=True)
    return p.parse_args()


def _parse_int_list(text: str) -> list[int]:
    return [int(part.strip()) for part in text.split(",") if part.strip()]


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


def _load_lot_size_map(contract_info_path: str) -> dict[str, float]:
    try:
        df = pd.read_parquet(contract_info_path)
        lot_size = df.groupby("fut_code")["per_unit"].first().dropna()
        return lot_size.to_dict()
    except Exception as exc:
        print(f"[WARN] 无法加载 lot_size，将使用默认值 1.0: {exc}")
        return {}


def main() -> None:
    args = _parse_args()

    out = BacktestOutput(
        args.out_dir,
        subdirs=["reports", "charts", "signals", "data"],
    )

    strategy = ShortReversalStrategy(
        config={
            "reversal_window": args.reversal_window,
            "skip_days": args.skip_days,
            "signal_clip": args.signal_clip,
            "min_obs": args.min_obs,
            "min_listing_days": args.min_listing_days,
            "vol_scale_windows": _parse_int_list(args.vol_scale_windows),
            "rebalance_buckets": args.rebalance_buckets,
            "selection_weighting": args.selection_weighting,
            "max_abs_weight": args.max_abs_weight,
            "max_gross_exposure": args.max_gross_exposure,
            "target_vol": args.target_vol,
            "transaction_cost_bps": args.cost_bps,
        }
    )

    contract_source = ColumnKeyedSource(args.contract_info, filter_col="fut_code")
    loader = DataLoader(
        kline_source=ParquetSource(args.data_dir),
        contract_source=contract_source,
        instrument_source=contract_source,
        kline_schema=KlineSchema.tushare(),
        contract_schema=ContractSchema.tushare(),
        instrument_schema=InstrumentSchema.china_from_contracts(),
    )
    backtest = VectorizedBacktest(
        lag=1,
        vol_target=None,
        vol_halflife=strategy.vol_halflife,
        trading_days=strategy.trading_days,
        max_gross_exposure=strategy.max_gross_exposure,
        cost_model=ProportionalCostModel(args.cost_bps / 10_000.0),
    )
    context = StrategyContext(loader=loader, sector_map={}, backtest=backtest)

    lot_size_map = _load_lot_size_map(args.contract_info)
    if lot_size_map:
        print(f"Loaded lot_size for {len(lot_size_map)} instruments from contract_info.")

    result = strategy.run_pipeline(
        context=context,
        start=args.start,
        end=args.end,
        verbose=args.verbose,
        lot_size_map=lot_size_map,
    )

    returns = result.returns
    pnl = result.pnl
    positions = result.positions
    bt_result = result.backtest_result

    out.save_parquet(returns, "data", "returns.parquet")
    out.save_json({"symbols": returns.columns.tolist()}, "data", "asset_list.json")
    out.save_parquet(result.settle_prices, "signals", "settle_prices.parquet")
    out.save_parquet(result.signal, "signals", "signal.parquet")
    out.save_parquet(result.tradable_mask, "signals", "tradable_mask.parquet")
    out.save_parquet(result.sigma_max, "signals", "sigma_max.parquet")
    out.save_parquet(result.raw_positions, "signals", "raw_positions.parquet")
    out.save_parquet(positions, "signals", "positions.parquet")

    annual_df = annual_stats(pnl)
    monthly_df = monthly_pivot(pnl)
    turnover_df = _turnover_cost_frame(
        bt_result.turnover_series if bt_result is not None else None,
        args.cost_bps / 10_000.0,
    )
    summary = pnl_stats(pnl)
    summary.update(_turnover_cost_summary(turnover_df, strategy.trading_days))
    summary.update(
        {
            "SignalCoverage(%)": round(float(result.signal.notna().mean().mean() * 100.0), 2),
            "LiveDays": int((positions.abs().sum(axis=1) > 0).sum()),
            "Symbols": int(returns.shape[1]),
            "ReversalWindow": strategy.reversal_window,
            "SkipDays": strategy.skip_days,
        }
    )
    summary_df = pd.DataFrame([summary]).rename(index={0: "ShortReversal"})

    out.save_csv(annual_df, "reports", "annual.csv")
    out.save_csv(monthly_df, "reports", "monthly.csv")
    out.save_csv(summary_df, "reports", "full_sample_summary.csv")
    out.save_csv(turnover_df, "reports", "turnover_cost.csv")

    out.save_fig(
        plot_nav_with_drawdown(
            {"ShortReversal": pnl},
            title=f"China Futures Short-Term Reversal ({args.reversal_window}d)",
        ),
        "charts", "nav_short_reversal.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar(
            {"ShortReversal": annual_df},
            title=f"China Futures Short-Term Reversal — Annual Returns ({args.reversal_window}d)",
        ),
        "charts", "annual_returns_bar.png", dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(
            monthly_df,
            title=f"China Futures Short-Term Reversal — Monthly Returns % ({args.reversal_window}d)",
        ),
        "charts", "monthly_heatmap.png", dpi=150, bbox_inches="tight",
    )

    print("\nFull sample summary:")
    print(summary_df.to_string())
    print("\nAll outputs:")
    out.summary()


if __name__ == "__main__":
    main()
