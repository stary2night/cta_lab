"""Carry (roll yield) CTA 中国期货回测入口。

策略逻辑：
  signal = log(近月价格 / 远月价格)，截断至 ±carry_clip 后截面排序。
  做多 carry 最高（逆价差最深）品种，做空 carry 最低（正价差最深）品种。
  不对自身历史 z-score 归一化 —— 这是与 basis_value 的核心区别。
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
from strategies.implementations.carry_backtest import CarryStrategy


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Carry 中国期货回测（展期收益）")
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
        default=str(_CTA_LAB.parent / "research_outputs" / "carry_china"),
        help="输出根目录",
    )
    p.add_argument(
        "--prebuilt-dir",
        default=None,
        help="预构建 near/far 矩阵目录；提供后将跳过运行时合约链构建",
    )
    p.add_argument("--start", default=None, help="回测开始日期，例如 2012-01-01")
    p.add_argument("--end", default=None, help="回测结束日期，例如 2025-12-31")
    p.add_argument("--min-obs", type=int, default=65, help="最少观测天数，默认65")
    p.add_argument("--min-listing-days", type=int, default=65, help="最少上市天数，默认65")
    p.add_argument(
        "--carry-clip", type=float, default=0.30,
        help="carry 截断阈值（log 单位），默认0.30（约±30%%年化）",
    )
    p.add_argument(
        "--active-oi-pct-threshold", type=float, default=0.05,
        help="远月最小持仓量占比，默认0.05",
    )
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

    strategy = CarryStrategy(
        config={
            "min_obs": args.min_obs,
            "carry_clip": args.carry_clip,
            "active_oi_pct_threshold": args.active_oi_pct_threshold,
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
        prebuilt_dir=args.prebuilt_dir,
    )

    returns = result.returns
    pnl = result.pnl
    positions = result.positions
    bt_result = result.backtest_result

    out.save_parquet(returns, "data", "returns.parquet")
    out.save_json({"symbols": returns.columns.tolist()}, "data", "asset_list.json")
    out.save_parquet(result.near_returns, "signals", "near_returns.parquet")
    out.save_parquet(result.far_returns, "signals", "far_returns.parquet")
    out.save_parquet(result.near_prices, "signals", "near_prices.parquet")
    out.save_parquet(result.far_prices, "signals", "far_prices.parquet")
    out.save_parquet(result.far_oi_share, "signals", "far_oi_share.parquet")
    out.save_parquet(result.term_structure, "signals", "term_structure.parquet")
    out.save_parquet(result.carry, "signals", "carry.parquet")
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
            "FarCoverage(%)": round(float(result.far_prices.notna().mean().mean() * 100.0), 2),
            "CarryCoverage(%)": round(float(result.carry.notna().mean().mean() * 100.0), 2),
            "AvgBackwardationPct(%)": round(
                float((result.carry > 0).sum(axis=1).div(
                    result.carry.notna().sum(axis=1).replace(0, float("nan"))
                ).mean() * 100.0), 2,
            ),
            "LiveDays": int((positions.abs().sum(axis=1) > 0).sum()),
            "Symbols": int(returns.shape[1]),
        }
    )
    summary_df = pd.DataFrame([summary]).rename(index={0: "Carry"})

    out.save_csv(annual_df, "reports", "annual.csv")
    out.save_csv(monthly_df, "reports", "monthly.csv")
    out.save_csv(summary_df, "reports", "full_sample_summary.csv")
    out.save_csv(turnover_df, "reports", "turnover_cost.csv")

    out.save_fig(
        plot_nav_with_drawdown(
            {"Carry": pnl},
            title="China Futures Carry (Roll Yield)",
        ),
        "charts", "nav_carry.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar(
            {"Carry": annual_df},
            title="China Futures Carry — Annual Returns",
        ),
        "charts", "annual_returns_bar.png", dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(
            monthly_df,
            title="China Futures Carry — Monthly Returns (%)",
        ),
        "charts", "monthly_heatmap.png", dpi=150, bbox_inches="tight",
    )

    print("\nFull sample summary:")
    print(summary_df.to_string())
    print("\nAll outputs:")
    out.summary()


if __name__ == "__main__":
    main()
