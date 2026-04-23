"""Global multi-factor CTA sleeve-blend backtest.

Combines China futures and overseas futures into one cross-market universe:
  - trend sleeve: MultiFactorTrendSignal + inverse-vol sizing
  - cross sleeve: MultiFactorCrossSectionalMomentumSignal factor portfolios
  - portfolio: 50% trend + 50% cross by default
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
from data.universe import SECTOR_MAP, SECTOR_MAP_OVERSEAS, build_symbol_sector_map
from strategies.implementations.multifactor_cta_backtest import MultiFactorCTAStrategy


CN_BROAD_SECTOR = {
    "股指期货": "Equity",
    "国债期货": "Rates",
    "有色金属": "Metals",
    "黑色金属": "Metals",
    "能源化工": "EnergyChem",
    "农产品": "Agriculture",
}

OV_BROAD_SECTOR = {
    "Equity Index": "Equity",
    "Gov. Bond": "Rates",
    "Energy": "EnergyChem",
    "Precious Metals": "Metals",
    "Base Metals": "Metals",
    "Agriculture": "Agriculture",
    "Currency": "FX",
    "Alt/Other": "Other",
}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Global multi-factor CTA sleeve-blend backtest")
    p.add_argument(
        "--china-data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "china_daily_full"),
    )
    p.add_argument(
        "--overseas-data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "overseas_daily_full"),
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "multifactor_cta_global"),
    )
    p.add_argument("--start", default=None, help="backtest start date, e.g. 2005-01-01")
    p.add_argument("--end", default=None, help="backtest end date, e.g. 2025-12-31")
    p.add_argument("--trend-weight", type=float, default=1.0)
    p.add_argument("--cross-weight", type=float, default=1.0)
    p.add_argument(
        "--cross-weighting",
        choices=["global_equal", "sector_inverse_vol"],
        default="global_equal",
    )
    p.add_argument("--cross-sector-vol-halflife", type=int, default=21)
    p.add_argument("--target-vol", type=float, default=0.05)
    p.add_argument("--cost-bps", type=float, default=5.0)
    p.add_argument("--max-abs-weight", type=float, default=0.10)
    p.add_argument("--max-gross-exposure", type=float, default=1.50)
    p.add_argument("--short-filter-mode", choices=["none", "momentum_vote", "donchian"], default="none")
    p.add_argument("--verbose", action="store_true", default=True)
    return p.parse_args()


def _prefixed_sector_map(prefix: str, sector_map: dict[str, str], broad_map: dict[str, str]) -> dict[str, str]:
    return {
        f"{prefix}{symbol}": broad_map.get(sector, sector)
        for symbol, sector in sector_map.items()
    }


def _load_prefixed_returns(
    data_dir: str | Path,
    schema: KlineSchema,
    prefix: str,
    start: str | None,
    end: str | None,
    min_obs: int,
    exclude: set[str],
) -> pd.DataFrame:
    loader = DataLoader(
        kline_source=ParquetSource(data_dir),
        kline_schema=schema,
    )
    tickers = loader.available_symbols(exclude=exclude)
    returns = loader.load_returns_matrix(tickers, start=start, end=end, min_obs=min_obs)
    return returns.rename(columns=lambda c: f"{prefix}{c}")


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
    out = BacktestOutput(args.out_dir, subdirs=["reports", "charts", "signals", "data"])

    china_symbol_sector = build_symbol_sector_map(SECTOR_MAP)
    overseas_symbol_sector = build_symbol_sector_map(SECTOR_MAP_OVERSEAS)
    global_sector_map = {}
    global_sector_map.update(_prefixed_sector_map("CN_", china_symbol_sector, CN_BROAD_SECTOR))
    global_sector_map.update(_prefixed_sector_map("OV_", overseas_symbol_sector, OV_BROAD_SECTOR))

    strategy = MultiFactorCTAStrategy(
        config={
            "sector_map": global_sector_map,
            "trend_weight": args.trend_weight,
            "cross_weight": args.cross_weight,
            "cross_weighting": args.cross_weighting,
            "cross_sector_vol_halflife": args.cross_sector_vol_halflife,
            "target_vol": args.target_vol,
            "transaction_cost_bps": args.cost_bps,
            "max_abs_weight": args.max_abs_weight,
            "max_gross_exposure": args.max_gross_exposure,
            "short_filter_mode": args.short_filter_mode,
        }
    )

    china_returns = _load_prefixed_returns(
        args.china_data_dir,
        KlineSchema.tushare(),
        "CN_",
        args.start,
        args.end,
        strategy.min_obs,
        strategy.exclude,
    )
    overseas_returns = _load_prefixed_returns(
        args.overseas_data_dir,
        KlineSchema.overseas(),
        "OV_",
        args.start,
        args.end,
        strategy.min_obs,
        strategy.exclude,
    )
    returns = pd.concat([china_returns, overseas_returns], axis=1).sort_index()
    returns = returns.loc[:, ~returns.columns.duplicated()].copy()

    if returns.empty:
        raise RuntimeError("No returns loaded for global multi-factor CTA.")

    if args.verbose:
        print("=" * 65)
        print("Step 1: Load global returns")
        print("=" * 65)
        print(
            f"\nGlobal returns matrix: {returns.shape} "
            f"({returns.index[0].date()} - {returns.index[-1].date()})"
        )
        print(
            f"  China symbols: {china_returns.shape[1]}  "
            f"Overseas symbols: {overseas_returns.shape[1]}"
        )

    trend_signal = strategy.generate_trend_signal(returns)
    cross_signal = strategy.generate_cross_signal(returns, sector_map=global_sector_map)
    blended_signal = strategy.blend_signals(trend_signal, cross_signal)
    short_filter = strategy.compute_short_filter(returns)
    filtered_signal = strategy.apply_short_filter(trend_signal, short_filter)
    sigma = strategy.compute_sigma(returns)
    raw_trend_positions = strategy.build_trend_positions(trend_signal, sigma)
    trend_positions = strategy.build_trend_positions(filtered_signal, sigma)
    cross_positions = strategy.build_cross_positions(returns, sector_map=global_sector_map)
    raw_positions = strategy.blend_positions(raw_trend_positions, cross_positions)
    positions = strategy.blend_positions(trend_positions, cross_positions)

    bt = VectorizedBacktest(
        lag=1,
        vol_target=strategy.target_vol,
        vol_halflife=strategy.vol_halflife,
        trading_days=strategy.trading_days,
        cost_model=ProportionalCostModel(strategy.transaction_cost_rate),
    )
    bt_result = bt.run(positions, returns)
    pnl = bt_result.returns.iloc[1:]

    out.save_parquet(returns, "data", "returns.parquet")
    out.save_json({"symbols": returns.columns.tolist(), "sector_map": global_sector_map}, "data", "asset_list.json")
    out.save_parquet(trend_signal, "signals", "trend_signal.parquet")
    out.save_parquet(cross_signal, "signals", "cross_signal.parquet")
    out.save_parquet(blended_signal, "signals", "blended_signal.parquet")
    out.save_parquet(short_filter, "signals", "short_filter.parquet")
    out.save_parquet(filtered_signal, "signals", "filtered_signal.parquet")
    out.save_parquet(raw_positions, "signals", "raw_positions.parquet")
    out.save_parquet(trend_positions, "signals", "trend_positions.parquet")
    out.save_parquet(cross_positions, "signals", "cross_positions.parquet")
    out.save_parquet(positions, "signals", "positions.parquet")

    annual_df = annual_stats(pnl)
    monthly_df = monthly_pivot(pnl)
    sector_df = sector_stats(positions, returns, global_sector_map, bt)
    turnover_df = _turnover_cost_frame(bt_result.turnover_series, strategy.transaction_cost_rate)
    summary = pnl_stats(pnl)
    summary.update(_turnover_cost_summary(turnover_df, strategy.trading_days))
    summary_df = pd.DataFrame([summary]).rename(index={0: "MultiFactorCTAGlobal"})

    out.save_csv(annual_df, "reports", "annual.csv")
    out.save_csv(monthly_df, "reports", "monthly.csv")
    out.save_csv(sector_df, "reports", "sector_contribution.csv")
    out.save_csv(summary_df, "reports", "full_sample_summary.csv")
    out.save_csv(turnover_df, "reports", "turnover_cost.csv")

    out.save_fig(
        plot_nav_with_drawdown(
            {"MultiFactorCTAGlobal": pnl},
            title="Global Futures MultiFactor CTA",
        ),
        "charts",
        "nav_multifactor_cta_global.png",
        dpi=150,
        bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar(
            {"MultiFactorCTAGlobal": annual_df},
            title="Global Futures MultiFactor CTA — Annual Returns",
        ),
        "charts",
        "annual_returns_bar.png",
        dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(
            monthly_df,
            title="Global Futures MultiFactor CTA — Monthly Returns (%)",
        ),
        "charts",
        "monthly_heatmap.png",
        dpi=150,
        bbox_inches="tight",
    )
    out.save_fig(
        plot_sector_nav(
            positions,
            returns,
            global_sector_map,
            bt,
            title="Global Futures MultiFactor CTA — Sector NAV",
        ),
        "charts",
        "sector_nav.png",
        dpi=150,
    )

    if args.verbose:
        gross = positions.abs().sum(axis=1)
        net = positions.sum(axis=1)
        print("\n" + "=" * 65)
        print("Step 2: Summary")
        print("=" * 65)
        print(
            f"  Sleeve weights: trend={args.trend_weight:g}, cross={args.cross_weight:g}  "
            f"cross_weighting={args.cross_weighting}"
        )
        print(
            f"  Position gross mean/max={gross.mean():.2f}/{gross.max():.2f}  "
            f"net abs mean={net.abs().mean():.2f}"
        )
        print("\nFull sample summary:")
        print(summary_df.to_string())
        print("\nAll outputs:")
        out.summary()


if __name__ == "__main__":
    main()
