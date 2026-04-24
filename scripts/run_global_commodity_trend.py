"""全球纯商品趋势策略回测（剔除股指/债券/外汇品种）。

只保留 Agriculture / EnergyChem / Metals 三个商品大类：
  - 中国：螺纹、铁矿、铜、豆粕、豆油、原油、沥青等（约75个）
  - 海外：CBOT谷物、LME金属、NYMEX能源、ICE软商品等

运行：
  cd /home/ubuntu/dengl/my_projects/cta_lab
  python3 scripts/run_global_commodity_trend.py
"""

from __future__ import annotations

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
from backtest.costs import ProportionalCostModel
from backtest.vectorized import VectorizedBacktest
from data.loader import DataLoader, KlineSchema
from data.sources.parquet_source import ParquetSource
from data.universe import SECTOR_MAP, SECTOR_MAP_OVERSEAS, build_symbol_sector_map
from strategies.implementations.multifactor_cta_backtest import MultiFactorCTAStrategy

# ── 路径 ──────────────────────────────────────────────────────────────────────

_ROOT = _CTA_LAB.parent
CHINA_DATA_DIR = _ROOT / "market_data" / "kline" / "china_daily_full"
OVERSEAS_DATA_DIR = _ROOT / "market_data" / "kline" / "overseas_daily_full"
OUT_DIR = _ROOT / "research_outputs" / "multifactor_cta_global" / "runs" / "02_trend_commodity_only"

# ── 宽泛行业映射（与 run_multifactor_cta_global.py 保持一致） ─────────────────

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

COMMODITY_SECTORS = {"Agriculture", "EnergyChem", "Metals"}

# ── 参数 ──────────────────────────────────────────────────────────────────────

START = "2005-01-01"
TARGET_VOL = 0.05
COST_BPS = 5.0


def _prefixed_sector_map(prefix, sector_map, broad_map):
    return {
        f"{prefix}{sym}": broad_map.get(sec, sec)
        for sym, sec in sector_map.items()
    }


def _load_prefixed_returns(data_dir, schema, prefix, start, min_obs, exclude):
    loader = DataLoader(kline_source=ParquetSource(data_dir), kline_schema=schema)
    tickers = loader.available_symbols(exclude=exclude)
    returns = loader.load_returns_matrix(tickers, start=start, min_obs=min_obs)
    return returns.rename(columns=lambda c: f"{prefix}{c}")


def _turnover_cost_frame(turnover, cost_rate):
    if turnover is None:
        return pd.DataFrame(columns=["turnover", "transaction_cost"])
    frame = turnover.to_frame("turnover")
    frame["transaction_cost"] = frame["turnover"] * cost_rate
    return frame


def _turnover_cost_summary(frame, trading_days):
    if frame.empty:
        return {"AvgTurnover(%)": 0, "AnnTurnover(x)": 0, "TotalCost(%)": 0, "AnnCost(%)": 0}
    return {
        "AvgTurnover(%)": round(float(frame["turnover"].mean() * 100), 2),
        "AnnTurnover(x)": round(float(frame["turnover"].mean() * trading_days), 2),
        "TotalCost(%)": round(float(frame["transaction_cost"].sum() * 100), 2),
        "AnnCost(%)": round(float(frame["transaction_cost"].mean() * trading_days * 100), 2),
    }


def main():
    out = BacktestOutput(str(OUT_DIR), subdirs=["reports", "charts", "signals", "data"])

    # ── 构建全局 sector_map ────────────────────────────────────────────────────
    china_sym_sec = build_symbol_sector_map(SECTOR_MAP)
    overseas_sym_sec = build_symbol_sector_map(SECTOR_MAP_OVERSEAS)
    global_sector_map = {}
    global_sector_map.update(_prefixed_sector_map("CN_", china_sym_sec, CN_BROAD_SECTOR))
    global_sector_map.update(_prefixed_sector_map("OV_", overseas_sym_sec, OV_BROAD_SECTOR))

    strategy = MultiFactorCTAStrategy(
        config={
            "sector_map": global_sector_map,
            "trend_weight": 1.0,
            "cross_weight": 0.0,
            "target_vol": TARGET_VOL,
            "transaction_cost_bps": COST_BPS,
            "max_abs_weight": 0.10,
            "max_gross_exposure": 1.50,
            "short_filter_mode": "momentum_vote",
        }
    )

    # ── 加载数据 ──────────────────────────────────────────────────────────────
    print("=" * 65)
    print("Step 1: 加载全球收益率矩阵")
    print("=" * 65)

    china_returns = _load_prefixed_returns(
        CHINA_DATA_DIR, KlineSchema.tushare(), "CN_", START,
        strategy.min_obs, strategy.exclude,
    )
    overseas_returns = _load_prefixed_returns(
        OVERSEAS_DATA_DIR, KlineSchema.overseas(), "OV_", START,
        strategy.min_obs, strategy.exclude,
    )
    returns_all = pd.concat([china_returns, overseas_returns], axis=1).sort_index()
    returns_all = returns_all.loc[:, ~returns_all.columns.duplicated()].copy()

    # ── 按商品板块过滤 ────────────────────────────────────────────────────────
    commodity_cols = [
        col for col in returns_all.columns
        if global_sector_map.get(col, "Other") in COMMODITY_SECTORS
    ]
    returns = returns_all[commodity_cols].copy()

    # 同步 sector_map 到保留品种
    filtered_sector_map = {k: v for k, v in global_sector_map.items() if k in commodity_cols}

    cn_cnt = sum(1 for c in commodity_cols if c.startswith("CN_"))
    ov_cnt = sum(1 for c in commodity_cols if c.startswith("OV_"))
    total_dropped = returns_all.shape[1] - len(commodity_cols)
    print(f"\n全样本品种: {returns_all.shape[1]}  →  商品类品种: {len(commodity_cols)}")
    print(f"  中国商品: {cn_cnt}  海外商品: {ov_cnt}  剔除非商品: {total_dropped}")
    print(f"  日期范围: {returns.index[0].date()} ~ {returns.index[-1].date()}")

    # ── 信号和仓位 ────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Step 2: 趋势信号 + 短期过滤 + 仓位构建")
    print("=" * 65)

    trend_signal = strategy.generate_trend_signal(returns)
    short_filter = strategy.compute_short_filter(returns)
    filtered_signal = strategy.apply_short_filter(trend_signal, short_filter)
    sigma = strategy.compute_sigma(returns)
    positions = strategy.build_trend_positions(filtered_signal, sigma)

    gross = positions.abs().sum(axis=1)
    net = positions.sum(axis=1)
    conflict_rate = (filtered_signal.eq(0.0) & trend_signal.ne(0.0)).mean().mean()
    print(f"  短期过滤剔除比例: {conflict_rate:.1%}")
    print(f"  Position gross mean/max = {gross.mean():.2f}/{gross.max():.2f}  net abs mean = {net.abs().mean():.2f}")

    # ── 回测 ──────────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Step 3: VectorizedBacktest")
    print("=" * 65)

    bt = VectorizedBacktest(
        lag=1,
        vol_target=TARGET_VOL,
        vol_halflife=strategy.vol_halflife,
        trading_days=strategy.trading_days,
        cost_model=ProportionalCostModel(COST_BPS / 10_000.0),
    )
    bt_result = bt.run(positions, returns)
    pnl = bt_result.returns.iloc[1:]

    ann_r = pnl.mean() * strategy.trading_days
    ann_v = pnl.std() * (strategy.trading_days ** 0.5)
    import numpy as np
    nav = (1 + pnl).cumprod()
    mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    print(f"  Sharpe={ann_r/ann_v:.3f}  Return={ann_r*100:.1f}%  Vol={ann_v*100:.1f}%  MaxDD={mdd*100:.1f}%")

    # ── 保存 ──────────────────────────────────────────────────────────────────
    out.save_parquet(returns, "data", "returns.parquet")
    import json
    (Path(OUT_DIR) / "data" / "asset_list.json").write_text(
        json.dumps({"symbols": returns.columns.tolist(), "sector_map": filtered_sector_map}, ensure_ascii=False, indent=2)
    )
    out.save_parquet(trend_signal, "signals", "trend_signal.parquet")
    out.save_parquet(positions, "signals", "positions.parquet")

    annual_df = annual_stats(pnl)
    monthly_df = monthly_pivot(pnl)
    sector_df = sector_stats(positions, returns, filtered_sector_map, bt)
    turnover_df = _turnover_cost_frame(bt_result.turnover_series, COST_BPS / 10_000.0)
    summary = pnl_stats(pnl)
    summary.update(_turnover_cost_summary(turnover_df, strategy.trading_days))
    summary_df = pd.DataFrame([summary]).rename(index={0: "GlobalCommodityTrend"})

    out.save_csv(annual_df, "reports", "annual.csv")
    out.save_csv(monthly_df, "reports", "monthly.csv")
    out.save_csv(sector_df, "reports", "sector_contribution.csv")
    out.save_csv(summary_df, "reports", "full_sample_summary.csv")
    out.save_csv(turnover_df, "reports", "turnover_cost.csv")

    out.save_fig(
        plot_nav_with_drawdown({"GlobalCommodityTrend": pnl}, title="Global Commodity Trend (Excl. FX/Rates/Equity)"),
        "charts", "nav.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar({"GlobalCommodityTrend": annual_df}, title="Global Commodity Trend — Annual Returns"),
        "charts", "annual_returns_bar.png", dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(monthly_df, title="Global Commodity Trend — Monthly Returns (%)"),
        "charts", "monthly_heatmap.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_sector_nav(positions, returns, filtered_sector_map, bt, title="Global Commodity Trend — Sector NAV"),
        "charts", "sector_nav.png", dpi=150,
    )

    print("\nFull sample summary:")
    print(summary_df.to_string())
    print("\nAnnual returns:")
    print(annual_df.to_string())
    print("\nSector contribution:")
    print(sector_df.to_string())
    print("\nAll outputs:", OUT_DIR)


if __name__ == "__main__":
    main()
