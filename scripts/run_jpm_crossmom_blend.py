"""JPM Trend + MultiFactor CrossMOM 融合策略回测。

对比组合：
  A: MF Trend + MF CrossMOM（= 现有 MF CTA，SR≈1.47 @ 5%vol）
  B1: JPM Trend(10%/1.5x) + MF CrossMOM，比例 1:1
  B2: JPM Trend(10%/1.5x) + MF CrossMOM，比例 1:2（CS 重）
  B3: JPM Trend(10%/1.5x) + MF CrossMOM，比例 2:1（TS 重）

统一设置：target_vol=10%，cost=5bps，lag=1，品种宇宙与 JPM 相同。

用法（cta_lab/ 目录下执行）：
    python scripts/run_jpm_crossmom_blend.py \\
        --data-dir /path/to/market_data/kline/china_daily_full \\
        --out-dir  /path/to/research_outputs/jpm_crossmom_blend
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

_HERE = Path(__file__).resolve().parent
_CTA_LAB = _HERE.parent
if str(_CTA_LAB) not in sys.path:
    sys.path.insert(0, str(_CTA_LAB))

from analysis.metrics import annual_stats, pnl_stats, monthly_pivot
from analysis.report.charts import plot_nav_with_drawdown, plot_annual_bar
from analysis.report.output import BacktestOutput
from backtest import ProportionalCostModel
from backtest.vectorized import VectorizedBacktest
from data.loader import DataLoader, KlineSchema
from data.sources.parquet_source import ParquetSource
from strategies.context import StrategyContext
from strategies.implementations.jpm_trend_trade.strategy import JPMTrendStrategy
from strategies.implementations.multifactor_cta_backtest.strategy import MultiFactorCTAStrategy


COST_BPS = 5.0
TARGET_VOL = 0.10
MAX_ABS_WEIGHT = 0.10
MAX_GROSS = 1.50
TRADING_DAYS = 252
VOL_HALFLIFE = 21


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "china_daily_full"),
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "jpm_crossmom_blend"),
    )
    return p.parse_args()


def _build_jpm_sleeve(
    jpm: JPMTrendStrategy,
    returns: pd.DataFrame,
    max_abs_weight: float = MAX_ABS_WEIGHT,
    max_gross: float = MAX_GROSS,
) -> pd.DataFrame:
    """JPM signal/sigma → clip → gross cap，sleeve 层约束，不含 vol-targeting。"""
    signal = jpm.generate_signals_from_returns(returns)
    sigma = jpm._compute_sigma(returns)
    vol_safe = sigma.replace(0, np.nan)
    raw = (signal / vol_safe).fillna(0.0)
    # 单品种上限
    raw = raw.clip(lower=-max_abs_weight, upper=max_abs_weight)
    # gross 上限
    gross = raw.abs().sum(axis=1)
    scale = (max_gross / gross).clip(upper=1.0).replace([np.inf, -np.inf], 0.0)
    return raw.mul(scale, axis=0).fillna(0.0)


def _blend_positions(
    ts_pos: pd.DataFrame,
    cs_pos: pd.DataFrame,
    ts_weight: float,
    cs_weight: float,
) -> pd.DataFrame:
    """按权重混合两个 sleeve 的持仓矩阵。"""
    ts_a, cs_a = ts_pos.align(cs_pos, join="outer", axis=None, fill_value=0.0)
    total = ts_weight + cs_weight
    return ((ts_weight * ts_a + cs_weight * cs_a) / total).fillna(0.0)


def _run_bt(positions: pd.DataFrame, returns: pd.DataFrame) -> pd.Series:
    """统一回测器：vol-target=10%, cost=5bps, lag=1。"""
    bt = VectorizedBacktest(
        lag=1,
        vol_target=TARGET_VOL,
        vol_halflife=VOL_HALFLIFE,
        trading_days=TRADING_DAYS,
        cost_model=ProportionalCostModel(COST_BPS / 10_000.0),
    )
    result = bt.run(positions, returns)
    return result.returns.iloc[1:]  # 去掉 NAV 起点占位行


def _turnover_ann(positions: pd.DataFrame, returns: pd.DataFrame) -> float:
    """粗算年化换手（不含 vol-targeting 缩放，用于方向性参考）。"""
    bt = VectorizedBacktest(
        lag=1,
        vol_target=TARGET_VOL,
        vol_halflife=VOL_HALFLIFE,
        trading_days=TRADING_DAYS,
        cost_model=ProportionalCostModel(COST_BPS / 10_000.0),
    )
    result = bt.run(positions, returns)
    to = result.turnover_series
    return float(to.mean() * TRADING_DAYS) if to is not None and not to.empty else float("nan")


def _print_section(title: str) -> None:
    print("\n" + "=" * 65)
    print(title)
    print("=" * 65)


def main() -> None:
    args = _parse_args()
    out = BacktestOutput(args.out_dir, subdirs=["reports", "charts"])

    # ── 1. 加载数据 ────────────────────────────────────────────────────────────
    _print_section("Step 1: Load returns")
    jpm_strategy = JPMTrendStrategy()
    loader = DataLoader(
        kline_source=ParquetSource(args.data_dir),
        kline_schema=KlineSchema.tushare(),
    )
    symbols = loader.available_symbols(exclude=jpm_strategy.exclude)
    returns = loader.load_returns_matrix(symbols, min_obs=jpm_strategy.min_obs)
    print(f"  Returns: {returns.shape}  {returns.index[0].date()} - {returns.index[-1].date()}")

    # ── 2. 构建 JPM TS Sleeve（加约束） ───────────────────────────────────────
    _print_section("Step 2: JPM TS sleeve (max_abs=10%, max_gross=1.5x)")
    jpm_pos = _build_jpm_sleeve(jpm_strategy, returns)
    gross_jpm = jpm_pos.abs().sum(axis=1)
    print(f"  JPM sleeve gross: mean={gross_jpm.mean():.3f}  max={gross_jpm.max():.3f}")

    # ── 3. 构建 MF CrossMOM Sleeve ────────────────────────────────────────────
    _print_section("Step 3: MF CrossMOM sleeve (MultiFactorCrossSectional)")
    mf = MultiFactorCTAStrategy()
    sector_map = {str(s): mf.sector_map.get(str(s), "Other") for s in returns.columns}
    cs_pos = mf.build_cross_positions(returns, sector_map=sector_map)
    gross_cs = cs_pos.abs().sum(axis=1)
    print(f"  CS sleeve gross:  mean={gross_cs.mean():.3f}  max={gross_cs.max():.3f}")

    # ── 4. 构建 MF Trend Sleeve（对照组） ────────────────────────────────────
    _print_section("Step 4: MF Trend sleeve (MultiFactorTrend, 对照组)")
    mf_trend_signal = mf.generate_trend_signal(returns)
    sigma_mf = mf.compute_sigma(returns)
    mf_ts_pos = mf.build_trend_positions(mf_trend_signal, sigma_mf)
    gross_mf = mf_ts_pos.abs().sum(axis=1)
    print(f"  MF TS sleeve gross: mean={gross_mf.mean():.3f}  max={gross_mf.max():.3f}")

    # ── 5. 回测各组合 ─────────────────────────────────────────────────────────
    _print_section("Step 5: Backtest all combinations")

    # 对照 A：MF Trend + MF CrossMOM（1:2，即 MF CTA 默认混合比例接近）
    blend_A_1_2 = _blend_positions(mf_ts_pos, cs_pos, ts_weight=1.0, cs_weight=2.0)
    pnl_A = _run_bt(blend_A_1_2, returns)

    # 对照 A'：MF Trend + MF CrossMOM（1:1）
    blend_A_1_1 = _blend_positions(mf_ts_pos, cs_pos, ts_weight=1.0, cs_weight=1.0)
    pnl_A_11 = _run_bt(blend_A_1_1, returns)

    # 对照 A''：MF Trend + MF CrossMOM（2:1）
    blend_A_2_1 = _blend_positions(mf_ts_pos, cs_pos, ts_weight=2.0, cs_weight=1.0)
    pnl_A_21 = _run_bt(blend_A_2_1, returns)

    # 方案 B1：JPM + CS（1:1）
    blend_B1 = _blend_positions(jpm_pos, cs_pos, ts_weight=1.0, cs_weight=1.0)
    pnl_B1 = _run_bt(blend_B1, returns)

    # 方案 B2：JPM + CS（1:2，CS 重）
    blend_B2 = _blend_positions(jpm_pos, cs_pos, ts_weight=1.0, cs_weight=2.0)
    pnl_B2 = _run_bt(blend_B2, returns)

    # 方案 B3：JPM + CS（2:1，TS 重）
    blend_B3 = _blend_positions(jpm_pos, cs_pos, ts_weight=2.0, cs_weight=1.0)
    pnl_B3 = _run_bt(blend_B3, returns)

    # ── 6. 汇总统计 ───────────────────────────────────────────────────────────
    _print_section("Step 6: Full-Sample Summary")

    labels = {
        "MFTrend+CS(1:2)":  pnl_A,
        "MFTrend+CS(1:1)":  pnl_A_11,
        "MFTrend+CS(2:1)":  pnl_A_21,
        "JPM+CS(1:1)":      pnl_B1,
        "JPM+CS(1:2)":      pnl_B2,
        "JPM+CS(2:1)":      pnl_B3,
    }

    rows = []
    for label, pnl in labels.items():
        s = pnl_stats(pnl)
        s["Label"] = label
        rows.append(s)
    summary = pd.DataFrame(rows).set_index("Label")
    print(summary[["Return(%)", "Vol(%)", "Sharpe", "MaxDD(%)", "Calmar", "HitRate(%)"]].to_string())

    # ── 7. 逐年 Sharpe 对比 ───────────────────────────────────────────────────
    _print_section("Step 7: Annual Sharpe Comparison")

    annual_rows = {}
    for label, pnl in labels.items():
        ann = annual_stats(pnl)
        if "Year" in ann.columns:
            ann = ann.set_index("Year")
        annual_rows[label] = ann["Sharpe"]

    annual_sharpe = pd.DataFrame(annual_rows)
    print(annual_sharpe.to_string())

    # ── 8. 保存输出 ───────────────────────────────────────────────────────────
    _print_section("Step 8: Save outputs")

    out.save_csv(summary, "reports", "full_sample_summary.csv")
    out.save_csv(annual_sharpe, "reports", "annual_sharpe.csv")

    # 逐年 return 对比
    annual_return_rows = {}
    for label, pnl in labels.items():
        ann = annual_stats(pnl)
        if "Year" in ann.columns:
            ann = ann.set_index("Year")
        annual_return_rows[label] = ann["Return(%)"]
    out.save_csv(pd.DataFrame(annual_return_rows), "reports", "annual_return.csv")

    # NAV 图
    nav_series = {}
    for label, pnl in labels.items():
        nav_series[label] = pnl
    out.save_fig(
        plot_nav_with_drawdown(
            nav_series,
            title="JPM+CS vs MFTrend+CS Blend — NAV (10% vol target, 5bps cost)",
        ),
        "charts", "nav_comparison.png", dpi=150, bbox_inches="tight",
    )

    # Annual returns bar（只画 B2 vs A 1:2 作为主对比）
    ann_A = annual_stats(pnl_A)
    ann_B2 = annual_stats(pnl_B2)
    out.save_fig(
        plot_annual_bar(
            {"MFTrend+CS(1:2)": ann_A, "JPM+CS(1:2)": ann_B2},
            title="MFTrend+CS vs JPM+CS (TS:CS=1:2) — Annual Returns",
        ),
        "charts", "annual_bar_main.png", dpi=150,
    )

    out.summary()
    print("\nDone.")


if __name__ == "__main__":
    main()
