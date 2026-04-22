"""双动量策略对比回测入口。

四种信号模式全样本对比：
  absolute  : sign(cum_log_ret_12m)          纯绝对动量 = TSMOM Binary
  relative  : 板块内分位排名                  纯相对动量（板块内截面）
  dual_ls   : 相对强 AND 绝对正→多；相对弱 AND 绝对负→空；否则平仓
  dual_lo   : 相对强 AND 绝对正→多；否则平仓（论文原始精神，仅做多）

用法（在 cta_lab/ 目录下执行）：
    python scripts/run_dual_momentum.py \\
        --data-dir /path/to/market_data/kline/china_daily_full \\
        --out-dir  /path/to/output

默认输出：<cta_lab 上级>/research_outputs/dual_momentum_china/
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
_CTA_LAB = _HERE.parent   # scripts/ -> cta_lab/
if str(_CTA_LAB) not in sys.path:
    sys.path.insert(0, str(_CTA_LAB))

from analysis.metrics import pnl_stats, annual_stats, monthly_pivot, sector_stats
from analysis.report.charts import (
    plot_nav_with_drawdown,
    plot_annual_bar,
    plot_rolling_sharpe,
    plot_monthly_heatmap,
)
from analysis.report.output import BacktestOutput
from backtest import ProportionalCostModel, turnover_cost_frame, turnover_cost_summary
from backtest.vectorized import VectorizedBacktest
from data.loader import DataLoader, KlineSchema
from data.sources.parquet_source import ParquetSource
from strategies.context import StrategyContext
from strategies.implementations.dual_momentum_backtest.strategy import DualMomentumStrategy
from strategies.implementations.dual_momentum_backtest.config import (
    SECTOR_MAP, TRADING_DAYS, EXCLUDE, MIN_OBS,
    TARGET_VOL, VOL_HALFLIFE, SIGMA_HALFLIFE, LOOKBACK, TOP_PCT, BOTTOM_PCT,
)

# ── 常量 ──────────────────────────────────────────────────────────────────────
MODES = ["absolute", "relative", "dual_ls", "dual_lo"]
LABELS = {
    "absolute": "Absolute (TSMOM)",
    "relative": "Relative (CrossMOM)",
    "dual_ls":  "Dual L/S",
    "dual_lo":  "Dual L-only",
}
COLORS = {
    "absolute": "steelblue",
    "relative": "seagreen",
    "dual_ls":  "darkorange",
    "dual_lo":  "crimson",
}


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="双动量策略对比回测")
    p.add_argument(
        "--data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "china_daily_full"),
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "dual_momentum_china"),
    )
    p.add_argument("--lookback",    type=int,   default=LOOKBACK)
    p.add_argument("--top-pct",     type=float, default=TOP_PCT)
    p.add_argument("--bottom-pct",  type=float, default=BOTTOM_PCT)
    p.add_argument("--target-vol",  type=float, default=TARGET_VOL)
    p.add_argument("--cost-bps", type=float, default=0.0, help="单边换手成本，单位 bps，默认0")
    p.add_argument("--verbose",     action="store_true", default=True)
    return p.parse_args()


# ── 信号覆盖率统计 ────────────────────────────────────────────────────────────

def _signal_coverage(sig_dict: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for mode, sig in sig_dict.items():
        valid = sig.stack().dropna()
        long_frac  = (valid > 0).mean()
        short_frac = (valid < 0).mean()
        flat_frac  = (valid == 0).mean()
        rows.append({
            "Mode":    LABELS[mode],
            "Long%":  round(long_frac * 100, 1),
            "Short%": round(short_frac * 100, 1),
            "Flat%":  round(flat_frac * 100, 1),
        })
    return pd.DataFrame(rows).set_index("Mode")


# ── 板块 Sharpe 热力图 ────────────────────────────────────────────────────────

def _plot_sector_heatmap(sector_dfs: dict[str, pd.DataFrame]) -> "Figure":
    import matplotlib.pyplot as plt
    modes = list(sector_dfs.keys())
    sectors = sorted(sector_dfs[modes[0]].index)
    data = np.full((len(sectors), len(modes)), np.nan)
    for j, mode in enumerate(modes):
        for i, sec in enumerate(sectors):
            if sec in sector_dfs[mode].index:
                data[i, j] = sector_dfs[mode].loc[sec, "Sharpe"]

    fig, ax = plt.subplots(figsize=(9, max(5, len(sectors) * 0.5)))
    vmax = np.nanpercentile(np.abs(data), 90)
    im = ax.imshow(data, cmap="RdYlGn", aspect="auto", vmin=-vmax, vmax=vmax)
    ax.set_xticks(range(len(modes)))
    ax.set_xticklabels([LABELS[m] for m in modes], fontsize=8, rotation=15)
    ax.set_yticks(range(len(sectors)))
    ax.set_yticklabels(sectors, fontsize=9)
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            v = data[i, j]
            if not np.isnan(v):
                ax.text(j, i, f"{v:.2f}", ha="center", va="center",
                        fontsize=8, color="black" if abs(v) < vmax*0.6 else "white")
    plt.colorbar(im, ax=ax, label="Sharpe Ratio", shrink=0.7)
    ax.set_title("Sector Sharpe by Strategy Mode", fontsize=12)
    plt.tight_layout()
    return fig


# ── 主流程 ────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    out = BacktestOutput(
        args.out_dir,
        subdirs=["reports", "charts", "data"],
    )

    if args.verbose:
        print("=" * 65)
        print("Step 1: Load china_daily_full returns")
        print("=" * 65)

    loader = DataLoader(
        kline_source=ParquetSource(args.data_dir),
        kline_schema=KlineSchema.tushare(),
    )
    backtest = VectorizedBacktest(
        lag=1,
        vol_target=args.target_vol,
        vol_halflife=VOL_HALFLIFE,
        trading_days=TRADING_DAYS,
        cost_model=ProportionalCostModel(args.cost_bps / 10_000.0),
    )
    context = StrategyContext(
        loader=loader,
        sector_map=dict(SECTOR_MAP),
        backtest=backtest,
    )
    strategy = DualMomentumStrategy(
        config={
            "lookback": args.lookback,
            "top_pct": args.top_pct,
            "bottom_pct": args.bottom_pct,
            "target_vol": args.target_vol,
        }
    )
    returns = context.load_returns_matrix(
        min_obs=MIN_OBS,
        exclude=set(EXCLUDE),
    )

    if args.verbose:
        print(
            f"\nReturns matrix: {returns.shape}  "
            f"({returns.index[0].date()} - {returns.index[-1].date()})"
        )

    out.save_parquet(returns, "data", "returns.parquet")

    sigma = strategy._compute_sigma(returns).replace(0, np.nan)
    bt = context.backtest
    assert bt is not None
    sym_sector = context.resolve_sector_map(returns.columns)

    if args.verbose:
        print("\n" + "=" * 65)
        print("Step 2: Compute signals & backtest (4 modes)")
        print("=" * 65)

    pnl_dict: dict[str, pd.Series]    = {}
    sig_dict: dict[str, pd.DataFrame] = {}
    pos_dict: dict[str, pd.DataFrame] = {}
    turnover_dict: dict[str, pd.DataFrame] = {}

    for mode in MODES:
        signal = strategy.generate_signals(
            returns,
            mode,
            context=context,
            sector_map=sym_sector,
        )
        weight = strategy.build_weights(signal, sigma)

        sig_dict[mode] = signal
        pos_dict[mode] = weight

        result = bt.run(weight, returns)
        pnl = result.returns.iloc[1:]
        pnl_dict[mode] = pnl
        turnover_dict[mode] = turnover_cost_frame(
            weight,
            args.cost_bps / 10_000.0,
            lag=bt.lag,
        )

        s = pnl_stats(pnl)
        if args.verbose:
            print(
                f"  [{LABELS[mode]:22s}]  "
                f"SR={s['Sharpe']:.3f}  "
                f"Ret={s['Return(%)']:.1f}%  "
                f"Vol={s['Vol(%)']:.1f}%  "
                f"MDD={s['MaxDD(%)']:.1f}%  "
                f"({pnl.index[0].date()} - {pnl.index[-1].date()})"
            )

    cov_df = _signal_coverage(sig_dict)
    print("\n" + "=" * 65)
    print("Signal Coverage (Long% / Short% / Flat%)")
    print("=" * 65)
    print(cov_df.to_string())
    out.save_csv(cov_df, "reports", "signal_coverage.csv")

    print("\n" + "=" * 65)
    print("Full-Sample Summary")
    print("=" * 65)
    summary_rows = []
    for mode in MODES:
        row = pnl_stats(pnl_dict[mode], include_skew=True)
        row.update(
            turnover_cost_summary(
                pos_dict[mode],
                args.cost_bps / 10_000.0,
                lag=bt.lag,
                trading_days=TRADING_DAYS,
            )
        )
        row["Mode"] = LABELS[mode]
        summary_rows.append(row)
    summary_df = pd.DataFrame(summary_rows).set_index("Mode")
    print(summary_df.to_string())
    out.save_csv(summary_df, "reports", "full_sample_summary.csv")
    for mode in MODES:
        out.save_csv(turnover_dict[mode], "reports", f"turnover_cost_{mode}.csv")

    print("\n" + "=" * 65)
    print("Annual Sharpe Comparison")
    print("=" * 65)
    ann_dict: dict[str, pd.DataFrame] = {}
    for mode in MODES:
        ann_dict[mode] = annual_stats(pnl_dict[mode])

    ann_sharpe = pd.concat(
        {LABELS[m]: ann_dict[m]["Sharpe"] for m in MODES}, axis=1
    )
    print(ann_sharpe.to_string())
    out.save_csv(ann_sharpe, "reports", "annual_sharpe_comparison.csv")
    out.save_csv(
        pd.concat({LABELS[m]: ann_dict[m]["Return(%)"] for m in MODES}, axis=1),
        "reports", "annual_return_comparison.csv",
    )

    mpiv = monthly_pivot(pnl_dict["dual_ls"])
    out.save_csv(mpiv, "reports", "monthly_dual_ls.csv")

    print("\n" + "=" * 65)
    print("Sector Contribution — Dual L/S")
    print("=" * 65)
    sector_dfs: dict[str, pd.DataFrame] = {}
    for mode in MODES:
        sector_dfs[mode] = sector_stats(
            pos_dict[mode], returns, sym_sector, bt, include_avg_pos=True
        )
    print(sector_dfs["dual_ls"].to_string())
    for mode in MODES:
        out.save_csv(sector_dfs[mode], "reports", f"sector_{mode}.csv")

    # ── Charts ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Charts")
    print("=" * 65)

    _pnl_named    = {LABELS[m]: pnl_dict[m] for m in MODES}
    _colors_named = {LABELS[m]: COLORS[m]   for m in MODES}
    _ann_named    = {LABELS[m]: ann_dict[m] for m in MODES}

    out.save_fig(
        plot_nav_with_drawdown(
            _pnl_named,
            title=f"China Domestic Futures — Dual Momentum Comparison (lookback={args.lookback}d)\n"
                  f"Top/bottom {args.top_pct*100:.0f}% per sector · EWMA vol-targeting 10% ann.",
            colors=_colors_named,
        ),
        "charts", "nav_comparison.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar(
            _ann_named,
            value_col="Sharpe",
            title="Annual Sharpe: Absolute vs Relative vs Dual L/S vs Dual L-only",
            colors=_colors_named,
        ),
        "charts", "annual_sharpe.png", dpi=150,
    )
    out.save_fig(
        plot_rolling_sharpe(
            _pnl_named,
            title="Dual Momentum — Rolling 1-Year Sharpe Comparison",
            colors=_colors_named,
        ),
        "charts", "rolling_sharpe.png", dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(
            mpiv,
            title="China Futures Dual L/S — Monthly Returns (%)",
        ),
        "charts", "monthly_heatmap_dual_ls.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        _plot_sector_heatmap(sector_dfs),
        "charts", "sector_heatmap.png", dpi=150,
    )

    print(f"\nAll outputs:")
    out.summary()
    print("\n── Final Comparison ──")
    print(summary_df[["Return(%)", "Sharpe", "MaxDD(%)", "Calmar", "Skewness"]].to_string())

    abs_sr = summary_df.loc[LABELS["absolute"], "Sharpe"]
    dls_sr = summary_df.loc[LABELS["dual_ls"],  "Sharpe"]
    dlo_sr = summary_df.loc[LABELS["dual_lo"],  "Sharpe"]
    rel_sr = summary_df.loc[LABELS["relative"], "Sharpe"]
    print(f"\nDual L/S   vs Absolute: {(dls_sr/abs_sr -1)*100:+.1f}%")
    print(f"Dual L-only vs Absolute: {(dlo_sr/abs_sr -1)*100:+.1f}%")
    print(f"Relative   vs Absolute: {(rel_sr/abs_sr -1)*100:+.1f}%")
    print(
        f"\nUniverse: {returns.shape[1]} instruments  "
        f"Period: {returns.index[0].date()} - {returns.index[-1].date()}"
    )


if __name__ == "__main__":
    main()
