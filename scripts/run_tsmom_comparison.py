"""三模式 TSMOM 对比回测：Binary / Linear / Nonlinear(FS)。

参考：
  - Moskowitz, Ooi & Pedersen (JFE, 2012)  — Binary TSMOM
  - Moskowitz, Sabbatucci, Tamoni & Uhl (2024) — Nonlinear TSMOM

三种信号：
  binary    : sign(cum_log_ret_{t-252:t})               → {-1,0,+1}
  linear    : cum_log_ret / sigma                        → ℝ
  nonlinear : f(z) = z/(z²+1), z=cum_log_ret/sigma      → (-0.5, 0.5)

头寸（共同）：weight = signal / sigma_ewma
回测（共同）：VectorizedBacktest，lag=1，vol_target=10%

用法（在 cta_lab/ 目录下执行）：
    python scripts/run_tsmom_comparison.py \\
        --data-dir /path/to/market_data/kline/china_daily_full \\
        --out-dir  /path/to/output

输出目录：<cta_lab 上级>/research_outputs/tsmom_comparison/
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

_HERE = Path(__file__).resolve().parent
_CTA_LAB = _HERE.parent   # scripts/ -> cta_lab/
if str(_CTA_LAB) not in sys.path:
    sys.path.insert(0, str(_CTA_LAB))

from analysis.metrics import pnl_stats, annual_stats
from analysis.report.charts import plot_nav_with_drawdown, plot_annual_bar, plot_rolling_sharpe

from strategies.implementations.tsmom_backtest.strategy import TSMOMStrategy
from strategies.implementations.tsmom_backtest.config import TRADING_DAYS, EXCLUDE, MIN_OBS
from data.loader import DataLoader, KlineSchema
from data.sources.parquet_source import ParquetSource
from signals.momentum.nltsmom import NLTSMOMSignal, SignalMode


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TSMOM 三模式对比回测")
    p.add_argument(
        "--data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "china_daily_full"),
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "tsmom_comparison"),
    )
    p.add_argument("--lookback", type=int, default=252)
    p.add_argument("--verbose", action="store_true", default=True)
    return p.parse_args()




# ── 图表 ──────────────────────────────────────────────────────────────────────

COLORS = {
    "binary":    "steelblue",
    "linear":    "seagreen",
    "nonlinear": "darkorange",
}
LABELS = {
    "binary":    "Binary TSMOM  sign(s)",
    "linear":    "Linear TSMOM  z=s/σ",
    "nonlinear": "Nonlinear FS  z/(z²+1)",
}




def _plot_signal_function(out_path: str) -> None:
    z = np.linspace(-4, 4, 400)
    binary = np.sign(z)
    linear = z
    nonlinear = z / (z ** 2 + 1)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(z, binary,    color=COLORS["binary"],    linewidth=1.8, label="Binary  sign(z)")
    ax.plot(z, linear,    color=COLORS["linear"],    linewidth=1.8, label="Linear  z",
            linestyle="--")
    ax.plot(z, nonlinear, color=COLORS["nonlinear"], linewidth=2.0,
            label="Nonlinear FS  z/(z²+1)")
    ax.axhline(0, color="gray", linewidth=0.8, linestyle=":")
    ax.axvline(0, color="gray", linewidth=0.8, linestyle=":")
    ax.axvline( 1, color="gray", linewidth=0.6, linestyle="--", alpha=0.4)
    ax.axvline(-1, color="gray", linewidth=0.6, linestyle="--", alpha=0.4)
    ax.set_xlabel("Normalized signal  z = cum_log_ret / σ", fontsize=11)
    ax.set_ylabel("Position weight  f(z)", fontsize=11)
    ax.set_title("TSMOM Signal Functions\n(weight = f(z) / σ, applied per-asset)", fontsize=12)
    ax.legend(fontsize=10)
    ax.set_ylim(-2, 2)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _plot_weight_scatter(
    returns: pd.DataFrame,
    strategy: TSMOMStrategy,
    out_path: str,
) -> None:
    sig_lin = NLTSMOMSignal(
        lookback=strategy.lookback,
        sigma_halflife=strategy.sigma_halflife,
        mode=SignalMode.LINEAR,
        trading_days=strategy.trading_days,
    )
    sig_nl = NLTSMOMSignal(
        lookback=strategy.lookback,
        sigma_halflife=strategy.sigma_halflife,
        mode=SignalMode.NONLINEAR,
        trading_days=strategy.trading_days,
    )
    z_mat = sig_lin.compute(returns)
    nl_mat = sig_nl.compute(returns)

    z_vals = z_mat.values.flatten()
    nl_vals = nl_mat.values.flatten()
    mask = np.isfinite(z_vals) & np.isfinite(nl_vals)
    z_vals, nl_vals = z_vals[mask], nl_vals[mask]

    clip = 6
    mask2 = np.abs(z_vals) < clip
    z_vals, nl_vals = z_vals[mask2], nl_vals[mask2]

    z_theory = np.linspace(-clip, clip, 400)
    nl_theory = z_theory / (z_theory ** 2 + 1)
    linear_theory = z_theory

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(z_vals, nl_vals, alpha=0.02, s=2, color=COLORS["nonlinear"], rasterized=True)
    ax.plot(z_theory, nl_theory, color=COLORS["nonlinear"], linewidth=2,
            label="Nonlinear FS  z/(z²+1)")
    ax.plot(z_theory, linear_theory, color=COLORS["linear"], linewidth=1.5,
            linestyle="--", label="Linear  z")
    ax.axhline(0, color="gray", linewidth=0.6)
    ax.axvline(0, color="gray", linewidth=0.6)
    ax.set_xlabel("z = cum_log_ret / σ", fontsize=11)
    ax.set_ylabel("Nonlinear signal f(z)", fontsize=11)
    ax.set_title("Actual Signal Distribution vs Theoretical Functions\n"
                 "(each point = one asset-day in sample)", fontsize=11)
    ax.legend(fontsize=10)
    ax.set_xlim(-clip, clip)
    ax.set_ylim(-0.6, 0.6)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


# ── 主流程 ────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()
    out_dir = Path(args.out_dir)
    dirs = {
        "reports": out_dir / "reports",
        "charts":  out_dir / "charts",
        "data":    out_dir / "data",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)

    strategy = TSMOMStrategy(config={"lookback": args.lookback})

    if args.verbose:
        print("=" * 65)
        print("Step 1: Load china_daily_full returns (shared)")
        print("=" * 65)

    loader = DataLoader(
        kline_source=ParquetSource(args.data_dir),
        kline_schema=KlineSchema.tushare(),
    )
    tickers = loader.available_symbols(exclude=set(EXCLUDE))
    returns = loader.load_returns_matrix(tickers, min_obs=MIN_OBS)

    if args.verbose:
        print(
            f"\nReturns matrix: {returns.shape}  "
            f"({returns.index[0].date()} - {returns.index[-1].date()})"
        )

    returns.to_parquet(dirs["data"] / "returns.parquet")

    modes = ["binary", "linear", "nonlinear"]
    pnl_dict: dict[str, pd.Series] = {}
    pos_dict: dict[str, pd.DataFrame] = {}
    sigma = strategy._compute_sigma(returns)
    bt = strategy._make_backtest()

    for mode in modes:
        if args.verbose:
            print(f"\n{'=' * 65}")
            print(f"Step 2: {LABELS[mode]}  (mode={mode})")
            print("=" * 65)

        signal = strategy.generate_signals(returns, mode=mode)
        weight = strategy.build_weights(signal, sigma)
        pos_dict[mode] = weight

        result = bt.run(weight, returns)
        pnl = result.returns.iloc[1:]
        pnl_dict[mode] = pnl

        strategy._print_summary(pnl, LABELS[mode])

    print("\n" + "=" * 65)
    print("Full-Sample Summary")
    print("=" * 65)
    summary_rows = []
    for mode in modes:
        row = pnl_stats(pnl_dict[mode], include_skew=True)
        row["Mode"] = LABELS[mode]
        summary_rows.append(row)
    summary_df = pd.DataFrame(summary_rows).set_index("Mode")
    print(summary_df.to_string())
    summary_df.to_csv(dirs["reports"] / "full_sample_comparison.csv")

    print("\n" + "=" * 65)
    print("Annual Sharpe Comparison")
    print("=" * 65)
    ann_dict: dict[str, pd.DataFrame] = {}
    for mode in modes:
        ann_dict[mode] = annual_stats(pnl_dict[mode])

    ann_compare = pd.concat(
        {LABELS[m]: ann_dict[m]["Sharpe"] for m in modes}, axis=1
    )
    print(ann_compare.to_string())
    ann_compare.to_csv(dirs["reports"] / "annual_sharpe_comparison.csv")
    pd.concat(
        {LABELS[m]: ann_dict[m]["Return(%)"] for m in modes}, axis=1
    ).to_csv(dirs["reports"] / "annual_return_comparison.csv")

    print("\n" + "=" * 65)
    print("Charts")
    print("=" * 65)

    _pnl_named  = {LABELS[m]: pnl_dict[m] for m in modes}
    _colors_named = {LABELS[m]: COLORS[m] for m in modes}
    _ann_named  = {LABELS[m]: ann_dict[m] for m in modes}

    fig = plot_nav_with_drawdown(
        _pnl_named,
        title=f"China Domestic Futures — TSMOM Signal Comparison (lookback={args.lookback}d)\n"
              "Weight = signal / σ_ewma, EWMA vol-targeting 10% ann.",
        colors=_colors_named,
    )
    fig.savefig(str(dirs["charts"] / "nav_comparison.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  Saved nav_comparison.png")

    fig = plot_annual_bar(
        _ann_named,
        value_col="Sharpe",
        title="Annual Sharpe Comparison: Binary vs Linear vs Nonlinear TSMOM",
        colors=_colors_named,
    )
    fig.savefig(str(dirs["charts"] / "annual_sharpe_comparison.png"), dpi=150)
    plt.close(fig)
    print("  Saved annual_sharpe_comparison.png")

    fig = plot_rolling_sharpe(
        _pnl_named,
        title="TSMOM Signal Comparison — Rolling 1-Year Sharpe",
        colors=_colors_named,
    )
    fig.savefig(str(dirs["charts"] / "rolling_sharpe_comparison.png"), dpi=150)
    plt.close(fig)
    print("  Saved rolling_sharpe_comparison.png")

    _plot_signal_function(str(dirs["charts"] / "signal_functions.png"))
    print("  Saved signal_functions.png")

    _plot_weight_scatter(returns, strategy, str(dirs["charts"] / "weight_scatter.png"))
    print("  Saved weight_scatter.png")

    print(f"\nAll reports : {dirs['reports']}/")
    print(f"All charts  : {dirs['charts']}/")
    print("\n── Final Comparison ──")
    print(summary_df[["Return(%)", "Sharpe", "MaxDD(%)", "Calmar", "Skewness"]].to_string())
    print(
        f"\nUniverse: {returns.shape[1]} instruments  "
        f"Period: {returns.index[0].date()} - {returns.index[-1].date()}"
    )
    nl_sr  = summary_df.loc[LABELS["nonlinear"], "Sharpe"]
    bin_sr = summary_df.loc[LABELS["binary"],    "Sharpe"]
    print(f"\nNonlinear vs Binary Sharpe improvement: {(nl_sr/bin_sr - 1)*100:+.1f}%")


if __name__ == "__main__":
    main()
