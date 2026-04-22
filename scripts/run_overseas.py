"""境外期货三策略对比回测入口。

在 overseas_daily_full/ 数据上同时运行：
  1. JPM t-stat 多周期趋势策略（lookbacks=[32,64,126,252,504]）
  2. TSMOM Binary 时序动量（lookback=252）
  3. Dual Momentum 双动量 L/S（lookback=252，板块内 top/bottom 50%）

用法（在 cta_lab/ 目录下执行）：
    python scripts/run_overseas.py \\
        --data-dir /path/to/market_data/kline/overseas_daily_full \\
        --out-dir  /path/to/output

默认输出：<cta_lab 上级>/research_outputs/overseas_comparison/
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
from strategies.implementations.overseas_backtest.strategy import OverseasTrendSuite


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="境外期货三策略对比回测")
    p.add_argument(
        "--data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "overseas_daily_full"),
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "overseas_comparison"),
    )
    p.add_argument("--start", default=None, help="回测起始日期 YYYY-MM-DD（默认全样本）")
    p.add_argument("--cost-bps", type=float, default=0.0, help="单边换手成本，单位 bps，默认0")
    p.add_argument("--verbose", action="store_true", default=True)
    return p.parse_args()


# ── 板块 Sharpe 热力图 ────────────────────────────────────────────────────────

def _plot_sector_heatmap(
    sector_dfs: dict[str, pd.DataFrame],
    labels: dict[str, str],
) -> "Figure":
    import matplotlib.pyplot as plt
    strats = list(sector_dfs.keys())
    sectors = sorted(set().union(*[set(sector_dfs[s].index) for s in strats]))
    data = np.full((len(sectors), len(strats)), np.nan)
    for j, strat in enumerate(strats):
        for i, sec in enumerate(sectors):
            if sec in sector_dfs[strat].index:
                data[i, j] = sector_dfs[strat].loc[sec, "Sharpe"]
    fig, ax = plt.subplots(figsize=(9, max(5, len(sectors)*0.45)))
    vmax = max(0.5, np.nanpercentile(np.abs(data), 90))
    im = ax.imshow(data, cmap="RdYlGn", aspect="auto", vmin=-vmax, vmax=vmax)
    ax.set_xticks(range(len(strats)))
    ax.set_xticklabels([labels[s] for s in strats], fontsize=8, rotation=10)
    ax.set_yticks(range(len(sectors)))
    ax.set_yticklabels(sectors, fontsize=9)
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            v = data[i, j]
            if not np.isnan(v):
                ax.text(j, i, f"{v:.2f}", ha="center", va="center",
                        fontsize=8, color="black" if abs(v) < vmax*0.6 else "white")
    plt.colorbar(im, ax=ax, label="Sharpe", shrink=0.7)
    ax.set_title("Overseas Futures — Sector Sharpe by Strategy", fontsize=12)
    plt.tight_layout()
    return fig


# ── 主流程 ────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    out = BacktestOutput(
        args.out_dir,
        subdirs=["reports", "charts", "data"],
    )

    suite = OverseasTrendSuite()
    loader = DataLoader(
        kline_source=ParquetSource(args.data_dir),
        kline_schema=KlineSchema.overseas(),
    )
    bt = VectorizedBacktest(
        lag=1,
        vol_target=suite.target_vol,
        vol_halflife=suite.vol_halflife,
        trading_days=suite.trading_days,
        cost_model=ProportionalCostModel(args.cost_bps / 10_000.0),
    )
    context = StrategyContext(
        loader=loader,
        sector_map=suite.sector_map,
        backtest=bt,
    )

    result = suite.run_pipeline(
        context=context,
        start=args.start,
        verbose=args.verbose,
    )

    returns = result.returns
    pnl_dict = result.pnl
    pos_dict = result.positions
    sym_sector = result.sector_map
    strategies = suite.strategies
    labels = suite.labels
    colors = suite.colors

    out.save_parquet(returns, "data", "returns.parquet")
    out.save_json(
        {"symbols": returns.columns.tolist(), "sector_map": sym_sector},
        "data", "asset_info.json",
    )

    print("\n" + "=" * 65)
    print("Full-Sample Summary (Overseas Futures)")
    print("=" * 65)
    summary_rows = []
    for strat in strategies:
        row = pnl_stats(pnl_dict[strat], include_skew=True)
        row.update(
            turnover_cost_summary(
                pos_dict[strat],
                args.cost_bps / 10_000.0,
                lag=bt.lag,
                trading_days=suite.trading_days,
            )
        )
        row["Strategy"] = labels[strat]
        summary_rows.append(row)
    summary_df = pd.DataFrame(summary_rows).set_index("Strategy")
    print(summary_df.to_string())
    out.save_csv(summary_df, "reports", "full_sample_summary.csv")
    for strat in strategies:
        out.save_csv(
            turnover_cost_frame(
                pos_dict[strat],
                args.cost_bps / 10_000.0,
                lag=bt.lag,
            ),
            "reports",
            f"turnover_cost_{strat}.csv",
        )

    print("\n" + "=" * 65)
    print("Annual Sharpe")
    print("=" * 65)
    ann_dict: dict[str, pd.DataFrame] = {}
    for strat in strategies:
        ann_dict[strat] = annual_stats(pnl_dict[strat])

    ann_sharpe = pd.concat(
        {labels[s]: ann_dict[s]["Sharpe"] for s in strategies}, axis=1
    )
    print(ann_sharpe.to_string())
    out.save_csv(ann_sharpe, "reports", "annual_sharpe.csv")
    out.save_csv(
        pd.concat({labels[s]: ann_dict[s]["Return(%)"] for s in strategies}, axis=1),
        "reports", "annual_return.csv",
    )

    print("\n" + "=" * 65)
    print("Sector Contribution")
    print("=" * 65)
    sector_dfs: dict[str, pd.DataFrame] = {}
    for strat in strategies:
        sector_dfs[strat] = sector_stats(pos_dict[strat], returns, sym_sector, bt)
    print("\n-- JPM --")
    print(sector_dfs["jpm"][["Symbols","Return(%)","Sharpe","MaxDD(%)"]].to_string())
    print("\n-- TSMOM --")
    print(sector_dfs["tsmom"][["Symbols","Return(%)","Sharpe","MaxDD(%)"]].to_string())
    print("\n-- Dual L/S --")
    print(sector_dfs["dual_ls"][["Symbols","Return(%)","Sharpe","MaxDD(%)"]].to_string())
    for strat in strategies:
        out.save_csv(sector_dfs[strat], "reports", f"sector_{strat}.csv")

    mpiv = monthly_pivot(pnl_dict["tsmom"])
    out.save_csv(mpiv, "reports", "monthly_tsmom.csv")

    # ── Charts ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Charts")
    print("=" * 65)

    _pnl_named    = {labels[s]: pnl_dict[s] for s in strategies}
    _colors_named = {labels[s]: colors[s]   for s in strategies}
    _ann_named    = {labels[s]: ann_dict[s] for s in strategies}

    out.save_fig(
        plot_nav_with_drawdown(
            _pnl_named,
            title="Overseas Futures — Strategy Comparison\nEWMA vol-targeting 10% ann., lag=1",
            colors=_colors_named,
        ),
        "charts", "nav_comparison.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar(
            _ann_named,
            value_col="Sharpe",
            title="Overseas Futures — Annual Sharpe by Strategy",
            colors=_colors_named,
        ),
        "charts", "annual_sharpe.png", dpi=150,
    )
    out.save_fig(
        plot_rolling_sharpe(
            _pnl_named,
            title="Overseas Futures — Rolling 1-Year Sharpe",
            colors=_colors_named,
        ),
        "charts", "rolling_sharpe.png", dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(
            mpiv,
            title="Overseas Futures TSMOM — Monthly Returns (%)",
        ),
        "charts", "monthly_heatmap_tsmom.png", dpi=150, bbox_inches="tight",
    )
    out.save_fig(
        _plot_sector_heatmap(sector_dfs, labels),
        "charts", "sector_heatmap.png", dpi=150,
    )

    print(f"\nAll outputs:")
    out.summary()
    print("\n── Final Comparison ──")
    print(summary_df[["Return(%)", "Sharpe", "MaxDD(%)", "Calmar", "Skewness"]].to_string())
    print(
        f"\nUniverse: {returns.shape[1]} instruments  "
        f"Period: {returns.index[0].date()} - {returns.index[-1].date()}"
    )


if __name__ == "__main__":
    main()
