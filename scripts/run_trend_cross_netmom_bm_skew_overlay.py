"""Targeted overlay study for BasisMomentum / SkewReversal on top of core3."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import pandas as pd

_HERE = Path(__file__).resolve().parent
_CTA_LAB = _HERE.parent
if str(_CTA_LAB) not in sys.path:
    sys.path.insert(0, str(_CTA_LAB))

from analysis.report.charts import plot_nav_with_drawdown
from analysis.report.output import BacktestOutput


TRADING_DAYS = 252


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="BM / Skew overlay study on top of core3")
    p.add_argument(
        "--source-parquet",
        default=str(
            _CTA_LAB.parent
            / "research_outputs"
            / "trend_cross_netmom_basis_skew_combo_china"
            / "data"
            / "daily_pnl.parquet"
        ),
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "trend_cross_netmom_bm_skew_overlay_china"),
    )
    p.add_argument("--max-bm", type=float, default=0.20)
    p.add_argument("--max-skew", type=float, default=0.20)
    p.add_argument("--max-total-overlay", type=float, default=0.30)
    p.add_argument("--step", type=float, default=0.025)
    return p.parse_args()


def _stats(s: pd.Series) -> dict[str, float]:
    ann_ret = float(s.mean() * TRADING_DAYS)
    ann_vol = float(s.std() * (TRADING_DAYS ** 0.5))
    sharpe = ann_ret / ann_vol if ann_vol > 0 else float("nan")
    nav = (1.0 + s).cumprod()
    mdd = float(((nav - nav.cummax()) / nav.cummax()).min())
    return {
        "ann_return": ann_ret,
        "ann_vol": ann_vol,
        "sharpe": sharpe,
        "max_drawdown": mdd,
    }


def main() -> None:
    args = _parse_args()
    out = BacktestOutput(args.out_dir, subdirs=["reports", "charts", "data"])

    df = pd.read_parquet(args.source_parquet)
    bases = ["EqWeight(3)", "InvVol(3)"]
    rows = []

    for base in bases:
        base_ret = df[base]
        bm = df["BasisMomentum"]
        skew = df["SkewReversal"]
        base_stats = _stats(base_ret)

        w = 0.0
        while w <= args.max_bm + 1e-12:
            u = 0.0
            while u <= args.max_skew + 1e-12:
                if w + u <= args.max_total_overlay + 1e-12:
                    core_w = 1.0 - w - u
                    combo = core_w * base_ret + w * bm + u * skew
                    stats = _stats(combo)
                    rows.append(
                        {
                            "base": base,
                            "w_core": round(core_w, 3),
                            "w_bm": round(w, 3),
                            "w_skew": round(u, 3),
                            **stats,
                            "ret_vs_base": stats["ann_return"] / base_stats["ann_return"],
                            "mdd_improvement": stats["max_drawdown"] - base_stats["max_drawdown"],
                        }
                    )
                u += args.step
            w += args.step

    grid = pd.DataFrame(rows)
    out.save_csv(grid, "reports", "overlay_grid.csv")

    recommendations = []
    for base in bases:
        base_row = grid[(grid["base"] == base) & (grid["w_bm"] == 0.0) & (grid["w_skew"] == 0.0)].iloc[0]
        # Keep at least 95% of base return and improve drawdown.
        conservative = (
            grid[
                (grid["base"] == base)
                & (grid["ret_vs_base"] >= 0.95)
                & (grid["max_drawdown"] > base_row["max_drawdown"])
            ]
            .sort_values(["sharpe", "max_drawdown"], ascending=[False, False])
            .head(3)
        )
        # Keep at least 90% return and prioritize lower drawdown.
        defensive = (
            grid[
                (grid["base"] == base)
                & (grid["ret_vs_base"] >= 0.90)
            ]
            .sort_values(["max_drawdown", "sharpe"], ascending=[False, False])
            .head(3)
        )
        conservative = conservative.assign(tag="conservative")
        defensive = defensive.assign(tag="defensive")
        recommendations.append(pd.concat([conservative, defensive], axis=0))

    rec_df = pd.concat(recommendations, axis=0).drop_duplicates(
        subset=["base", "w_core", "w_bm", "w_skew", "tag"]
    )
    out.save_csv(rec_df, "reports", "recommendations.csv")

    # Prepare a compact comparison table using the top recommendation of each bucket.
    best_rows = []
    for base in bases:
        subset = rec_df[rec_df["base"] == base]
        if subset.empty:
            continue
        best_rows.append(subset[subset["tag"] == "conservative"].head(1))
        best_rows.append(subset[subset["tag"] == "defensive"].head(1))
    best_df = pd.concat(best_rows, axis=0).reset_index(drop=True)
    out.save_csv(best_df, "reports", "best_overlay_cases.csv")

    nav_dict = {}
    for _, row in best_df.iterrows():
        label = (
            f"{row['base']} | {row['tag']} | "
            f"BM={row['w_bm']:.1%} Skew={row['w_skew']:.1%}"
        )
        nav_dict[label] = (
            row["w_core"] * df[row["base"]]
            + row["w_bm"] * df["BasisMomentum"]
            + row["w_skew"] * df["SkewReversal"]
        )
    # Include base cores for reference.
    nav_dict["EqWeight(3) Base"] = df["EqWeight(3)"]
    nav_dict["InvVol(3) Base"] = df["InvVol(3)"]

    out.save_fig(
        plot_nav_with_drawdown(nav_dict, title="BM / Skew Overlay on Trend+Cross+NetMOM Core"),
        "charts",
        "nav_overlay_cases.png",
        dpi=150,
        bbox_inches="tight",
    )

    print("\nBest overlay cases:")
    print(best_df.round(4).to_string(index=False))
    print("\nOutputs:")
    out.summary()


if __name__ == "__main__":
    main()
