"""Overlay study for BasisMomentum / Carry / SkewReversal on top of InvVol(3)."""

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
from backtest import ProportionalCostModel
from backtest.vectorized import VectorizedBacktest


TRADING_DAYS = 252


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="BM / Carry / Skew overlay study on top of InvVol(3)"
    )
    p.add_argument(
        "--core-source-parquet",
        default=str(
            _CTA_LAB.parent
            / "research_outputs"
            / "trend_cross_netmom_basis_skew_combo_china"
            / "data"
            / "daily_pnl.parquet"
        ),
    )
    p.add_argument(
        "--carry-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "carry_china_2014"),
    )
    p.add_argument(
        "--skew-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "skew_reversal_china_sector_cap"),
    )
    p.add_argument(
        "--out-dir",
        default=str(
            _CTA_LAB.parent
            / "research_outputs"
            / "trend_cross_netmom_bm_carry_skew_overlay_china"
        ),
    )
    p.add_argument("--start", default="2014-01-01")
    p.add_argument("--max-bm", type=float, default=0.20)
    p.add_argument("--max-carry", type=float, default=0.20)
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


def _load_return_from_positions(
    run_dir: Path,
    vol_target: float | None,
    max_gross_exposure: float,
    cost_bps: float,
) -> pd.Series:
    pos = pd.read_parquet(run_dir / "signals" / "positions.parquet")
    ret = pd.read_parquet(run_dir / "data" / "returns.parquet")
    bt = VectorizedBacktest(
        lag=1,
        vol_target=vol_target,
        vol_halflife=21,
        trading_days=TRADING_DAYS,
        max_gross_exposure=max_gross_exposure,
        cost_model=ProportionalCostModel(cost_bps / 10_000.0),
    )
    return bt.run(pos, ret).returns.iloc[1:]


def main() -> None:
    args = _parse_args()
    out = BacktestOutput(args.out_dir, subdirs=["reports", "charts", "data"])

    core_df = pd.read_parquet(args.core_source_parquet)
    base_ret = core_df["InvVol(3)"]
    bm = core_df["BasisMomentum"]
    carry = _load_return_from_positions(
        run_dir=Path(args.carry_dir),
        vol_target=None,
        max_gross_exposure=1.0,
        cost_bps=5.0,
    )
    skew = _load_return_from_positions(
        run_dir=Path(args.skew_dir),
        vol_target=0.05,
        max_gross_exposure=1.0,
        cost_bps=5.0,
    )

    df = pd.concat(
        {
            "InvVol(3)": base_ret,
            "BasisMomentum": bm,
            "Carry": carry,
            "SkewReversal": skew,
        },
        axis=1,
        join="inner",
    )
    df = df.loc[df.index >= pd.Timestamp(args.start)].dropna(how="any")

    base_stats = _stats(df["InvVol(3)"])
    rows = []

    w_bm = 0.0
    while w_bm <= args.max_bm + 1e-12:
        w_carry = 0.0
        while w_carry <= args.max_carry + 1e-12:
            w_skew = 0.0
            while w_skew <= args.max_skew + 1e-12:
                overlay = w_bm + w_carry + w_skew
                if overlay <= args.max_total_overlay + 1e-12:
                    w_core = 1.0 - overlay
                    combo = (
                        w_core * df["InvVol(3)"]
                        + w_bm * df["BasisMomentum"]
                        + w_carry * df["Carry"]
                        + w_skew * df["SkewReversal"]
                    )
                    stats = _stats(combo)
                    rows.append(
                        {
                            "w_core": round(w_core, 3),
                            "w_bm": round(w_bm, 3),
                            "w_carry": round(w_carry, 3),
                            "w_skew": round(w_skew, 3),
                            **stats,
                            "ret_vs_base": stats["ann_return"] / base_stats["ann_return"],
                            "mdd_improvement": stats["max_drawdown"] - base_stats["max_drawdown"],
                        }
                    )
                w_skew += args.step
            w_carry += args.step
        w_bm += args.step

    grid = pd.DataFrame(rows)
    out.save_csv(grid, "reports", "overlay_grid.csv")

    base_row = grid[
        (grid["w_bm"] == 0.0) & (grid["w_carry"] == 0.0) & (grid["w_skew"] == 0.0)
    ].iloc[0]

    conservative = (
        grid[
            (grid["ret_vs_base"] >= 0.95)
            & (grid["max_drawdown"] > base_row["max_drawdown"])
        ]
        .sort_values(["sharpe", "max_drawdown"], ascending=[False, False])
        .head(10)
        .assign(tag="conservative")
    )
    defensive = (
        grid[(grid["ret_vs_base"] >= 0.90)]
        .sort_values(["max_drawdown", "sharpe"], ascending=[False, False])
        .head(10)
        .assign(tag="defensive")
    )
    return_focused = (
        grid[(grid["ann_return"] >= base_stats["ann_return"] * 0.98)]
        .sort_values(["sharpe", "ann_return"], ascending=[False, False])
        .head(10)
        .assign(tag="return_focused")
    )

    rec_df = pd.concat([conservative, defensive, return_focused], axis=0).drop_duplicates(
        subset=["w_core", "w_bm", "w_carry", "w_skew", "tag"]
    )
    out.save_csv(rec_df, "reports", "recommendations.csv")

    best_rows = []
    for tag in ["conservative", "defensive", "return_focused"]:
        subset = rec_df[rec_df["tag"] == tag]
        if not subset.empty:
            best_rows.append(subset.head(1))
    best_df = pd.concat(best_rows, axis=0).reset_index(drop=True)
    out.save_csv(best_df, "reports", "best_overlay_cases.csv")

    nav_dict = {"InvVol(3) Base": df["InvVol(3)"]}
    for _, row in best_df.iterrows():
        label = (
            f"{row['tag']} | BM={row['w_bm']:.1%} "
            f"Carry={row['w_carry']:.1%} Skew={row['w_skew']:.1%}"
        )
        nav_dict[label] = (
            row["w_core"] * df["InvVol(3)"]
            + row["w_bm"] * df["BasisMomentum"]
            + row["w_carry"] * df["Carry"]
            + row["w_skew"] * df["SkewReversal"]
        )

    out.save_parquet(df, "data", "daily_pnl_inputs.parquet")
    out.save_fig(
        plot_nav_with_drawdown(
            nav_dict,
            title="BM / Carry / Skew Overlay on Trend+Cross+NetMOM Core",
        ),
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
