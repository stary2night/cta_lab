"""China futures 6-sleeve portfolio study.

Sleeves:
  - MF Trend
  - MF CrossMom
  - NetMOM
  - Basis Momentum
  - Basis Value
  - Skew Reversal

Portfolios:
  - EqWeight(3), InvVol(3)
  - EqWeight(6), InvVol(6), RiskParity(6)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_CTA_LAB = _HERE.parent
if str(_CTA_LAB) not in sys.path:
    sys.path.insert(0, str(_CTA_LAB))

from analysis.metrics import annual_stats, monthly_pivot, pnl_stats
from analysis.report.charts import plot_annual_bar, plot_monthly_heatmap, plot_nav_with_drawdown
from analysis.report.output import BacktestOutput
from backtest import ProportionalCostModel
from backtest.vectorized import VectorizedBacktest


TRADING_DAYS = 252


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="China futures 6-sleeve portfolio study")
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "trend_cross_netmom_basis_skew_combo_china"),
    )
    p.add_argument(
        "--mf-trend-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "multifactor_cta_china" / "runs" / "01_trend_only"),
    )
    p.add_argument(
        "--mf-cross-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "multifactor_cta_china" / "runs" / "09_cross_only_globalew"),
    )
    p.add_argument(
        "--netmom-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "netmom_china_v2_baseline" / "trading_default"),
    )
    p.add_argument(
        "--basis-momentum-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "basis_momentum_china"),
    )
    p.add_argument(
        "--basis-value-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "basis_value_china"),
    )
    p.add_argument(
        "--skew-reversal-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "skew_reversal_china"),
    )
    p.add_argument("--rp-lookback", type=int, default=63)
    p.add_argument("--rp-vol-floor", type=float, default=1e-6)
    return p.parse_args()


def _pnl_from_positions(
    pos: pd.DataFrame,
    ret: pd.DataFrame,
    cost_rate: float,
    vol_target: float,
    max_abs_weight: float | None = None,
    max_gross_exposure: float | None = None,
) -> pd.Series:
    bt = VectorizedBacktest(
        lag=1,
        vol_target=vol_target,
        vol_halflife=21,
        trading_days=TRADING_DAYS,
        cost_model=ProportionalCostModel(cost_rate),
        max_abs_weight=max_abs_weight,
        max_gross_exposure=max_gross_exposure,
    )
    return bt.run(pos, ret).returns.iloc[1:]


def _load_sleeve_returns(name: str, run_dir: Path) -> pd.Series:
    if name == "NetMOM":
        pos = pd.read_parquet(run_dir / "signals" / "netmom_positions.parquet")
        ret = pd.read_parquet(run_dir / "data" / "returns.parquet")
        cfg = json.loads((run_dir / "data" / "asset_list.json").read_text())["strategy_config"]
        pnl = _pnl_from_positions(
            pos=pos,
            ret=ret,
            cost_rate=float(cfg["fee_rate"]),
            vol_target=0.10,
            max_abs_weight=float(cfg["max_abs_weight"]),
            max_gross_exposure=float(cfg["max_gross_exposure"]),
        )
    elif name in {"MF_Trend", "MF_Cross"}:
        pos = pd.read_parquet(run_dir / "signals" / "positions.parquet")
        ret = pd.read_parquet(run_dir / "data" / "returns.parquet")
        pnl = _pnl_from_positions(
            pos=pos,
            ret=ret,
            cost_rate=5.0 / 10_000.0,
            vol_target=0.10,
        )
    elif name == "BasisMomentum":
        pos = pd.read_parquet(run_dir / "signals" / "positions.parquet")
        ret = pd.read_parquet(run_dir / "data" / "returns.parquet")
        pnl = _pnl_from_positions(
            pos=pos,
            ret=ret,
            cost_rate=5.0 / 10_000.0,
            vol_target=None,
            max_gross_exposure=1.0,
        )
    elif name == "BasisValue":
        pnl = pd.read_parquet(run_dir / "reports" / "daily_pnl.parquet").iloc[:, 0]
    elif name == "SkewReversal":
        pos = pd.read_parquet(run_dir / "signals" / "positions.parquet")
        ret = pd.read_parquet(run_dir / "data" / "returns.parquet")
        pnl = _pnl_from_positions(
            pos=pos,
            ret=ret,
            cost_rate=5.0 / 10_000.0,
            vol_target=0.05,
            max_gross_exposure=1.0,
        )
    else:
        raise ValueError(f"Unsupported sleeve: {name}")

    pnl.name = name
    return pnl


def _inverse_vol_weights(returns_df: pd.DataFrame) -> pd.Series:
    vol = returns_df.std() * np.sqrt(TRADING_DAYS)
    inv = 1.0 / vol.replace(0.0, np.nan)
    w = inv / inv.sum()
    return w.fillna(0.0)


def _risk_parity_weights(cov: np.ndarray, n_iter: int = 200, tol: float = 1e-8) -> np.ndarray:
    n = cov.shape[0]
    w = np.full(n, 1.0 / n, dtype=float)
    cov = np.asarray(cov, dtype=float)
    cov = (cov + cov.T) / 2.0
    for _ in range(n_iter):
        mrc = cov @ w
        rc = w * mrc
        port_var = float(w @ mrc)
        if port_var <= 0:
            return np.full(n, 1.0 / n, dtype=float)
        target = port_var / n
        rc_safe = np.where(rc <= 0, 1e-12, rc)
        w_next = w * (target / rc_safe)
        w_next = np.clip(w_next, 1e-12, None)
        w_next = w_next / w_next.sum()
        if np.max(np.abs(w_next - w)) < tol:
            w = w_next
            break
        w = w_next
    return w / w.sum()


def _build_risk_parity_combo(returns_df: pd.DataFrame, lookback: int, vol_floor: float) -> tuple[pd.Series, pd.DataFrame]:
    cols = returns_df.columns.tolist()
    eq = np.full(len(cols), 1.0 / len(cols), dtype=float)
    weights = pd.DataFrame(index=returns_df.index, columns=cols, dtype=float)
    for i, dt in enumerate(returns_df.index):
        hist = returns_df.iloc[max(0, i - lookback) : i]
        if len(hist) < max(20, len(cols) * 5):
            weights.loc[dt] = eq
            continue
        cov = hist.cov().values
        cov = np.nan_to_num(cov, nan=0.0)
        cov[np.diag_indices_from(cov)] = np.maximum(np.diag(cov), vol_floor)
        weights.loc[dt] = _risk_parity_weights(cov)
    shifted = weights.shift(1).fillna(pd.Series(eq, index=cols))
    combo = (shifted * returns_df).sum(axis=1)
    combo.name = "RiskParity(6)"
    return combo, weights


def _make_corr_heatmap(corr: pd.DataFrame, title: str):
    fig, ax = plt.subplots(figsize=(7.5, 6.5))
    im = ax.imshow(corr.values, cmap="RdYlGn", vmin=-1.0, vmax=1.0)
    ax.set_xticks(range(len(corr.columns)))
    ax.set_xticklabels(corr.columns, rotation=30, ha="right")
    ax.set_yticks(range(len(corr.index)))
    ax.set_yticklabels(corr.index)
    ax.set_title(title)
    for i in range(corr.shape[0]):
        for j in range(corr.shape[1]):
            ax.text(j, i, f"{corr.iat[i, j]:.2f}", ha="center", va="center", fontsize=8)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    return fig


def _summary_row(name: str, pnl: pd.Series) -> dict[str, float | str]:
    summary = pnl_stats(pnl)
    return {
        "Strategy": name,
        "Return(%)": summary["Return(%)"],
        "Vol(%)": summary["Vol(%)"],
        "Sharpe": summary["Sharpe"],
        "MaxDD(%)": summary["MaxDD(%)"],
        "Calmar": summary["Calmar"],
        "HitRate(%)": summary["HitRate(%)"],
    }


def main() -> None:
    args = _parse_args()
    out = BacktestOutput(args.out_dir, subdirs=["reports", "charts", "data"])

    sleeve_paths = {
        "MF_Trend": Path(args.mf_trend_dir),
        "MF_Cross": Path(args.mf_cross_dir),
        "NetMOM": Path(args.netmom_dir),
        "BasisMomentum": Path(args.basis_momentum_dir),
        "BasisValue": Path(args.basis_value_dir),
        "SkewReversal": Path(args.skew_reversal_dir),
    }
    sleeve_returns = pd.concat(
        [_load_sleeve_returns(name, path) for name, path in sleeve_paths.items()],
        axis=1,
        join="inner",
    ).dropna(how="any")

    core3 = sleeve_returns[["MF_Trend", "MF_Cross", "NetMOM"]]
    all6 = sleeve_returns

    eq3_w = pd.Series(1.0 / core3.shape[1], index=core3.columns)
    inv3_w = _inverse_vol_weights(core3)
    eq6_w = pd.Series(1.0 / all6.shape[1], index=all6.columns)
    inv6_w = _inverse_vol_weights(all6)

    combo_returns = pd.DataFrame(
        {
            "EqWeight(3)": core3.mul(eq3_w, axis=1).sum(axis=1),
            "InvVol(3)": core3.mul(inv3_w, axis=1).sum(axis=1),
            "EqWeight(6)": all6.mul(eq6_w, axis=1).sum(axis=1),
            "InvVol(6)": all6.mul(inv6_w, axis=1).sum(axis=1),
        }
    )
    rp6, rp6_weights = _build_risk_parity_combo(all6, args.rp_lookback, args.rp_vol_floor)
    combo_returns["RiskParity(6)"] = rp6

    all_returns = pd.concat([sleeve_returns, combo_returns], axis=1)
    corr_daily = sleeve_returns.corr()
    corr_monthly = ((1.0 + sleeve_returns).resample("ME").prod() - 1.0).corr()

    summary_df = pd.DataFrame(
        [_summary_row(name, all_returns[name]) for name in all_returns.columns]
    )
    comparison_df = summary_df[summary_df["Strategy"].isin(["EqWeight(3)", "InvVol(3)", "EqWeight(6)", "InvVol(6)", "RiskParity(6)"])]

    out.save_csv(summary_df.set_index("Strategy"), "reports", "summary.csv")
    out.save_csv(comparison_df.set_index("Strategy"), "reports", "comparison_3vs6.csv")
    out.save_csv(corr_daily, "reports", "correlation.csv")
    out.save_parquet(all_returns, "data", "daily_pnl.parquet")
    out.save_parquet(combo_returns, "data", "combo_returns.parquet")
    out.save_parquet(pd.DataFrame({"EqWeight(3)": eq3_w, "InvVol(3)": inv3_w}), "data", "weights_core3.parquet")
    out.save_parquet(pd.DataFrame({"EqWeight(6)": eq6_w, "InvVol(6)": inv6_w}), "data", "weights_all6_static.parquet")
    out.save_parquet(rp6_weights, "data", "weights_riskparity6.parquet")
    out.save_json(
        {
            "sleeve_paths": {k: str(v) for k, v in sleeve_paths.items()},
            "risk_parity_lookback": args.rp_lookback,
        },
        "data",
        "run_config.json",
    )

    for name in combo_returns.columns:
        out.save_csv(annual_stats(combo_returns[name]), "reports", f"annual_{name.lower().replace('(', '').replace(')', '').replace(' ', '_')}.csv")
        out.save_csv(monthly_pivot(combo_returns[name]), "reports", f"monthly_{name.lower().replace('(', '').replace(')', '').replace(' ', '_')}.csv")

    out.save_fig(
        _make_corr_heatmap(corr_daily, "6-Sleeve Correlation (Daily)"),
        "charts",
        "corr_heatmap_6sleeve.png",
        dpi=150,
        bbox_inches="tight",
    )
    out.save_fig(
        plot_nav_with_drawdown(
            {
                "Trend": sleeve_returns["MF_Trend"],
                "CrossMom": sleeve_returns["MF_Cross"],
                "NetMOM": sleeve_returns["NetMOM"],
                "BasisMom": sleeve_returns["BasisMomentum"],
                "BasisValue": sleeve_returns["BasisValue"],
                "SkewRev": sleeve_returns["SkewReversal"],
            },
            title="China Futures 6 Sleeves - Individual NAV",
        ),
        "charts",
        "nav_6sleeve_individual.png",
        dpi=150,
        bbox_inches="tight",
    )
    out.save_fig(
        plot_nav_with_drawdown(
            {
                "EqWeight(3)": combo_returns["EqWeight(3)"],
                "InvVol(3)": combo_returns["InvVol(3)"],
                "EqWeight(6)": combo_returns["EqWeight(6)"],
                "InvVol(6)": combo_returns["InvVol(6)"],
                "RiskParity(6)": combo_returns["RiskParity(6)"],
            },
            title="China Futures 3 vs 6 Sleeve Portfolio NAV",
        ),
        "charts",
        "nav_3vs6_comparison.png",
        dpi=150,
        bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar(
            {
                "EqWeight(6)": annual_stats(combo_returns["EqWeight(6)"]),
                "InvVol(6)": annual_stats(combo_returns["InvVol(6)"]),
                "RiskParity(6)": annual_stats(combo_returns["RiskParity(6)"]),
            },
            title="China Futures 6 Sleeve Portfolio - Annual Returns",
        ),
        "charts",
        "annual_bar_6sleeve.png",
        dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(
            monthly_pivot(combo_returns["RiskParity(6)"]),
            title="China Futures RiskParity(6) - Monthly Returns (%)",
        ),
        "charts",
        "monthly_heatmap_riskparity6.png",
        dpi=150,
        bbox_inches="tight",
    )

    print("\nSummary:")
    print(summary_df.round(3).to_string(index=False))
    print("\nOutputs:")
    out.summary()


if __name__ == "__main__":
    main()
