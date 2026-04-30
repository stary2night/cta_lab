"""China futures trend + cross + NetMOM sleeve-combo backtest."""

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
    p = argparse.ArgumentParser(description="China futures sleeve combo backtest")
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "sleeve_combo_china"),
        help="output root directory",
    )
    p.add_argument(
        "--mf-trend-dir",
        default=str(
            _CTA_LAB.parent
            / "research_outputs"
            / "multifactor_cta_china"
            / "runs"
            / "01_trend_only"
        ),
    )
    p.add_argument(
        "--mf-cross-dir",
        default=str(
            _CTA_LAB.parent
            / "research_outputs"
            / "multifactor_cta_china"
            / "runs"
            / "09_cross_only_globalew"
        ),
    )
    p.add_argument(
        "--netmom-dir",
        default=str(
            _CTA_LAB.parent
            / "research_outputs"
            / "netmom_china_v2_baseline"
            / "trading_default"
        ),
    )
    p.add_argument("--rp-lookback", type=int, default=63)
    p.add_argument("--rp-vol-floor", type=float, default=1e-6)
    return p.parse_args()


def _load_pnl_from_run(name: str, run_dir: Path) -> pd.Series:
    if name == "NetMOM":
        pos = pd.read_parquet(run_dir / "signals" / "netmom_positions.parquet")
        ret = pd.read_parquet(run_dir / "data" / "returns.parquet")
        cfg = json.loads((run_dir / "data" / "asset_list.json").read_text())["strategy_config"]
        cost_rate = float(cfg["fee_rate"])
        max_abs = float(cfg["max_abs_weight"])
        max_gross = float(cfg["max_gross_exposure"])
    else:
        pos = pd.read_parquet(run_dir / "signals" / "positions.parquet")
        ret = pd.read_parquet(run_dir / "data" / "returns.parquet")
        cost_rate = 5.0 / 10_000.0
        max_abs = None
        max_gross = None

    bt = VectorizedBacktest(
        lag=1,
        vol_target=0.10,
        vol_halflife=21,
        trading_days=TRADING_DAYS,
        cost_model=ProportionalCostModel(cost_rate),
        max_abs_weight=max_abs,
        max_gross_exposure=max_gross,
    )
    pnl = bt.run(pos, ret).returns.iloc[1:]
    pnl.name = name
    return pnl


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


def _build_risk_parity_combo(
    sleeve_returns: pd.DataFrame,
    lookback: int,
    vol_floor: float,
) -> tuple[pd.Series, pd.DataFrame]:
    cols = sleeve_returns.columns.tolist()
    weights = pd.DataFrame(index=sleeve_returns.index, columns=cols, dtype=float)
    eq = np.full(len(cols), 1.0 / len(cols), dtype=float)
    for i, dt in enumerate(sleeve_returns.index):
        hist = sleeve_returns.iloc[max(0, i - lookback) : i]
        if len(hist) < max(20, len(cols) * 5):
            weights.loc[dt] = eq
            continue
        cov = hist.cov().values
        cov = np.nan_to_num(cov, nan=0.0)
        cov[np.diag_indices_from(cov)] = np.maximum(np.diag(cov), vol_floor)
        weights.loc[dt] = _risk_parity_weights(cov)
    shifted = weights.shift(1)
    shifted = shifted.fillna(pd.Series(eq, index=cols))
    combo = (shifted * sleeve_returns).sum(axis=1)
    combo.name = "RiskParity"
    return combo, weights


def _make_corr_heatmap(corr: pd.DataFrame, title: str):
    fig, ax = plt.subplots(figsize=(6.5, 5.5))
    im = ax.imshow(corr.values, cmap="RdYlGn", vmin=-1.0, vmax=1.0)
    ax.set_xticks(range(len(corr.columns)))
    ax.set_xticklabels(corr.columns, rotation=25, ha="right")
    ax.set_yticks(range(len(corr.index)))
    ax.set_yticklabels(corr.index)
    ax.set_title(title)
    for i in range(corr.shape[0]):
        for j in range(corr.shape[1]):
            ax.text(j, i, f"{corr.iat[i, j]:.2f}", ha="center", va="center", fontsize=9)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    return fig


def _summary_row(name: str, pnl: pd.Series) -> dict[str, float | str]:
    summary = pnl_stats(pnl)
    return {
        "strategy": name,
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
    }
    sleeve_returns = pd.concat(
        [_load_pnl_from_run(name, path) for name, path in sleeve_paths.items()],
        axis=1,
        join="inner",
    ).dropna(how="any")

    eq_weights = pd.DataFrame(
        1.0 / sleeve_returns.shape[1],
        index=sleeve_returns.index,
        columns=sleeve_returns.columns,
    )
    eq_combo = sleeve_returns.mean(axis=1)
    eq_combo.name = "EqualWeight"
    rp_combo, rp_weights = _build_risk_parity_combo(
        sleeve_returns=sleeve_returns,
        lookback=args.rp_lookback,
        vol_floor=args.rp_vol_floor,
    )

    combo_returns = pd.concat([eq_combo, rp_combo], axis=1)
    all_returns = pd.concat([sleeve_returns, combo_returns], axis=1)
    corr_daily = sleeve_returns.corr()
    corr_monthly = ((1.0 + sleeve_returns).resample("ME").prod() - 1.0).corr()
    summary_df = pd.DataFrame(
        [_summary_row(name, all_returns[name]) for name in all_returns.columns]
    ).set_index("strategy")

    annual_frames = {name: annual_stats(all_returns[name]) for name in all_returns.columns}
    monthly_frames = {name: monthly_pivot(all_returns[name]) for name in all_returns.columns}

    out.save_csv(corr_daily, "reports", "corr_daily.csv")
    out.save_csv(corr_monthly, "reports", "corr_monthly.csv")
    out.save_csv(summary_df, "reports", "full_sample_summary.csv")
    for name, df in annual_frames.items():
        out.save_csv(df, "reports", f"annual_{name.lower()}.csv")
    for name, df in monthly_frames.items():
        out.save_csv(df, "reports", f"monthly_{name.lower()}.csv")

    out.save_parquet(sleeve_returns, "data", "sleeve_returns.parquet")
    out.save_parquet(combo_returns, "data", "combo_returns.parquet")
    out.save_parquet(eq_weights, "data", "equal_weight_weights.parquet")
    out.save_parquet(rp_weights, "data", "risk_parity_weights.parquet")
    out.save_json(
        {
            "mf_trend_dir": str(sleeve_paths["MF_Trend"]),
            "mf_cross_dir": str(sleeve_paths["MF_Cross"]),
            "netmom_dir": str(sleeve_paths["NetMOM"]),
            "risk_parity_lookback": args.rp_lookback,
            "risk_parity_method": "rolling covariance + iterative equal risk contribution",
        },
        "data",
        "run_config.json",
    )

    out.save_fig(
        plot_nav_with_drawdown(
            {
                "MF Trend": sleeve_returns["MF_Trend"],
                "MF Cross": sleeve_returns["MF_Cross"],
                "NetMOM": sleeve_returns["NetMOM"],
                "EqualWeight": combo_returns["EqualWeight"],
                "RiskParity": combo_returns["RiskParity"],
            },
            title="China Futures Sleeve Combo Comparison",
        ),
        "charts",
        "nav_sleeves_and_combos.png",
        dpi=150,
        bbox_inches="tight",
    )
    out.save_fig(
        plot_nav_with_drawdown(
            {
                "EqualWeight": combo_returns["EqualWeight"],
                "RiskParity": combo_returns["RiskParity"],
            },
            title="China Futures Sleeve Combo NAV",
        ),
        "charts",
        "nav_combos.png",
        dpi=150,
        bbox_inches="tight",
    )
    out.save_fig(
        _make_corr_heatmap(corr_daily, "Sleeve Correlation (Daily)"),
        "charts",
        "corr_daily_heatmap.png",
        dpi=150,
        bbox_inches="tight",
    )
    out.save_fig(
        _make_corr_heatmap(corr_monthly, "Sleeve Correlation (Monthly)"),
        "charts",
        "corr_monthly_heatmap.png",
        dpi=150,
        bbox_inches="tight",
    )
    out.save_fig(
        plot_annual_bar(
            {
                "EqualWeight": annual_frames["EqualWeight"],
                "RiskParity": annual_frames["RiskParity"],
            },
            title="China Futures Sleeve Combo - Annual Returns",
        ),
        "charts",
        "annual_combo_bar.png",
        dpi=150,
    )
    out.save_fig(
        plot_monthly_heatmap(
            monthly_frames["RiskParity"],
            title="China Futures Sleeve Combo - RiskParity Monthly Returns (%)",
        ),
        "charts",
        "monthly_heatmap_risk_parity.png",
        dpi=150,
        bbox_inches="tight",
    )

    print("\nSummary:")
    print(summary_df.round(3).to_string())
    print("\nOutputs:")
    out.summary()


if __name__ == "__main__":
    main()
