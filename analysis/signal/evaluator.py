"""轻量 signal evaluation：IC / Rank IC / IR。"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .labels import build_forward_returns


@dataclass
class SignalEvaluationReport:
    """信号评估结果。"""

    summary: pd.DataFrame
    ic_series: dict[int, pd.Series]
    rank_ic_series: dict[int, pd.Series]
    future_returns: dict[int, pd.Series | pd.DataFrame]


def _rowwise_pearson(left: pd.DataFrame, right: pd.DataFrame) -> pd.Series:
    """按日期做截面 Pearson 相关，向量化实现。"""
    mask = left.notna() & right.notna()
    count = mask.sum(axis=1)

    left_valid = left.where(mask, 0.0)
    right_valid = right.where(mask, 0.0)

    denom = count.where(count > 0, np.nan)
    left_mean = left_valid.sum(axis=1).div(denom)
    right_mean = right_valid.sum(axis=1).div(denom)

    left_centered = left.sub(left_mean, axis=0).where(mask, 0.0)
    right_centered = right.sub(right_mean, axis=0).where(mask, 0.0)

    cov = (left_centered * right_centered).sum(axis=1)
    left_ss = (left_centered.pow(2)).sum(axis=1)
    right_ss = (right_centered.pow(2)).sum(axis=1)

    with np.errstate(invalid="ignore", divide="ignore"):
        corr = cov / np.sqrt(left_ss * right_ss)

    invalid = (count < 2) | (left_ss <= 0) | (right_ss <= 0)
    corr = corr.mask(invalid)
    return corr.astype(float)


def information_coefficient(
    signal_df: pd.DataFrame,
    future_returns_df: pd.DataFrame,
    rank: bool = False,
) -> pd.Series:
    """逐日截面 IC / Rank IC。"""
    common_dates = signal_df.index.intersection(future_returns_df.index)
    common_symbols = signal_df.columns.intersection(future_returns_df.columns)

    sig = signal_df.loc[common_dates, common_symbols]
    fwd = future_returns_df.loc[common_dates, common_symbols]

    if rank:
        sig = sig.rank(axis=1, method="average")
        fwd = fwd.rank(axis=1, method="average")

    result = _rowwise_pearson(sig, fwd)
    result.name = "rank_ic" if rank else "ic"
    return result


def information_ratio(ic_series: pd.Series, annualization: int = 252) -> float:
    """IC 序列的年化 IR。"""
    clean = ic_series.dropna()
    if len(clean) < 2:
        return float("nan")
    std = clean.std(ddof=0)
    if std == 0:
        return float("nan")
    return float(clean.mean() / std * np.sqrt(annualization))


def evaluate_signal(
    signal_df: pd.DataFrame,
    prices: pd.DataFrame | None = None,
    horizons: list[int] | tuple[int, ...] = (1, 5, 20, 60),
    future_returns: dict[int, pd.DataFrame] | None = None,
) -> SignalEvaluationReport:
    """评估截面信号对未来收益的预测能力。"""
    if future_returns is None:
        if prices is None:
            raise ValueError("Either prices or future_returns must be provided.")
        built = build_forward_returns(prices, horizons)
    else:
        built = {h: future_returns[h] for h in horizons}

    summary_records: list[dict[str, float | int]] = []
    ic_map: dict[int, pd.Series] = {}
    rank_ic_map: dict[int, pd.Series] = {}

    for horizon, fwd in built.items():
        if not isinstance(fwd, pd.DataFrame):
            raise ValueError("evaluate_signal currently expects DataFrame prices/signals.")

        ic_series = information_coefficient(signal_df, fwd, rank=False)
        rank_ic_series = information_coefficient(signal_df, fwd, rank=True)

        ic_map[horizon] = ic_series
        rank_ic_map[horizon] = rank_ic_series

        summary_records.append(
            {
                "horizon": horizon,
                "ic_mean": float(ic_series.mean(skipna=True)),
                "ic_std": float(ic_series.std(skipna=True, ddof=0)),
                "ic_ir": information_ratio(ic_series),
                "rank_ic_mean": float(rank_ic_series.mean(skipna=True)),
                "rank_ic_std": float(rank_ic_series.std(skipna=True, ddof=0)),
                "rank_ic_ir": information_ratio(rank_ic_series),
                "n_obs": int(ic_series.notna().sum()),
            }
        )

    summary = pd.DataFrame(summary_records).set_index("horizon")
    return SignalEvaluationReport(
        summary=summary,
        ic_series=ic_map,
        rank_ic_series=rank_ic_map,
        future_returns=built,
    )
