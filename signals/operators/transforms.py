"""通用信号算子，适用于时序信号和截面 score 的后处理。"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd


SignalLike = pd.Series | pd.DataFrame


def lag(values: SignalLike, periods: int = 1) -> SignalLike:
    """对信号做时间滞后，避免未来函数。"""
    return values.shift(periods)


def smooth(
    values: SignalLike,
    window: int,
    method: Literal["mean", "ewm"] = "mean",
) -> SignalLike:
    """平滑信号，支持滚动均值和 EWM。"""
    if window <= 0:
        raise ValueError("window must be positive.")
    if method == "mean":
        return values.rolling(window).mean()
    if method == "ewm":
        return values.ewm(span=window, adjust=False).mean()
    raise ValueError("method must be 'mean' or 'ewm'.")


def clip(values: SignalLike, lower: float = -3.0, upper: float = 3.0) -> SignalLike:
    """裁剪异常信号值。"""
    return values.clip(lower=lower, upper=upper)


def zscore(values: SignalLike, axis: int = 0, ddof: int = 0) -> SignalLike:
    """按列或按行标准化。"""
    mean = values.mean(axis=axis)
    std = values.std(axis=axis, ddof=ddof).replace(0, np.nan)
    if axis == 0:
        return values.subtract(mean, axis=1 if isinstance(values, pd.DataFrame) else 0).divide(std, axis=1 if isinstance(values, pd.DataFrame) else 0)
    if axis == 1:
        if not isinstance(values, pd.DataFrame):
            raise ValueError("axis=1 requires a DataFrame input.")
        return values.sub(mean, axis=0).div(std, axis=0)
    raise ValueError("axis must be 0 or 1.")


def rolling_zscore(values: SignalLike, window: int, ddof: int = 0) -> SignalLike:
    """基于历史窗口的时序 zscore。"""
    if window <= 0:
        raise ValueError("window must be positive.")
    mean = values.rolling(window).mean()
    std = values.rolling(window).std(ddof=ddof).replace(0, np.nan)
    return (values - mean) / std


def winsorize(
    values: SignalLike,
    lower_q: float = 0.05,
    upper_q: float = 0.95,
    axis: int = 0,
) -> SignalLike:
    """按分位数缩尾，支持时序或截面。"""
    if not 0.0 <= lower_q <= upper_q <= 1.0:
        raise ValueError("quantiles must satisfy 0 <= lower_q <= upper_q <= 1.")

    if isinstance(values, pd.Series):
        lower = values.quantile(lower_q)
        upper = values.quantile(upper_q)
        return values.clip(lower=lower, upper=upper)

    if axis == 0:
        lower = values.quantile(lower_q, axis=0)
        upper = values.quantile(upper_q, axis=0)
        return values.clip(lower=lower, upper=upper, axis=1)
    if axis == 1:
        lower = values.quantile(lower_q, axis=1)
        upper = values.quantile(upper_q, axis=1)
        return values.clip(lower=lower, upper=upper, axis=0)
    raise ValueError("axis must be 0 or 1.")


def cross_sectional_rank(
    values: pd.DataFrame,
    pct: bool = True,
    ascending: bool = True,
) -> pd.DataFrame:
    """对每日截面做排名，常用于 raw score 的标准化。"""
    return values.rank(axis=1, pct=pct, ascending=ascending)


def normalize_by_abs_sum(values: pd.DataFrame) -> pd.DataFrame:
    """按日把截面信号缩放到绝对值和为 1。"""
    denom = values.abs().sum(axis=1).replace(0, np.nan)
    return values.div(denom, axis=0)
