"""future return 标签构造。"""

from __future__ import annotations

import numpy as np
import pandas as pd


def forward_return(
    prices: pd.Series | pd.DataFrame,
    horizon: int = 1,
) -> pd.Series | pd.DataFrame:
    """构造未来 horizon 期简单收益率标签。

    标签定义：
        label_t = price_{t+horizon} / price_t - 1
    """
    if horizon <= 0:
        raise ValueError("horizon must be positive.")
    return prices.shift(-horizon).div(prices).sub(1.0)


def forward_log_return(
    prices: pd.Series | pd.DataFrame,
    horizon: int = 1,
) -> pd.Series | pd.DataFrame:
    """构造未来 horizon 期对数收益率标签。"""
    if horizon <= 0:
        raise ValueError("horizon must be positive.")
    return np.log(prices.shift(-horizon) / prices)


def build_forward_returns(
    prices: pd.Series | pd.DataFrame,
    horizons: list[int] | tuple[int, ...],
    log: bool = False,
) -> dict[int, pd.Series | pd.DataFrame]:
    """批量构造多个 horizon 的 future return 标签。"""
    builder = forward_log_return if log else forward_return
    return {h: builder(prices, horizon=h) for h in horizons}
