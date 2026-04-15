import pandas as pd
import numpy as np

from signals.base import Signal

_ANNUALIZE = np.sqrt(252)


class SharpeMomentum(Signal):
    """Sharpe 动量：N日累计收益 / N日年化波动率（GMAT3 Mom1/Mom3）。"""

    def __init__(self, lookback: int = 252, vol_window: int | None = None) -> None:
        """初始化 SharpeMomentum 信号。

        Args:
            lookback: 计算累计收益的回望窗口，单位交易日，默认 252。
            vol_window: 计算波动率的窗口，默认与 lookback 相同。
        """
        self.lookback = lookback
        self.vol_window = vol_window if vol_window is not None else lookback

    def compute(self, prices: pd.Series) -> pd.Series:
        """计算 Sharpe 动量信号。

        返回累计对数收益除以年化波动率，波动率为零时返回 NaN。
        """
        log_returns = np.log(prices / prices.shift(1))
        cum_ret = log_returns.rolling(self.lookback).sum()
        vol = log_returns.rolling(self.vol_window).std() * _ANNUALIZE
        signal = cum_ret / vol.replace(0, np.nan)
        return signal
