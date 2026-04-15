import pandas as pd
import numpy as np

from signals.base import Signal


class TSMOM(Signal):
    """时序动量信号：lookback 日对数收益之和的符号。"""

    def __init__(self, lookback: int = 252) -> None:
        """初始化 TSMOM 信号。

        Args:
            lookback: 回望窗口，单位交易日，默认 252（一年）。
        """
        self.lookback = lookback

    def compute(self, prices: pd.Series) -> pd.Series:
        """计算 lookback 日累计对数收益的符号。

        返回 {-1, 0, +1} 的浮点序列，前 lookback 个值为 NaN。
        """
        log_returns = np.log(prices / prices.shift(1))
        cum_ret = log_returns.rolling(self.lookback).sum()
        return np.sign(cum_ret)
