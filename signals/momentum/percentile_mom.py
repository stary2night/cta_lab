import pandas as pd
import numpy as np

from signals.base import Signal


class PercentileMomentum(Signal):
    """分位数动量：当前价格在过去 N 日的分位数位置（GMAT3 Mom4）。

    输出范围 [-0.5, 0.5]，0 为中性，正值偏多，负值偏空。
    """

    def __init__(self, lookback: int = 252) -> None:
        """初始化 PercentileMomentum 信号。

        Args:
            lookback: 回望窗口，单位交易日，默认 252。
        """
        self.lookback = lookback

    def compute(self, prices: pd.Series) -> pd.Series:
        """计算分位数动量信号。

        对每个时间点 t，计算 prices[t] 在其 lookback 日滚动窗口内的历史百分位，
        然后中心化（减去 0.5），使中性信号为 0。
        """
        # 当前值在历史窗口中的“居中分位”：
        # P(x_hist < x_t) + 0.5 * P(x_hist == x_t)
        # 这样在 ties / 平盘时会回到更合理的中性值，而不是系统性偏空。
        pct = prices.rolling(self.lookback).apply(
            lambda x: (x[-1] > x[:-1]).mean() + 0.5 * (x[-1] == x[:-1]).mean(),
            raw=True,
        )
        return pct - 0.5
