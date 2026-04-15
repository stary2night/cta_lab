import pandas as pd
import numpy as np

from signals.base import Signal


class AbsoluteMomentum(Signal):
    """绝对动量：V(t) / V(t-N) - 1（GMAT3 Mom2，价格相对变化）。"""

    def __init__(self, lookback: int = 252) -> None:
        """初始化 AbsoluteMomentum 信号。

        Args:
            lookback: 回望窗口，单位交易日，默认 252。
        """
        self.lookback = lookback

    def compute(self, prices: pd.Series) -> pd.Series:
        """计算绝对动量：当前价格相对 lookback 日前价格的变化比例。"""
        return prices / prices.shift(self.lookback) - 1
