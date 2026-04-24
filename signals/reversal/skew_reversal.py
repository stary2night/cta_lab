from __future__ import annotations

import pandas as pd

from signals.base import Signal


class SkewReversalSignal(Signal):
    """多窗口偏度反转因子。

    因子值越高表示右偏越强，越偏向过热/做空；
    因子值越低表示左偏越强，越偏向超卖/做多。
    """

    def __init__(self, windows: tuple[int, ...] = (130, 195, 260)) -> None:
        if not windows or any(window <= 2 for window in windows):
            raise ValueError("windows must be non-empty and > 2")
        self.windows = tuple(int(window) for window in windows)

    def compute(self, returns: pd.Series) -> pd.Series:
        factors = [
            returns.rolling(window, min_periods=window).skew()
            for window in self.windows
        ]
        return pd.concat(factors, axis=1).mean(axis=1)

    def compute_from_returns(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        return self.compute_matrix(returns_df)
