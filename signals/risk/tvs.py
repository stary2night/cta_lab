import pandas as pd
import numpy as np

from signals.base import Signal


class TVS(Signal):
    """尾部波动锐度：Sharpe 代理序列与波动率序列的滚动相关性。

    TVS > 0 表示波动上升时 Sharpe 也上升（尾部风险可接受）。
    TVS < 0 表示波动上升时 Sharpe 下降（尾部风险恶化）。
    portfolio 层用 TVS 决定风险乘数（1.5x / 1.0x / 0.5x）。
    """

    def __init__(
        self,
        window: int = 260,
        sharpe_window: int = 20,
        vol_window: int = 20,
    ) -> None:
        """初始化 TVS 信号。

        Args:
            window: 计算滚动相关的窗口，单位交易日，默认 260。
            sharpe_window: 计算 Sharpe 代理（rolling mean / rolling std）的窗口，默认 20。
            vol_window: 计算波动率的窗口，默认 20。
        """
        self.window = window
        self.sharpe_window = sharpe_window
        self.vol_window = vol_window

    def compute(self, prices: pd.Series) -> pd.Series:
        """计算 TVS 信号：Sharpe 代理与波动率的滚动 Pearson 相关系数。"""
        log_returns = np.log(prices / prices.shift(1))
        rolling_mean = log_returns.rolling(self.sharpe_window).mean()
        rolling_std = log_returns.rolling(self.sharpe_window).std()
        sharpe_proxy = rolling_mean / rolling_std.replace(0, np.nan)
        vol = log_returns.rolling(self.vol_window).std()
        tvs = sharpe_proxy.rolling(self.window).corr(vol)
        return tvs
