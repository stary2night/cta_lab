"""单根 K 线数据结构与 K 线序列（含固有统计变换）。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

_ANNUALIZE = 252  # 年化因子（交易日）


@dataclass
class Bar:
    """单根日 K 线，期货关键字段包含结算价。"""

    date: date
    open: float
    high: float
    low: float
    close: float
    settle: float        # 结算价（期货定盈亏的核心价格）
    volume: float
    open_interest: float


class BarSeries:
    """K 线序列，以 settle 为主要价格序列，供策略层直接消费。

    Parameters
    ----------
    symbol:
        品种或合约代码。
    data:
        DataFrame，index 为 pd.DatetimeIndex，列名包含
        open / high / low / close / settle / volume / open_interest。
    """

    _REQUIRED_COLS = {"open", "high", "low", "close", "settle", "volume", "open_interest"}

    def __init__(self, symbol: str, data: pd.DataFrame) -> None:
        missing = self._REQUIRED_COLS - set(data.columns)
        if missing:
            raise ValueError(f"BarSeries data missing columns: {missing}")
        if not isinstance(data.index, pd.DatetimeIndex):
            raise TypeError("BarSeries data index must be pd.DatetimeIndex.")
        self.symbol = symbol
        self.data = data.sort_index()

    # ------------------------------------------------------------------
    # 固有变换方法
    # ------------------------------------------------------------------

    def log_returns(self) -> pd.Series:
        """计算 settle 价的对数日收益率序列。"""
        return np.log(self.data["settle"] / self.data["settle"].shift(1))

    def pct_returns(self) -> pd.Series:
        """计算 settle 价的百分比日收益率序列。"""
        return self.data["settle"].pct_change()

    def ewm_vol(self, halflife: int = 60) -> pd.Series:
        """计算 settle 对数收益的 EWM 年化波动率序列。"""
        lr = self.log_returns()
        return lr.ewm(halflife=halflife, min_periods=1).std() * np.sqrt(_ANNUALIZE)

    def rolling_vol(self, window: int = 20) -> pd.Series:
        """计算 settle 对数收益的滚动年化波动率序列。"""
        lr = self.log_returns()
        return lr.rolling(window=window).std() * np.sqrt(_ANNUALIZE)

    def drawdown(self) -> pd.Series:
        """计算基于 settle 价的水下回撤序列（值域 [−1, 0]）。"""
        settle = self.data["settle"]
        rolling_max = settle.cummax()
        return (settle - rolling_max) / rolling_max

    # ------------------------------------------------------------------
    # 容器协议
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        """返回序列长度（交易日数）。"""
        return len(self.data)

    def __getitem__(self, key: slice | str | pd.Timestamp) -> "BarSeries":
        """支持日期切片，返回新的 BarSeries 对象。"""
        sliced = self.data.loc[key]
        if isinstance(sliced, pd.Series):
            # 单行切片退化为 DataFrame
            sliced = sliced.to_frame().T
        return BarSeries(self.symbol, sliced)

    def __repr__(self) -> str:
        return (
            f"BarSeries(symbol={self.symbol!r}, "
            f"rows={len(self)}, "
            f"range=[{self.data.index[0].date()} ~ {self.data.index[-1].date()}])"
        )
