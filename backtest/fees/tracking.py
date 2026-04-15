"""跟踪费（管理费）模型。"""

from __future__ import annotations

import pandas as pd

from .base import FeeModel


class TrackingFee(FeeModel):
    """跟踪费（管理费）：每日按年化费率计提。

    fee = annual_rate / 252
    每日扣除，与是否调仓无关。
    """

    def __init__(self, annual_rate: float = 0.005) -> None:
        self.annual_rate = annual_rate

    def daily_fee(
        self,
        date: pd.Timestamp,
        nav: float,
        holdings: pd.Series,
        prev_holdings: pd.Series,
        is_rebalance: bool,
    ) -> float:
        return self.annual_rate / 252
