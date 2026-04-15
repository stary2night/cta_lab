"""费用模型基类。"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class FeeModel(ABC):
    """费用模型基类。"""

    @abstractmethod
    def daily_fee(
        self,
        date: pd.Timestamp,
        nav: float,
        holdings: pd.Series,       # 当前持仓权重
        prev_holdings: pd.Series,  # 上期持仓权重
        is_rebalance: bool,        # 是否为调仓日
    ) -> float:
        """返回当日费用（占 NAV 的比例）。"""
