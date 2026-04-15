"""再平衡时间表生成器基类。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

from data.model.calendar import TradingCalendar


@dataclass
class RebalanceRecord:
    """单次再平衡记录，含计算日、调仓日和子组合索引。"""

    calc_date: pd.Timestamp
    adjust_date: pd.Timestamp
    sub_index: int  # 0 表示单子组合，GMAT3 为 0-3


class RebalanceScheduler(ABC):
    """再平衡时间表生成器基类。"""

    @abstractmethod
    def produce_schedule(
        self,
        calendar: TradingCalendar,
        start: str,
        end: str,
    ) -> list[RebalanceRecord]:
        """生成调仓计划列表。"""
