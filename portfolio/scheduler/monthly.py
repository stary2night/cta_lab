"""月度调仓调度器（CTA）：每月最后一个交易日计算，次交易日调仓。"""

from __future__ import annotations

import pandas as pd

from data.model.calendar import TradingCalendar
from .base import RebalanceRecord, RebalanceScheduler


class MonthlyScheduler(RebalanceScheduler):
    """月度调仓：每月最后一个交易日计算，次交易日调仓（CTA）。"""

    def __init__(self, lag: int = 1) -> None:
        """初始化，lag 为计算日到调仓日的交易日间隔，默认 1。"""
        self.lag = lag

    def produce_schedule(
        self,
        calendar: TradingCalendar,
        start: str,
        end: str,
    ) -> list[RebalanceRecord]:
        """生成月度调仓计划，每月最后交易日为 calc_date，offset lag 日为 adjust_date。"""
        month_ends = calendar.get_month_end_dates(start, end)
        records: list[RebalanceRecord] = []
        for calc_date in month_ends:
            try:
                adjust_date = calendar.offset(calc_date, self.lag)
            except ValueError:
                # 超出日历范围时跳过
                continue
            records.append(
                RebalanceRecord(
                    calc_date=pd.Timestamp(calc_date),
                    adjust_date=pd.Timestamp(adjust_date),
                    sub_index=0,
                )
            )
        return records
