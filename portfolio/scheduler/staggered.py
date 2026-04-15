"""错峰多子组合调度器（GMAT3）。"""

from __future__ import annotations

import pandas as pd

from data.model.calendar import TradingCalendar
from .base import RebalanceRecord, RebalanceScheduler


class StaggeredScheduler(RebalanceScheduler):
    """通用错峰多子组合调度器（GMAT3）。

    GMAT3 配置：n_sub=4，子组合 0 在每月第 2 个交易日计算、第 4 个交易日调仓，
    子组合 1/2/3 各错后 1 个交易日。
    """

    def __init__(
        self,
        n_sub: int = 4,
        first_calc_offset: int = 1,
        calc_to_adjust_lag: int = 2,
        blend_weights: list[float] | None = None,
    ) -> None:
        """初始化错峰调度器。

        n_sub：子组合数量。
        first_calc_offset：月内第一个子组合计算日的 0-indexed 偏移（默认 1，即第2个交易日）。
        calc_to_adjust_lag：计算日到调仓日的交易日间隔。
        blend_weights：各子组合融合权重，None 表示等权。
        """
        self.n_sub = n_sub
        self.first_calc_offset = first_calc_offset
        self.calc_to_adjust_lag = calc_to_adjust_lag
        self._blend_weights = blend_weights

    def produce_schedule(
        self,
        calendar: TradingCalendar,
        start: str,
        end: str,
    ) -> list[RebalanceRecord]:
        """生成错峰多子组合调仓计划。"""
        all_dates = calendar.get_dates_in_range(start, end)

        # 按月分组
        monthly_groups: dict[pd.Period, pd.DatetimeIndex] = {}
        for date in all_dates:
            period = date.to_period("M")
            if period not in monthly_groups:
                monthly_groups[period] = []
            monthly_groups[period].append(date)

        records: list[RebalanceRecord] = []
        for period in sorted(monthly_groups.keys()):
            month_dates = monthly_groups[period]
            # 子组合 k 使用月内第 (first_calc_offset + k) 个交易日作为 calc_date
            for k in range(self.n_sub):
                calc_idx = self.first_calc_offset + k
                if calc_idx >= len(month_dates):
                    # 当月交易日不足，跳过该子组合
                    continue
                calc_date = month_dates[calc_idx]
                try:
                    adjust_date = calendar.offset(calc_date, self.calc_to_adjust_lag)
                except ValueError:
                    # 调仓日超出日历范围，跳过
                    continue
                records.append(
                    RebalanceRecord(
                        calc_date=pd.Timestamp(calc_date),
                        adjust_date=pd.Timestamp(adjust_date),
                        sub_index=k,
                    )
                )
        return records

    @property
    def weights(self) -> list[float]:
        """返回各子组合的融合权重（归一化）。"""
        if self._blend_weights is None:
            w = [1.0 / self.n_sub] * self.n_sub
        else:
            total = sum(self._blend_weights)
            w = [x / total for x in self._blend_weights]
        return w
