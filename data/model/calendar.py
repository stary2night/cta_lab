"""交易日历：单交易所日历与多交易所合并日历。"""

from __future__ import annotations

import pandas as pd


class TradingCalendar:
    """单交易所交易日历，提供日期判断与偏移操作。"""

    def __init__(self, exchange: str, trading_dates: pd.DatetimeIndex) -> None:
        self.exchange = exchange
        self._dates = pd.DatetimeIndex(sorted(trading_dates.normalize().unique()))
        self._date_set: set[pd.Timestamp] = set(self._dates)

    # ------------------------------------------------------------------
    # 基础查询
    # ------------------------------------------------------------------

    def is_trading_day(self, date: pd.Timestamp | str) -> bool:
        """判断给定日期是否为交易日。"""
        return pd.Timestamp(date).normalize() in self._date_set

    def offset(self, date: pd.Timestamp | str, n: int) -> pd.Timestamp:
        """从 date 向前（n>0）或向后（n<0）偏移 n 个交易日。"""
        ts = pd.Timestamp(date).normalize()
        pos = self._dates.searchsorted(ts)
        # 若 date 本身不在日历中，searchsorted 找到的是下一个位置
        if pos < len(self._dates) and self._dates[pos] == ts:
            target_pos = pos + n
        else:
            # date 不是交易日：向后偏移从下一个交易日算起，向前偏移从前一个算起
            if n >= 0:
                target_pos = pos + n
            else:
                target_pos = pos + n
        if target_pos < 0 or target_pos >= len(self._dates):
            raise ValueError(
                f"Offset {n} from {date} goes out of calendar range."
            )
        return self._dates[target_pos]

    def trading_days_between(self, start: pd.Timestamp | str, end: pd.Timestamp | str) -> int:
        """计算 start 到 end 之间的交易日数（含两端点）。"""
        s = pd.Timestamp(start).normalize()
        e = pd.Timestamp(end).normalize()
        mask = (self._dates >= s) & (self._dates <= e)
        return int(mask.sum())

    def next_trading_day(self, date: pd.Timestamp | str) -> pd.Timestamp:
        """返回 date 之后的第一个交易日（不含 date 本身）。"""
        ts = pd.Timestamp(date).normalize()
        pos = self._dates.searchsorted(ts, side="right")
        if pos >= len(self._dates):
            raise ValueError(f"No trading day after {date} in calendar.")
        return self._dates[pos]

    def prev_trading_day(self, date: pd.Timestamp | str) -> pd.Timestamp:
        """返回 date 之前的最后一个交易日（不含 date 本身）。"""
        ts = pd.Timestamp(date).normalize()
        pos = self._dates.searchsorted(ts, side="left")
        if pos == 0:
            raise ValueError(f"No trading day before {date} in calendar.")
        return self._dates[pos - 1]

    def get_month_end_dates(
        self, start: pd.Timestamp | str, end: pd.Timestamp | str
    ) -> pd.DatetimeIndex:
        """返回 [start, end] 范围内每月的最后一个交易日。"""
        dates_in_range = self.get_dates_in_range(start, end)
        return dates_in_range[dates_in_range.is_month_end] if len(dates_in_range) == 0 else (
            pd.DatetimeIndex(
                dates_in_range.to_series()
                .groupby(dates_in_range.to_period("M"))
                .last()
                .values
            )
        )

    def get_dates_in_range(
        self, start: pd.Timestamp | str, end: pd.Timestamp | str
    ) -> pd.DatetimeIndex:
        """返回 [start, end] 范围内的所有交易日。"""
        s = pd.Timestamp(start).normalize()
        e = pd.Timestamp(end).normalize()
        return self._dates[(self._dates >= s) & (self._dates <= e)]


class MultiExchangeCalendar:
    """多交易所合并日历，取各交易所交易日并集。"""

    def __init__(self, calendars: list[TradingCalendar]) -> None:
        if not calendars:
            raise ValueError("At least one TradingCalendar is required.")
        self._calendars = calendars
        # 合并并去重排序
        all_dates = pd.DatetimeIndex(
            sorted(set().union(*[set(cal._dates) for cal in calendars]))
        )
        self._dates = all_dates
        self._date_set: set[pd.Timestamp] = set(all_dates)

    def is_trading_day(self, date: pd.Timestamp | str) -> bool:
        """任意交易所有交易日即返回 True。"""
        return pd.Timestamp(date).normalize() in self._date_set

    def get_dates_in_range(
        self, start: pd.Timestamp | str, end: pd.Timestamp | str
    ) -> pd.DatetimeIndex:
        """返回 [start, end] 范围内的合并交易日序列。"""
        s = pd.Timestamp(start).normalize()
        e = pd.Timestamp(end).normalize()
        return self._dates[(self._dates >= s) & (self._dates <= e)]
