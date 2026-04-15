"""tests/test_data_module/test_calendar.py — TradingCalendar & MultiExchangeCalendar 测试"""

import pandas as pd
import pytest

from data.model.calendar import MultiExchangeCalendar, TradingCalendar


# ── fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def cal_2024() -> TradingCalendar:
    """2024 年全年工作日日历（Mon–Fri）。"""
    dates = pd.date_range("2024-01-02", "2024-12-31", freq="B")
    return TradingCalendar("SHFE", dates)


@pytest.fixture
def cal_q1() -> TradingCalendar:
    dates = pd.date_range("2024-01-02", "2024-03-29", freq="B")
    return TradingCalendar("SHFE", dates)


# ── is_trading_day ────────────────────────────────────────────────────────

class TestIsTradingDay:
    def test_trading_day_true(self, cal_2024):
        assert cal_2024.is_trading_day("2024-01-02") is True

    def test_weekend_false(self, cal_2024):
        assert cal_2024.is_trading_day("2024-01-06") is False  # Saturday

    def test_timestamp_input(self, cal_2024):
        assert cal_2024.is_trading_day(pd.Timestamp("2024-01-02")) is True

    def test_out_of_range_false(self, cal_q1):
        assert cal_q1.is_trading_day("2024-04-01") is False


# ── offset ────────────────────────────────────────────────────────────────

class TestOffset:
    def test_offset_positive_1(self, cal_q1):
        result = cal_q1.offset("2024-01-02", 1)
        assert result == pd.Timestamp("2024-01-03")

    def test_offset_positive_5(self, cal_q1):
        result = cal_q1.offset("2024-01-02", 5)
        assert result == pd.Timestamp("2024-01-09")

    def test_offset_zero(self, cal_q1):
        result = cal_q1.offset("2024-01-02", 0)
        assert result == pd.Timestamp("2024-01-02")

    def test_offset_negative(self, cal_q1):
        result = cal_q1.offset("2024-01-05", -1)
        assert result == pd.Timestamp("2024-01-04")

    def test_offset_out_of_range_raises(self, cal_q1):
        with pytest.raises(ValueError):
            cal_q1.offset("2024-03-29", 1)  # 最后一天再往后


# ── next / prev trading day ───────────────────────────────────────────────

class TestNextPrevTradingDay:
    def test_next_trading_day(self, cal_q1):
        nxt = cal_q1.next_trading_day("2024-01-02")
        assert nxt == pd.Timestamp("2024-01-03")

    def test_next_skips_weekend(self, cal_q1):
        # 2024-01-05 是 Friday，下一个交易日应是 2024-01-08 (Monday)
        nxt = cal_q1.next_trading_day("2024-01-05")
        assert nxt == pd.Timestamp("2024-01-08")

    def test_next_at_end_raises(self, cal_q1):
        with pytest.raises(ValueError):
            cal_q1.next_trading_day("2024-03-29")

    def test_prev_trading_day(self, cal_q1):
        prv = cal_q1.prev_trading_day("2024-01-03")
        assert prv == pd.Timestamp("2024-01-02")

    def test_prev_skips_weekend(self, cal_q1):
        # 2024-01-08 是 Monday，前一个交易日应是 2024-01-05 (Friday)
        prv = cal_q1.prev_trading_day("2024-01-08")
        assert prv == pd.Timestamp("2024-01-05")

    def test_prev_at_start_raises(self, cal_q1):
        with pytest.raises(ValueError):
            cal_q1.prev_trading_day("2024-01-02")


# ── trading_days_between ──────────────────────────────────────────────────

class TestTradingDaysBetween:
    def test_same_day(self, cal_q1):
        assert cal_q1.trading_days_between("2024-01-02", "2024-01-02") == 1

    def test_one_week(self, cal_q1):
        # 2024-01-02 ~ 2024-01-08 含 2/3/4/5/8 共 5 天
        assert cal_q1.trading_days_between("2024-01-02", "2024-01-08") == 5

    def test_includes_endpoints(self, cal_q1):
        count = cal_q1.trading_days_between("2024-01-02", "2024-01-05")
        assert count == 4  # 2/3/4/5


# ── get_month_end_dates ───────────────────────────────────────────────────

class TestGetMonthEndDates:
    def test_three_months(self, cal_q1):
        mends = cal_q1.get_month_end_dates("2024-01-01", "2024-03-31")
        assert len(mends) == 3

    def test_each_month_one_date(self, cal_2024):
        mends = cal_2024.get_month_end_dates("2024-01-01", "2024-12-31")
        assert len(mends) == 12

    def test_month_end_is_last_trading_day(self, cal_q1):
        mends = cal_q1.get_month_end_dates("2024-01-01", "2024-01-31")
        jan_end = mends[0]
        # 下一个交易日应在 2 月
        nxt = cal_q1.next_trading_day(jan_end)
        assert nxt.month == 2

    def test_dates_sorted(self, cal_q1):
        mends = cal_q1.get_month_end_dates("2024-01-01", "2024-03-31")
        assert list(mends) == sorted(mends)


# ── get_dates_in_range ────────────────────────────────────────────────────

class TestGetDatesInRange:
    def test_range_count(self, cal_q1):
        dates = cal_q1.get_dates_in_range("2024-01-02", "2024-01-12")
        assert len(dates) == 9  # 2/3/4/5/8/9/10/11/12

    def test_inclusive_endpoints(self, cal_q1):
        dates = cal_q1.get_dates_in_range("2024-01-02", "2024-01-02")
        assert len(dates) == 1
        assert dates[0] == pd.Timestamp("2024-01-02")


# ── MultiExchangeCalendar ─────────────────────────────────────────────────

class TestMultiExchangeCalendar:
    def test_union_of_dates(self):
        d1 = pd.date_range("2024-01-02", periods=3, freq="B")  # Tue/Wed/Thu
        d2 = pd.date_range("2024-01-05", periods=3, freq="B")  # Fri/Mon/Tue
        cal1 = TradingCalendar("A", d1)
        cal2 = TradingCalendar("B", d2)
        mc = MultiExchangeCalendar([cal1, cal2])
        # 并集：2/3/4/5/8/9
        all_dates = mc.get_dates_in_range("2024-01-01", "2024-01-15")
        assert len(all_dates) == 6

    def test_is_trading_day_union(self):
        d1 = pd.DatetimeIndex(["2024-01-02"])
        d2 = pd.DatetimeIndex(["2024-01-03"])
        mc = MultiExchangeCalendar([
            TradingCalendar("A", d1),
            TradingCalendar("B", d2),
        ])
        assert mc.is_trading_day("2024-01-02") is True
        assert mc.is_trading_day("2024-01-03") is True
        assert mc.is_trading_day("2024-01-04") is False

    def test_empty_calendars_raises(self):
        with pytest.raises(ValueError):
            MultiExchangeCalendar([])

    def test_no_duplicates_in_union(self):
        dates = pd.date_range("2024-01-02", periods=5, freq="B")
        cal1 = TradingCalendar("A", dates)
        cal2 = TradingCalendar("B", dates)  # 完全相同
        mc = MultiExchangeCalendar([cal1, cal2])
        all_dates = mc.get_dates_in_range("2024-01-01", "2024-01-31")
        assert len(all_dates) == len(set(all_dates))
