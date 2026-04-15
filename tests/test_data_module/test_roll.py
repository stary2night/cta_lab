"""tests/test_data_module/test_roll.py — RollRule 及 ContractSchedule 测试"""

import numpy as np
import pandas as pd
import pytest
from datetime import date

from data.model.bar import BarSeries
from data.model.contract import Contract
from data.model.roll import (
    CalendarRoll, ContractSchedule, OIMaxRoll,
    RollEvent, VolumeMaxRoll,
)


# ── fixtures ──────────────────────────────────────────────────────────────

DATES = pd.date_range("2024-08-01", periods=10, freq="B")
REF = DATES[3]  # 2024-08-06


def make_bs(code: str, oi: float, volume: float) -> BarSeries:
    n = len(DATES)
    df = pd.DataFrame({
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
        "settle": 100.0,
        "volume": volume,
        "open_interest": oi,
    }, index=DATES)
    return BarSeries(code, df)


@pytest.fixture
def two_contracts():
    c1 = Contract("RB", "RB2409", "SHFE", date(2024, 1, 1), date(2024, 9, 15), date(2024, 9, 14))
    c2 = Contract("RB", "RB2410", "SHFE", date(2024, 1, 1), date(2024, 10, 15), date(2024, 10, 14))
    return c1, c2


# ── OIMaxRoll ─────────────────────────────────────────────────────────────

class TestOIMaxRoll:
    def test_selects_highest_oi(self, two_contracts):
        c1, c2 = two_contracts
        bar_data = {
            "RB2409": make_bs("RB2409", oi=8000, volume=500),
            "RB2410": make_bs("RB2410", oi=3000, volume=700),
        }
        chosen = OIMaxRoll().select_contract(REF, [c1, c2], bar_data)
        assert chosen.code == "RB2409"

    def test_selects_second_when_first_has_lower_oi(self, two_contracts):
        c1, c2 = two_contracts
        bar_data = {
            "RB2409": make_bs("RB2409", oi=1000, volume=500),
            "RB2410": make_bs("RB2410", oi=9000, volume=200),
        }
        chosen = OIMaxRoll().select_contract(REF, [c1, c2], bar_data)
        assert chosen.code == "RB2410"

    def test_no_data_raises(self, two_contracts):
        c1, c2 = two_contracts
        with pytest.raises(ValueError, match="no valid contract"):
            OIMaxRoll().select_contract(REF, [c1, c2], {})

    def test_date_not_in_data_skipped(self, two_contracts):
        """只有 c2 有该日数据时，应选 c2。"""
        c1, c2 = two_contracts
        bar_data = {
            "RB2410": make_bs("RB2410", oi=5000, volume=500),
        }
        chosen = OIMaxRoll().select_contract(REF, [c1, c2], bar_data)
        assert chosen.code == "RB2410"


# ── VolumeMaxRoll ─────────────────────────────────────────────────────────

class TestVolumeMaxRoll:
    def test_selects_highest_volume(self, two_contracts):
        c1, c2 = two_contracts
        bar_data = {
            "RB2409": make_bs("RB2409", oi=8000, volume=200),
            "RB2410": make_bs("RB2410", oi=3000, volume=1500),
        }
        chosen = VolumeMaxRoll().select_contract(REF, [c1, c2], bar_data)
        assert chosen.code == "RB2410"

    def test_no_data_raises(self, two_contracts):
        c1, c2 = two_contracts
        with pytest.raises(ValueError, match="no valid contract"):
            VolumeMaxRoll().select_contract(REF, [c1, c2], {})


# ── CalendarRoll ──────────────────────────────────────────────────────────

class TestCalendarRoll:
    def _make_contracts_and_data(self):
        # c1 到期 2024-08-09（DATES[5] = 2024-08-08 之后 1 天）
        # c2 到期 2024-10-15
        c1 = Contract("RB", "RB2408", "SHFE", date(2024, 1, 1), date(2024, 8, 15), date(2024, 8, 9))
        c2 = Contract("RB", "RB2410", "SHFE", date(2024, 1, 1), date(2024, 10, 15), date(2024, 10, 14))
        bar_data = {
            "RB2408": make_bs("RB2408", oi=5000, volume=500),
            "RB2410": make_bs("RB2410", oi=3000, volume=300),
        }
        return c1, c2, bar_data

    def test_holds_near_month_when_far_from_expiry(self):
        c1, c2, bar_data = self._make_contracts_and_data()
        rule = CalendarRoll(days_before_expiry=5)
        # DATES[0] = 2024-08-01，距 c1 last_trade_date(2024-08-09) = 8 天 >= 5，选 c1
        chosen = rule.select_contract(DATES[0], [c1, c2], bar_data)
        assert chosen.code == "RB2408"

    def test_switches_near_expiry(self):
        c1, c2, bar_data = self._make_contracts_and_data()
        rule = CalendarRoll(days_before_expiry=5)
        # DATES[6] = 2024-08-09，距 c1 last_trade_date = 0 天 < 5，应切到 c2
        chosen = rule.select_contract(DATES[6], [c1, c2], bar_data)
        assert chosen.code == "RB2410"

    def test_no_active_contract_raises(self):
        c1 = Contract("RB", "RB2408", "SHFE", date(2024, 1, 1), date(2024, 8, 5), date(2024, 8, 1))
        # 2024-08-06 时 c1 already expired
        with pytest.raises(ValueError, match="no active contract"):
            CalendarRoll(5).select_contract(DATES[3], [c1], {})


# ── ContractSchedule ──────────────────────────────────────────────────────

class TestContractSchedule:
    @pytest.fixture
    def schedule(self):
        events = [
            RollEvent(DATES[0], "", "RB2408"),
            RollEvent(DATES[5], "RB2408", "RB2410"),
        ]
        return ContractSchedule(events, "RB")

    def test_get_active_before_first_roll(self, schedule):
        assert schedule.get_active_contract(DATES[0]) == "RB2408"

    def test_get_active_after_first_roll(self, schedule):
        assert schedule.get_active_contract(DATES[3]) == "RB2408"

    def test_get_active_after_second_roll(self, schedule):
        assert schedule.get_active_contract(DATES[6]) == "RB2410"

    def test_get_active_before_any_roll_raises(self):
        events = [RollEvent(DATES[2], "", "RB2408")]
        sched = ContractSchedule(events, "RB")
        with pytest.raises(ValueError, match="no contract scheduled"):
            sched.get_active_contract(DATES[0])

    def test_to_series_returns_series(self, schedule):
        s = schedule.to_series()
        assert isinstance(s, pd.Series)
        assert len(s) == 2

    def test_to_series_empty_schedule(self):
        sched = ContractSchedule([], "RB")
        s = sched.to_series()
        assert isinstance(s, pd.Series)
        assert len(s) == 0

    def test_events_sorted_by_date(self):
        events = [
            RollEvent(DATES[5], "RB2408", "RB2410"),
            RollEvent(DATES[0], "", "RB2408"),
        ]
        sched = ContractSchedule(events, "RB")
        dates = [e.date for e in sched.events]
        assert dates == sorted(dates)
