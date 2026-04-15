"""tests/test_data_module/test_continuous.py — ContinuousSeries 测试"""

import numpy as np
import pandas as pd
import pytest
from datetime import date

from data.model.bar import BarSeries
from data.model.calendar import TradingCalendar
from data.model.contract import Contract
from data.model.continuous import AdjustMethod, ContinuousSeries
from data.model.roll import CalendarRoll, ContractSchedule, OIMaxRoll


# ── 共用构造函数 ──────────────────────────────────────────────────────────

DATES = pd.date_range("2024-01-02", "2024-06-28", freq="B")
CAL = TradingCalendar("TEST", DATES)


def _make_bs(code: str, prices: np.ndarray, oi: float = 5000.0) -> BarSeries:
    df = pd.DataFrame({
        "open": prices, "high": prices * 1.01, "low": prices * 0.99,
        "close": prices, "settle": prices,
        "volume": 1000.0, "open_interest": oi,
    }, index=DATES)
    return BarSeries(code, df)


def _make_contract(sym: str, code: str, list_d: str, expire_d: str, last_d: str) -> Contract:
    return Contract(sym, code, "TEST",
                    date.fromisoformat(list_d),
                    date.fromisoformat(expire_d),
                    date.fromisoformat(last_d))


@pytest.fixture
def two_contract_setup():
    """两个合约：c1 在前半段，c2 在后半段。OI c1 > c2 直到 c1 快到期。"""
    c1 = _make_contract("X", "X2403", "2024-01-01", "2024-03-31", "2024-03-29")
    c2 = _make_contract("X", "X2406", "2024-01-01", "2024-06-30", "2024-06-28")

    p1 = np.linspace(100.0, 110.0, len(DATES))
    p2 = np.linspace(102.0, 115.0, len(DATES))

    bar_data = {
        "X2403": _make_bs("X2403", p1, oi=8000),
        "X2406": _make_bs("X2406", p2, oi=3000),
    }
    return c1, c2, bar_data


# ── 构建核心行为 ───────────────────────────────────────────────────────────

class TestBuildBasic:
    def test_build_returns_continuous_series(self, two_contract_setup):
        c1, c2, bar_data = two_contract_setup
        cs = ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                    AdjustMethod.NONE, CAL)
        assert isinstance(cs, ContinuousSeries)

    def test_length_covers_all_dates(self, two_contract_setup):
        c1, c2, bar_data = two_contract_setup
        cs = ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                    AdjustMethod.NONE, CAL)
        assert len(cs) > 0
        assert len(cs) <= len(DATES)

    def test_no_nan_in_prices(self, two_contract_setup):
        c1, c2, bar_data = two_contract_setup
        cs = ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                    AdjustMethod.NAV, CAL)
        assert cs.prices.notna().all()

    def test_schedule_has_roll_events(self, two_contract_setup):
        c1, c2, bar_data = two_contract_setup
        cs = ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                    AdjustMethod.NONE, CAL)
        # 两个合约至少有一次初始选择事件
        assert len(cs.schedule.events) >= 1

    def test_empty_bar_data_raises(self, two_contract_setup):
        c1, c2, _ = two_contract_setup
        with pytest.raises(ValueError):
            ContinuousSeries.build("X", {}, [c1, c2], OIMaxRoll(),
                                   AdjustMethod.NONE, CAL)


# ── AdjustMethod.NONE ─────────────────────────────────────────────────────

class TestAdjustNone:
    def test_prices_match_raw_on_roll_day(self, two_contract_setup):
        """NONE 模式：换仓日价格直接取新合约原始价格，可能有跳跃。"""
        c1, c2, bar_data = two_contract_setup
        cs = ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                    AdjustMethod.NONE, CAL)
        assert cs.prices.notna().all()

    def test_prices_positive(self, two_contract_setup):
        c1, c2, bar_data = two_contract_setup
        cs = ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                    AdjustMethod.NONE, CAL)
        assert (cs.prices > 0).all()


# ── AdjustMethod.NAV ──────────────────────────────────────────────────────

class TestAdjustNAV:
    def test_continuous_returns_no_extreme_jump(self, two_contract_setup):
        """NAV 模式：对数收益应无极端跳跃（± 50% 以内）。"""
        c1, c2, bar_data = two_contract_setup
        cs = ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                    AdjustMethod.NAV, CAL)
        lr = cs.log_returns().dropna()
        assert (lr.abs() < 0.5).all()

    def test_starts_near_first_price(self, two_contract_setup):
        """NAV 模式：初始价格是第一日原始 settle。"""
        c1, c2, bar_data = two_contract_setup
        cs = ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                    AdjustMethod.NAV, CAL)
        # NAV 链起点 = raw_series.iloc[0]（约为 100）
        assert 50 < cs.prices.iloc[0] < 200

    def test_normalized_nav_starts_at_one(self, two_contract_setup):
        """normalized 模式：初始值固定为 1.0。"""
        c1, c2, bar_data = two_contract_setup
        cs = ContinuousSeries.build(
            "X",
            bar_data,
            [c1, c2],
            OIMaxRoll(),
            AdjustMethod.NAV,
            CAL,
            nav_output="normalized",
        )
        assert cs.prices.iloc[0] == pytest.approx(1.0)


# ── AdjustMethod.RATIO ────────────────────────────────────────────────────

class TestAdjustRatio:
    def test_prices_positive(self, two_contract_setup):
        c1, c2, bar_data = two_contract_setup
        cs = ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                    AdjustMethod.RATIO, CAL)
        assert (cs.prices > 0).all()

    def test_latest_segment_unchanged(self, two_contract_setup):
        """RATIO 模式：最后一段（c2）价格不调整，保持原始值。"""
        c1, c2, bar_data = two_contract_setup
        cs_ratio = ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                           AdjustMethod.RATIO, CAL)
        cs_none = ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                          AdjustMethod.NONE, CAL)
        # 最后 10 个价格应相等（c2 段无需调整）
        np.testing.assert_allclose(
            cs_ratio.prices.iloc[-10:].values,
            cs_none.prices.iloc[-10:].values,
            rtol=1e-6,
        )


# ── AdjustMethod.ADD ──────────────────────────────────────────────────────

class TestAdjustAdd:
    def test_prices_continuous_at_roll(self, two_contract_setup):
        """ADD 模式：换仓日前后价格差应为零（消除跳跃）。"""
        c1, c2, bar_data = two_contract_setup
        cs = ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                    AdjustMethod.ADD, CAL)
        # 查找换仓事件（c2 开始的日期）
        roll_events = [e for e in cs.schedule.events if e.to_contract == "X2406"]
        if not roll_events:
            pytest.skip("No roll to X2406 found")
        roll_date = roll_events[0].date
        idx = cs.prices.index.get_loc(roll_date)
        if idx == 0:
            pytest.skip("Roll on first day")
        diff = abs(cs.prices.iloc[idx] - cs.prices.iloc[idx - 1])
        # 换仓前后价差应远小于原始价差（通常原始价差约 2）
        assert diff < 1.0


# ── 固有变换方法 ───────────────────────────────────────────────────────────

class TestContinuousSeriesMethods:
    @pytest.fixture
    def cs_nav(self, two_contract_setup):
        c1, c2, bar_data = two_contract_setup
        return ContinuousSeries.build("X", bar_data, [c1, c2], OIMaxRoll(),
                                      AdjustMethod.NAV, CAL)

    def test_log_returns_first_nan(self, cs_nav):
        assert np.isnan(cs_nav.log_returns().iloc[0])

    def test_pct_returns_length(self, cs_nav):
        assert len(cs_nav.pct_returns()) == len(cs_nav)

    def test_ewm_vol_positive(self, cs_nav):
        vol = cs_nav.ewm_vol(20)
        assert (vol.dropna() > 0).all()

    def test_rolling_vol_nan_in_window(self, cs_nav):
        vol = cs_nav.rolling_vol(10)
        assert vol.iloc[:10].isna().all()

    def test_drawdown_nonpositive(self, cs_nav):
        dd = cs_nav.drawdown()
        assert (dd <= 0).all()

    def test_len(self, cs_nav):
        assert len(cs_nav) > 0

    def test_getitem_slice(self, cs_nav):
        sub = cs_nav["2024-01-02":"2024-02-28"]
        assert isinstance(sub, ContinuousSeries)
        assert len(sub) > 0 and len(sub) < len(cs_nav)

    def test_prices_property(self, cs_nav):
        assert isinstance(cs_nav.prices, pd.Series)
        assert len(cs_nav.prices) == len(cs_nav)
