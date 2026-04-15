"""tests/test_data_module/test_bar.py — Bar & BarSeries 测试"""

import numpy as np
import pandas as pd
import pytest

from data.model.bar import Bar, BarSeries


# ── 公共 fixture ──────────────────────────────────────────────────────────

@pytest.fixture
def dates_20() -> pd.DatetimeIndex:
    return pd.date_range("2024-01-02", periods=20, freq="B")


@pytest.fixture
def monotone_bs(dates_20) -> BarSeries:
    """单调递增的结算价序列，便于验证回撤、收益符号。"""
    settle = np.linspace(100.0, 119.0, 20)
    df = pd.DataFrame({
        "open": settle - 0.5, "high": settle + 1.0,
        "low": settle - 1.0, "close": settle,
        "settle": settle, "volume": 1000.0, "open_interest": 5000.0,
    }, index=dates_20)
    return BarSeries("RB", df)


@pytest.fixture
def random_bs(dates_20) -> BarSeries:
    """随机价格序列（固定 seed）。"""
    rng = np.random.default_rng(42)
    settle = np.cumprod(1 + rng.standard_normal(20) * 0.01) * 100
    df = pd.DataFrame({
        "open": settle, "high": settle * 1.01, "low": settle * 0.99,
        "close": settle, "settle": settle,
        "volume": 1000.0, "open_interest": 5000.0,
    }, index=dates_20)
    return BarSeries("HC", df)


# ── 构造验证 ──────────────────────────────────────────────────────────────

class TestBarSeriesConstruction:
    def test_valid_construction(self, monotone_bs):
        assert monotone_bs.symbol == "RB"
        assert len(monotone_bs) == 20

    def test_missing_column_raises(self, dates_20):
        df = pd.DataFrame({"open": 1.0, "settle": 100.0}, index=dates_20)
        with pytest.raises(ValueError, match="missing columns"):
            BarSeries("RB", df)

    def test_non_datetime_index_raises(self):
        idx = pd.RangeIndex(10)
        df = pd.DataFrame({
            "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
            "settle": 1.0, "volume": 1.0, "open_interest": 1.0,
        }, index=idx)
        with pytest.raises(TypeError, match="DatetimeIndex"):
            BarSeries("RB", df)

    def test_data_sorted_on_construction(self, dates_20):
        """乱序输入应在构造后自动排序。"""
        shuffled = dates_20[[3, 1, 0, 2, 4] + list(range(5, 20))]
        df = pd.DataFrame({
            "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
            "settle": 100.0, "volume": 1.0, "open_interest": 1.0,
        }, index=shuffled)
        bs = BarSeries("RB", df)
        assert list(bs.data.index) == sorted(bs.data.index)


# ── log_returns ────────────────────────────────────────────────────────────

class TestLogReturns:
    def test_first_value_is_nan(self, monotone_bs):
        lr = monotone_bs.log_returns()
        assert np.isnan(lr.iloc[0])

    def test_length(self, monotone_bs):
        assert len(monotone_bs.log_returns()) == 20

    def test_positive_for_monotone_increasing(self, monotone_bs):
        lr = monotone_bs.log_returns().dropna()
        assert (lr > 0).all()

    def test_approximately_equal_to_pct_for_small_changes(self, random_bs):
        lr = random_bs.log_returns().dropna()
        pr = random_bs.pct_returns().dropna()
        # log(1+r) ≈ r for small r
        np.testing.assert_allclose(lr.values, np.log(1 + pr.values), rtol=1e-9)


# ── pct_returns ────────────────────────────────────────────────────────────

class TestPctReturns:
    def test_first_value_is_nan(self, monotone_bs):
        assert np.isnan(monotone_bs.pct_returns().iloc[0])

    def test_values_positive_for_monotone(self, monotone_bs):
        assert (monotone_bs.pct_returns().dropna() > 0).all()


# ── ewm_vol ───────────────────────────────────────────────────────────────

class TestEwmVol:
    def test_no_nan_after_second(self, random_bs):
        """ewm_vol：第1日收益为 NaN（无前一日价格），第2日起 min_periods=1 生效，应无 NaN。"""
        vol = random_bs.ewm_vol(60)
        # iloc[0] = NaN（log_returns 第一行为 NaN，ewm 从第2个样本起有值）
        # iloc[2:] 肯定都有效
        assert vol.iloc[2:].notna().all()

    def test_annualized(self, random_bs):
        """年化波动率应显著大于日波动率（×√252 ≈ 15.9x）。"""
        vol_ann = random_bs.ewm_vol(60).dropna().iloc[-1]
        lr = random_bs.log_returns().dropna()
        vol_daily = float(lr.ewm(halflife=60, min_periods=1).std().iloc[-1])
        assert abs(vol_ann - vol_daily * np.sqrt(252)) < 1e-9

    def test_positive(self, random_bs):
        assert (random_bs.ewm_vol(60).dropna() > 0).all()


# ── rolling_vol ───────────────────────────────────────────────────────────

class TestRollingVol:
    def test_first_window_minus_one_are_nan(self, random_bs):
        vol = random_bs.rolling_vol(5)
        assert vol.iloc[:5].isna().all()

    def test_after_window_not_nan(self, random_bs):
        vol = random_bs.rolling_vol(5)
        assert vol.iloc[5:].notna().all()

    def test_annualized(self, random_bs):
        vol_ann = random_bs.rolling_vol(10).dropna().iloc[-1]
        lr = random_bs.log_returns()
        vol_daily = float(lr.rolling(10).std().dropna().iloc[-1])
        assert abs(vol_ann - vol_daily * np.sqrt(252)) < 1e-9


# ── drawdown ──────────────────────────────────────────────────────────────

class TestDrawdown:
    def test_range(self, random_bs):
        dd = random_bs.drawdown()
        assert (dd <= 0).all()
        assert (dd >= -1).all()

    def test_zero_at_new_high(self, monotone_bs):
        """单调递增序列每日都是新高，回撤应全为 0。"""
        dd = monotone_bs.drawdown()
        np.testing.assert_allclose(dd.values, 0.0, atol=1e-12)

    def test_nonzero_after_decline(self):
        """价格先涨后跌，回撤应在跌幅期间为负。"""
        idx = pd.date_range("2024-01-02", periods=5, freq="B")
        settle = np.array([100.0, 110.0, 105.0, 103.0, 108.0])
        df = pd.DataFrame({
            "open": settle, "high": settle, "low": settle,
            "close": settle, "settle": settle,
            "volume": 1.0, "open_interest": 1.0,
        }, index=idx)
        bs = BarSeries("X", df)
        dd = bs.drawdown()
        # 第 3 日 (105) 相对高点 (110) 的回撤 ≈ -0.0455
        assert dd.iloc[2] < 0
        assert abs(dd.iloc[2] - (105 / 110 - 1)) < 1e-9


# ── __getitem__ (切片) ────────────────────────────────────────────────────

class TestGetItem:
    def test_slice_returns_bar_series(self, monotone_bs):
        sub = monotone_bs["2024-01-02":"2024-01-12"]
        assert isinstance(sub, BarSeries)

    def test_slice_correct_length(self, monotone_bs):
        sub = monotone_bs["2024-01-02":"2024-01-12"]
        assert len(sub) > 0
        assert len(sub) < len(monotone_bs)

    def test_symbol_preserved(self, monotone_bs):
        sub = monotone_bs["2024-01-02":"2024-01-05"]
        assert sub.symbol == "RB"

    def test_single_date_slice(self, monotone_bs):
        """单日切片应返回 1 行的 BarSeries。"""
        sub = monotone_bs["2024-01-02":"2024-01-02"]
        assert len(sub) == 1
