"""signals operators 测试：时序处理、截面标准化与强度映射。"""

import numpy as np
import pandas as pd

from signals import (
    clip,
    cross_sectional_rank,
    lag,
    normalize_by_abs_sum,
    rolling_zscore,
    smooth,
    winsorize,
    zscore,
)


DATES = pd.date_range("2024-01-02", periods=6, freq="B")


class TestTimeSeriesOperators:
    def test_lag_shifts_series(self):
        s = pd.Series([1.0, 2.0, 3.0], index=DATES[:3])
        out = lag(s, periods=1)
        assert out.iloc[0] != out.iloc[0]
        assert out.iloc[1] == 1.0

    def test_smooth_mean(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0], index=DATES[:4])
        out = smooth(s, window=2, method="mean")
        assert np.isclose(out.iloc[-1], 3.5)

    def test_rolling_zscore_centered(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0], index=DATES[:5])
        out = rolling_zscore(s, window=3)
        assert np.isclose(out.iloc[-1], 1.22474487139)


class TestCrossSectionalOperators:
    def test_cross_sectional_rank_outputs_pct(self):
        df = pd.DataFrame(
            {"A": [1.0], "B": [3.0], "C": [2.0]},
            index=[DATES[0]],
        )
        out = cross_sectional_rank(df)
        assert np.isclose(out.loc[DATES[0], "A"], 1 / 3)
        assert np.isclose(out.loc[DATES[0], "B"], 1.0)

    def test_normalize_by_abs_sum(self):
        df = pd.DataFrame(
            {"A": [1.0], "B": [-2.0], "C": [1.0]},
            index=[DATES[0]],
        )
        out = normalize_by_abs_sum(df)
        assert np.isclose(out.abs().sum(axis=1).iloc[0], 1.0)


class TestValueOperators:
    def test_clip_limits_values(self):
        s = pd.Series([-10.0, 0.0, 10.0], index=DATES[:3])
        out = clip(s, lower=-2.0, upper=2.0)
        assert out.tolist() == [-2.0, 0.0, 2.0]

    def test_winsorize_series(self):
        s = pd.Series([1.0, 2.0, 3.0, 100.0], index=DATES[:4])
        out = winsorize(s, lower_q=0.0, upper_q=0.75)
        assert out.iloc[-1] <= 27.25

    def test_zscore_dataframe_axis1(self):
        df = pd.DataFrame(
            {"A": [1.0, 10.0], "B": [2.0, 10.0], "C": [3.0, 10.0]},
            index=DATES[:2],
        )
        out = zscore(df, axis=1)
        assert np.isclose(out.iloc[0].mean(), 0.0)
        assert out.iloc[1].isna().all()
