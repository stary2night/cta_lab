"""analysis.signal 测试：future return labels 与 IC/IR 评估。"""

import numpy as np
import pandas as pd
import warnings

from analysis.signal import (
    build_forward_returns,
    evaluate_signal,
    forward_log_return,
    forward_return,
    information_coefficient,
    information_ratio,
)


DATES = pd.date_range("2024-01-02", periods=5, freq="B")


class TestLabels:
    def test_forward_return_series(self):
        prices = pd.Series([100.0, 110.0, 121.0], index=DATES[:3])
        out = forward_return(prices, horizon=1)
        assert np.isclose(out.iloc[0], 0.10)
        assert np.isclose(out.iloc[1], 0.10)
        assert np.isnan(out.iloc[2])

    def test_forward_log_return_series(self):
        prices = pd.Series([100.0, 110.0], index=DATES[:2])
        out = forward_log_return(prices, horizon=1)
        assert np.isclose(out.iloc[0], np.log(1.1))

    def test_build_forward_returns_dict(self):
        prices = pd.DataFrame({"A": [100.0, 101.0, 102.0]}, index=DATES[:3])
        out = build_forward_returns(prices, horizons=[1, 2])
        assert set(out.keys()) == {1, 2}


class TestICEvaluation:
    def test_information_coefficient_perfect_positive(self):
        signal_df = pd.DataFrame(
            {"A": [1.0, 1.0], "B": [2.0, 2.0], "C": [3.0, 3.0]},
            index=DATES[:2],
        )
        future_df = pd.DataFrame(
            {"A": [0.1, 0.1], "B": [0.2, 0.2], "C": [0.3, 0.3]},
            index=DATES[:2],
        )
        out = information_coefficient(signal_df, future_df)
        assert np.allclose(out.dropna().values, 1.0)

    def test_information_ratio_positive(self):
        ic = pd.Series([0.1, 0.2, 0.3], index=DATES[:3])
        assert information_ratio(ic) > 0

    def test_information_coefficient_constant_cross_section_returns_nan_without_warning(self):
        signal_df = pd.DataFrame(
            {"A": [1.0], "B": [1.0], "C": [1.0]},
            index=[DATES[0]],
        )
        future_df = pd.DataFrame(
            {"A": [0.1], "B": [0.2], "C": [0.3]},
            index=[DATES[0]],
        )

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("error", RuntimeWarning)
            out = information_coefficient(signal_df, future_df)

        assert len(caught) == 0
        assert np.isnan(out.iloc[0])

    def test_evaluate_signal_summary_contains_horizons(self):
        signal_df = pd.DataFrame(
            {
                "A": [1.0, 2.0, 3.0, 4.0],
                "B": [4.0, 3.0, 2.0, 1.0],
                "C": [2.0, 2.0, 2.0, 2.0],
            },
            index=DATES[:4],
        )
        prices = pd.DataFrame(
            {
                "A": [100.0, 102.0, 104.0, 106.0],
                "B": [100.0, 99.0, 98.0, 97.0],
                "C": [100.0, 100.0, 100.0, 100.0],
            },
            index=DATES[:4],
        )

        report = evaluate_signal(signal_df, prices, horizons=[1, 2])
        assert list(report.summary.index) == [1, 2]
        assert "ic_mean" in report.summary.columns
        assert "rank_ic_ir" in report.summary.columns

    def test_evaluate_signal_accepts_prebuilt_future_returns(self):
        signal_df = pd.DataFrame(
            {"A": [1.0, 2.0, 3.0], "B": [3.0, 2.0, 1.0]},
            index=DATES[:3],
        )
        prices = pd.DataFrame(
            {"A": [100.0, 101.0, 102.0], "B": [100.0, 99.0, 98.0]},
            index=DATES[:3],
        )
        fwd = build_forward_returns(prices, horizons=[1, 2])

        report = evaluate_signal(signal_df, future_returns=fwd, horizons=[1, 2])
        assert list(report.summary.index) == [1, 2]
