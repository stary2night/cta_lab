"""portfolio 层测试：signal -> portfolio 桥接、定仓语义与融合边界。"""

import numpy as np
import pandas as pd

from portfolio import (
    EqualRiskSizer,
    RiskBudgetSizer,
    ThresholdSelector,
    TopBottomSelector,
    blend,
)


DATES = pd.date_range("2024-01-02", periods=3, freq="B")


class TestSelectors:
    def test_top_bottom_selector_outputs_long_short_mask(self):
        scores = pd.DataFrame(
            {
                "A": [0.9, 0.1],
                "B": [0.7, 0.8],
                "C": [0.2, 0.6],
                "D": [0.1, 0.4],
            },
            index=pd.date_range("2024-01-02", periods=2, freq="B"),
        )
        out = TopBottomSelector(top_n=1, bottom_n=1).apply(scores)

        expected = pd.DataFrame(
            {
                "A": [1.0, -1.0],
                "B": [0.0, 1.0],
                "C": [0.0, 0.0],
                "D": [-1.0, 0.0],
            },
            index=scores.index,
        )
        pd.testing.assert_frame_equal(out, expected)

    def test_threshold_selector_respects_cutoffs(self):
        scores = pd.DataFrame(
            {
                "A": [0.85],
                "B": [0.50],
                "C": [0.10],
            },
            index=[DATES[0]],
        )
        out = ThresholdSelector(long_threshold=0.8, short_threshold=0.2).apply(scores)

        expected = pd.DataFrame(
            {"A": [1.0], "B": [0.0], "C": [-1.0]},
            index=scores.index,
        )
        pd.testing.assert_frame_equal(out, expected)


class TestSizers:
    def test_equal_risk_raw_mode_preserves_strength_ratio(self):
        signal_df = pd.DataFrame(
            {"A": [0.5], "B": [1.0]},
            index=[DATES[0]],
        )
        vol_df = pd.DataFrame(
            {"A": [0.2], "B": [0.2]},
            index=signal_df.index,
        )

        out = EqualRiskSizer(target_vol=0.4, signal_mode="raw").compute(signal_df, vol_df)
        assert np.isclose(out.loc[DATES[0], "B"] / out.loc[DATES[0], "A"], 2.0)

    def test_risk_budget_raw_mode_preserves_long_short_strength(self):
        signal_df = pd.DataFrame(
            {"A": [1.0], "B": [-0.5]},
            index=[DATES[0]],
        )
        vol_df = pd.DataFrame(
            {"A": [0.2], "B": [0.2]},
            index=signal_df.index,
        )

        out = RiskBudgetSizer(
            base_risk=0.1,
            rev_weight=0.5,
            signal_mode="raw",
        ).compute(signal_df, vol_df)

        assert np.isclose(out.loc[DATES[0], "A"], 0.25)
        assert np.isclose(out.loc[DATES[0], "B"], -0.125)


class TestBlender:
    def test_blend_does_not_extend_subportfolio_past_last_date(self):
        sub_weights = {
            0: pd.DataFrame({"A": [1.0, 1.0]}, index=DATES[:2]),
            1: pd.DataFrame({"A": [3.0, 3.0]}, index=DATES[1:]),
        }

        out = blend(sub_weights, weights=[0.5, 0.5])

        assert np.isclose(out.loc[DATES[0], "A"], 0.5)
        assert np.isclose(out.loc[DATES[1], "A"], 2.0)
        assert np.isclose(out.loc[DATES[2], "A"], 1.5)
