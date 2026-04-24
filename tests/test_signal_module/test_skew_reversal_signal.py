from __future__ import annotations

import pandas as pd

from signals.reversal import SkewReversalSignal


def test_skew_reversal_signal_distinguishes_positive_and_negative_skew() -> None:
    dates = pd.bdate_range("2024-01-01", periods=140)
    positive = pd.Series([0.0] * 139 + [0.10], index=dates)
    negative = pd.Series([0.0] * 139 + [-0.10], index=dates)

    signal = SkewReversalSignal(windows=(130,))

    pos_value = signal.compute(positive).iloc[-1]
    neg_value = signal.compute(negative).iloc[-1]

    assert pos_value > 0.0
    assert neg_value < 0.0
