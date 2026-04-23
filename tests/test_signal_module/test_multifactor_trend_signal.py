from __future__ import annotations

import pandas as pd

from signals.momentum.multifactor_trend import MultiFactorTrendSignal


def test_multifactor_trend_signal_returns_bounded_matrix() -> None:
    dates = pd.bdate_range("2024-01-01", periods=80)
    returns = pd.DataFrame(
        {
            "UP": [0.002] * 80,
            "DOWN": [-0.001] * 80,
            "CHOP": [0.001, -0.001] * 40,
        },
        index=dates,
    )

    signal = MultiFactorTrendSignal(
        trend_window=20,
        short_mean_window=10,
        vol_window=5,
        breakout_windows=(5, 10, 20),
        residual_windows=(20, 10),
    ).compute_from_returns(returns)

    assert signal.shape == returns.shape
    assert signal.max().max() <= 1.0
    assert signal.min().min() >= -1.0
    assert signal["UP"].iloc[-1] > signal["DOWN"].iloc[-1]
