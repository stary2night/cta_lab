from __future__ import annotations

import numpy as np
import pandas as pd

from signals.momentum.multifactor_crossmom import MultiFactorCrossSectionalMomentumSignal


def test_multifactor_crossmom_signal_ranks_within_sector() -> None:
    dates = pd.bdate_range("2024-01-01", periods=100)
    trend = np.arange(100)
    returns = pd.DataFrame(
        {
            "A": 0.0005 + 0.000020 * trend,
            "B": -0.0005 - 0.000010 * trend,
            "C": 0.0004 + 0.000015 * trend,
            "D": -0.0004 - 0.000008 * trend,
        },
        index=dates,
    )
    signal = MultiFactorCrossSectionalMomentumSignal(
        sector_map={"A": "S1", "B": "S1", "C": "S2", "D": "S2"},
        lookback=20,
        short_mean_window=10,
        vol_window=5,
        top_pct=0.20,
        bottom_pct=0.20,
    ).compute(returns)

    assert signal.shape == returns.shape
    assert signal.max().max() <= 1.0
    assert signal.min().min() >= -1.0
    assert signal["A"].iloc[-1] > signal["B"].iloc[-1]
    assert signal["C"].iloc[-1] > signal["D"].iloc[-1]
    assert signal["A"].iloc[-1] > 0
    assert signal["B"].iloc[-1] < 0


def test_multifactor_crossmom_signal_stays_neutral_during_warmup() -> None:
    dates = pd.bdate_range("2024-01-01", periods=30)
    returns = pd.DataFrame(
        {
            "A": [0.0020] * 30,
            "B": [-0.0010] * 30,
            "C": [0.0015] * 30,
            "D": [-0.0008] * 30,
        },
        index=dates,
    )
    signal = MultiFactorCrossSectionalMomentumSignal(
        sector_map={"A": "S1", "B": "S1", "C": "S2", "D": "S2"},
        lookback=60,
        min_periods=40,
    ).compute(returns)

    assert signal.abs().sum().sum() == 0.0


def test_multifactor_crossmom_signal_keeps_single_asset_sector_neutral() -> None:
    dates = pd.bdate_range("2024-01-01", periods=80)
    returns = pd.DataFrame(
        {
            "A": [0.0020] * 80,
            "B": [-0.0010] * 80,
            "SOLO": [0.0030] * 80,
        },
        index=dates,
    )
    signal = MultiFactorCrossSectionalMomentumSignal(
        sector_map={"A": "S1", "B": "S1", "SOLO": "S2"},
        lookback=20,
        short_mean_window=10,
        vol_window=5,
    ).compute(returns)

    assert signal["SOLO"].iloc[-1] == 0.0
