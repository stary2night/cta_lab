from __future__ import annotations

import pandas as pd

from backtest import VectorizedBacktest
from strategies.implementations.multifactor_cta_backtest import MultiFactorCTAStrategy


def test_multifactor_short_filter_zeros_conflicting_direction() -> None:
    dates = pd.bdate_range("2024-01-01", periods=3)
    signal = pd.DataFrame(
        {"A": [1.0, -1.0, 0.5], "B": [-1.0, 1.0, -0.5]},
        index=dates,
    )
    short_filter = pd.DataFrame(
        {"A": [-1.0, -1.0, 1.0], "B": [1.0, 1.0, -1.0]},
        index=dates,
    )

    filtered = MultiFactorCTAStrategy.apply_short_filter(signal, short_filter)

    assert filtered.loc[dates[0], "A"] == 0.0
    assert filtered.loc[dates[0], "B"] == 0.0
    assert filtered.loc[dates[1], "A"] == -1.0
    assert filtered.loc[dates[1], "B"] == 1.0
    assert filtered.loc[dates[2], "A"] == 0.5
    assert filtered.loc[dates[2], "B"] == -0.5


def test_multifactor_strategy_runs_on_returns_matrix() -> None:
    dates = pd.bdate_range("2020-01-01", periods=180)
    returns = pd.DataFrame(
        {
            "A": [0.002] * 90 + [-0.001] * 90,
            "B": [-0.001] * 90 + [0.002] * 90,
            "C": [0.001, -0.001] * 90,
            "D": [-0.001, 0.001] * 90,
        },
        index=dates,
    )
    strategy = MultiFactorCTAStrategy(
        config={
            "trend_window": 40,
            "trend_short_mean_window": 20,
            "trend_vol_window": 5,
            "trend_breakout_windows": [5, 10, 20],
            "trend_residual_windows": [20, 10],
            "cross_lookback": 40,
            "cross_short_mean_window": 20,
            "cross_vol_window": 5,
            "short_windows": [5, 10],
            "smoothing_window": 5,
            "min_obs": 30,
            "sector_map": {"A": "S1", "B": "S1", "C": "S2", "D": "S2"},
        }
    )

    result = strategy.run_vectorized(
        returns,
        backtest=VectorizedBacktest(lag=1, vol_target=None, trim_inactive=False),
    )

    assert result.positions_df is not None
    assert set(result.positions_df.columns) == {"A", "B", "C", "D"}
    assert result.turnover_series is not None
    assert len(result.returns) > 0


def test_multifactor_cross_sleeve_is_dollar_neutral() -> None:
    dates = pd.bdate_range("2020-01-01", periods=120)
    returns = pd.DataFrame(
        {
            "A": [0.002] * 120,
            "B": [-0.001] * 120,
            "C": [0.0015] * 120,
            "D": [-0.0008] * 120,
        },
        index=dates,
    )
    strategy = MultiFactorCTAStrategy(
        config={
            "cross_lookback": 40,
            "cross_short_mean_window": 20,
            "cross_vol_window": 5,
            "top_pct": 0.20,
            "bottom_pct": 0.20,
            "sector_map": {"A": "S1", "B": "S1", "C": "S2", "D": "S2"},
        }
    )

    positions = strategy.build_cross_positions(returns)
    active = positions.abs().sum(axis=1) > 0

    assert active.any()
    assert positions.loc[active].sum(axis=1).abs().max() < 1e-12
    assert positions.loc[active].abs().sum(axis=1).max() <= 1.0


def test_multifactor_cross_sleeve_supports_sector_inverse_vol_branch() -> None:
    dates = pd.bdate_range("2020-01-01", periods=160)
    returns = pd.DataFrame(
        {
            "A": [0.002, -0.001, 0.003, 0.001] * 40,
            "B": [-0.001, 0.0005, -0.002, -0.001] * 40,
            "C": [0.0015, -0.0005, 0.002, 0.0008] * 40,
            "D": [-0.0008, 0.0003, -0.0015, -0.0006] * 40,
        },
        index=dates,
    )
    strategy = MultiFactorCTAStrategy(
        config={
            "cross_lookback": 40,
            "cross_short_mean_window": 20,
            "cross_vol_window": 5,
            "cross_weighting": "sector_inverse_vol",
            "cross_sector_vol_halflife": 10,
            "top_pct": 0.20,
            "bottom_pct": 0.20,
            "sector_map": {"A": "S1", "B": "S1", "C": "S2", "D": "S2"},
        }
    )

    positions = strategy.build_cross_positions(returns)
    active = positions.abs().sum(axis=1) > 0

    assert active.any()
    assert positions.loc[active].abs().sum(axis=1).max() <= 1.0 + 1e-12
