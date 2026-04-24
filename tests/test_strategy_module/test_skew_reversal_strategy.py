from __future__ import annotations

import pandas as pd

from backtest import VectorizedBacktest
from strategies.implementations.skew_reversal_backtest import SkewReversalStrategy


def test_skew_reversal_oi_filter_blocks_non_declining_open_interest() -> None:
    dates = pd.bdate_range("2024-01-01", periods=1)
    skew_factor = pd.DataFrame(
        {"A": [-2.0], "B": [-1.0], "C": [1.0], "D": [2.0]},
        index=dates,
    )
    oi_change = pd.DataFrame(
        {"A": [-0.10], "B": [0.05], "C": [0.03], "D": [-0.20]},
        index=dates,
    )
    strategy = SkewReversalStrategy(
        config={"top_pct": 0.25, "bottom_pct": 0.25, "rebalance_buckets": 1}
    )

    positions = strategy.build_daily_positions(skew_factor, oi_change=oi_change)

    assert positions.loc[dates[0], "A"] == 0.5
    assert positions.loc[dates[0], "D"] == -0.5
    assert positions.loc[dates[0], "B"] == 0.0
    assert positions.loc[dates[0], "C"] == 0.0


def test_skew_reversal_staggered_rebalance_averages_tranches() -> None:
    dates = pd.bdate_range("2024-01-01", periods=4)
    daily_positions = pd.DataFrame({"A": [1.0, 0.0, 0.0, 0.0]}, index=dates)
    strategy = SkewReversalStrategy(config={"rebalance_buckets": 2})

    positions = strategy.apply_staggered_rebalance(daily_positions)

    assert positions.loc[dates[0], "A"] == 0.5
    assert positions.loc[dates[1], "A"] == 0.5
    assert positions.loc[dates[2], "A"] == 0.0


def test_skew_reversal_strategy_runs_on_returns_matrix() -> None:
    dates = pd.bdate_range("2020-01-01", periods=320)
    returns = pd.DataFrame(
        {
            "A": [0.0] * 319 + [-0.08],
            "B": [0.0] * 319 + [-0.04],
            "C": [0.0] * 319 + [0.04],
            "D": [0.0] * 319 + [0.08],
        },
        index=dates,
    )
    oi_change = pd.DataFrame(-0.05, index=dates, columns=returns.columns)
    strategy = SkewReversalStrategy(config={"rebalance_buckets": 5, "min_obs": 260})

    result = strategy.run_vectorized(
        returns,
        backtest=VectorizedBacktest(lag=1, vol_target=None, trim_inactive=False),
        close_returns_df=returns,
        oi_change_df=oi_change,
    )

    assert result.positions_df is not None
    assert set(result.positions_df.columns) == {"A", "B", "C", "D"}
    assert result.turnover_series is not None


def test_skew_reversal_tradable_mask_requires_listing_and_liquidity() -> None:
    dates = pd.bdate_range("2024-01-01", periods=6)
    settle_prices = pd.DataFrame(
        {
            "A": [100, 100, 100, 100, 100, 100],
            "B": [100, 100, 100, 100, 100, 100],
        },
        index=dates,
    )
    open_interest = pd.DataFrame(
        {
            "A": [20, 20, 20, 20, 20, 20],
            "B": [1, 1, 1, 1, 1, 1],
        },
        index=dates,
    )
    strategy = SkewReversalStrategy(
        config={
            "min_listing_days": 3,
            "liquidity_lookback": 2,
            "liquidity_threshold_pre2017": 1000.0,
            "liquidity_threshold_post2017": 1000.0,
        }
    )

    mask = strategy.build_tradable_mask(settle_prices, open_interest)

    assert not bool(mask.loc[dates[0], "A"])
    assert bool(mask.loc[dates[3], "A"])
    assert not bool(mask.loc[dates[3], "B"])
