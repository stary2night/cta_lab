from __future__ import annotations

import numpy as np
import pandas as pd

from backtest import VectorizedBacktest
from strategies import EventDrivenStrategy, StrategyBase, VectorizedStrategy
from strategies.examples import SimpleRelativeMomentumEventStrategy
from strategies.implementations.crossmom_backtest import CrossMOMStrategy
from strategies.implementations.dual_momentum_backtest import DualMomentumStrategy


def test_event_driven_strategy_run_event_backtest() -> None:
    dates = pd.bdate_range("2024-01-02", periods=45)
    prices = pd.DataFrame(
        {
            "TREND": np.linspace(100.0, 120.0, len(dates)),
            "DEFENSIVE": np.linspace(100.0, 98.0, len(dates)),
        },
        index=dates,
    )

    strategy = SimpleRelativeMomentumEventStrategy(lookback=10, rebalance_every=10)
    result = strategy.run_event_backtest(prices, commission_rate=0.0)

    assert isinstance(strategy, EventDrivenStrategy)
    assert not result.nav.empty
    assert result.positions_df is not None
    assert result.positions_df["TREND"].max() > 0.0
    assert result.turnover_series is not None
    assert (result.turnover_series > 0).sum() >= 1


class BuyAndHoldVectorizedStrategy(VectorizedStrategy):
    name = "buy_and_hold_vectorized"

    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(1.0, index=price_df.index, columns=price_df.columns)

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict | None = None,
    ) -> pd.DataFrame:
        return signal_df.div(signal_df.abs().sum(axis=1), axis=0).fillna(0.0)


def test_vectorized_strategy_protocol_runs_vectorized_backtest() -> None:
    dates = pd.bdate_range("2024-01-02", periods=5)
    returns = pd.DataFrame(
        {
            "A": [0.0, 0.01, 0.02, -0.01, 0.00],
            "B": [0.0, -0.01, 0.00, 0.01, 0.02],
        },
        index=dates,
    )

    strategy = BuyAndHoldVectorizedStrategy()
    result = strategy.run_vectorized(
        returns,
        backtest=VectorizedBacktest(lag=0, trim_inactive=False),
    )

    assert isinstance(strategy, VectorizedStrategy)
    assert not result.nav.empty
    assert result.positions_df is not None
    assert set(result.positions_df.columns) == {"A", "B"}


def test_strategy_base_is_vectorized_strategy() -> None:
    assert issubclass(StrategyBase, VectorizedStrategy)


def test_crossmom_and_dual_momentum_inherit_strategy_base() -> None:
    assert issubclass(CrossMOMStrategy, StrategyBase)
    assert issubclass(CrossMOMStrategy, VectorizedStrategy)
    assert issubclass(DualMomentumStrategy, StrategyBase)
    assert issubclass(DualMomentumStrategy, VectorizedStrategy)
