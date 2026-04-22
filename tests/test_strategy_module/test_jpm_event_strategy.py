from __future__ import annotations

import numpy as np
import pandas as pd

from backtest.event import EventDrivenBacktestEngine, MarketDataPortal
from strategies.base import EventDrivenStrategy
from strategies.implementations.jpm_trend_trade import (
    JPMConfig,
    JPMEventDrivenConfig,
    JPMEventDrivenStrategy,
    JPMTrendStrategy,
)


def _synthetic_returns() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=80)
    return pd.DataFrame(
        {
            "A": np.linspace(0.001, 0.003, len(dates)),
            "B": np.linspace(-0.001, -0.002, len(dates)),
            "C": np.sin(np.linspace(0, 6, len(dates))) * 0.001,
        },
        index=dates,
    )


def _small_jpm_config() -> JPMConfig:
    return JPMConfig(
        lookbacks=[4, 8],
        min_obs=10,
        sigma_halflife=4,
        vol_halflife=4,
        corr_window=12,
        corr_min_periods=6,
        target_vol=0.10,
    )


def test_jpm_event_driven_baseline_runs() -> None:
    strategy = JPMEventDrivenStrategy(
        jpm_config=_small_jpm_config(),
        event_config=JPMEventDrivenConfig(
            mode="baseline",
            rebalance_every=5,
            min_history=12,
            apply_vol_target=True,
        ),
    )
    result = strategy.run_event_backtest(
        data_portal=MarketDataPortal.from_returns(_synthetic_returns()),
        commission_rate=0.0,
    )

    assert isinstance(strategy, EventDrivenStrategy)
    assert not result.nav.empty
    assert result.positions_df is not None
    assert result.positions_df.abs().sum(axis=1).max() > 0.0
    assert result.turnover_series is not None
    assert (result.turnover_series > 0).sum() >= 1


def test_jpm_event_driven_corrcap_runs() -> None:
    strategy = JPMEventDrivenStrategy(
        jpm_config=_small_jpm_config(),
        event_config={
            "mode": "corrcap",
            "rebalance_every": 5,
            "min_history": 12,
        },
    )
    result = EventDrivenBacktestEngine(
        data_portal=MarketDataPortal.from_returns(_synthetic_returns()),
        commission_rate=0.0,
    ).run(strategy)

    assert not result.nav.empty
    assert result.positions_df is not None
    assert result.positions_df.abs().sum(axis=1).max() > 0.0


def test_jpm_event_driven_precomputes_market_features_once() -> None:
    class CountingJPMTrendStrategy(JPMTrendStrategy):
        def __init__(self) -> None:
            super().__init__(config=_small_jpm_config())
            self.signal_calls = 0
            self.sigma_calls = 0

        def generate_signals_from_returns(self, returns_df: pd.DataFrame) -> pd.DataFrame:
            self.signal_calls += 1
            return super().generate_signals_from_returns(returns_df)

        def _compute_sigma(self, returns_df: pd.DataFrame) -> pd.DataFrame:
            self.sigma_calls += 1
            return super()._compute_sigma(returns_df)

    base_strategy = CountingJPMTrendStrategy()
    strategy = JPMEventDrivenStrategy(
        strategy=base_strategy,
        event_config={
            "mode": "baseline",
            "rebalance_every": 5,
            "min_history": 12,
        },
    )

    strategy.run_event_backtest(
        data_portal=MarketDataPortal.from_returns(_synthetic_returns()),
        commission_rate=0.0,
    )

    assert base_strategy.signal_calls == 1
    assert base_strategy.sigma_calls == 1
