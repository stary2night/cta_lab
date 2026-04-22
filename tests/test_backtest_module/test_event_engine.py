from __future__ import annotations

import pandas as pd
import pandas.testing as pdt

from backtest import VectorizedBacktest
from backtest.event import (
    EventDrivenBacktestEngine,
    MarketDataPortal,
    Order,
    TargetWeightStrategyAdapter,
)
from strategies.base import EventDrivenStrategy


class BuyAndHoldTargetWeight(EventDrivenStrategy):
    name = "buy_and_hold"

    def on_bar(self, context):
        if context.now == pd.Timestamp("2024-01-01"):
            return [Order.target_weight("A", 1.0, context.now)]
        return []


def test_event_engine_runs_target_weight_strategy() -> None:
    prices = pd.DataFrame(
        {"A": [1.0, 1.1, 1.21]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
    )

    engine = EventDrivenBacktestEngine(
        data_portal=MarketDataPortal.from_prices(prices),
        commission_rate=0.0,
    )
    result = engine.run(BuyAndHoldTargetWeight())

    assert result.nav.iloc[0] == 1.0
    assert round(result.nav.iloc[-1], 6) == 1.21
    assert round(result.returns.iloc[1], 6) == 0.1
    assert result.positions_df is not None
    assert result.positions_df.loc[pd.Timestamp("2024-01-01"), "A"] == 1.0
    assert result.turnover_series is not None
    assert result.turnover_series.iloc[0] == 1.0


def test_event_engine_applies_commission_on_target_weight_order() -> None:
    prices = pd.DataFrame(
        {"A": [1.0, 1.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )

    engine = EventDrivenBacktestEngine(
        data_portal=MarketDataPortal.from_prices(prices),
        commission_rate=0.01,
    )
    result = engine.run(BuyAndHoldTargetWeight())

    assert round(result.nav.iloc[0], 6) == 0.99
    assert result.fee_log is not None
    assert round(result.fee_log.loc[pd.Timestamp("2024-01-01"), "commission"], 6) == 0.01


def test_target_weight_adapter_matches_vectorized_backtest_lag0_fee0() -> None:
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    prices = pd.DataFrame(
        {
            "A": [1.0, 1.1, 1.21],
            "B": [1.0, 0.9, 0.81],
        },
        index=dates,
    )
    weights = pd.DataFrame(
        {
            "A": [0.5, 0.5, 0.5],
            "B": [0.5, 0.5, 0.5],
        },
        index=dates,
    )

    event_result = EventDrivenBacktestEngine(
        data_portal=MarketDataPortal.from_prices(prices),
        commission_rate=0.0,
    ).run(TargetWeightStrategyAdapter(weights=weights))

    vector_result = VectorizedBacktest(
        lag=0,
        fee_rate=0.0,
        trim_inactive=False,
    ).run(weights, prices.pct_change())
    vector_nav = vector_result.nav.loc[event_result.nav.index]
    vector_returns = vector_result.returns.loc[event_result.returns.index]

    pdt.assert_series_equal(event_result.nav, vector_nav, check_names=False)
    pdt.assert_series_equal(event_result.returns, vector_returns, check_names=False)
    assert event_result.positions_df is not None
    pdt.assert_frame_equal(event_result.positions_df, weights, check_names=False)


def test_sparse_rebalance_allows_weight_drift_without_turnover() -> None:
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    prices = pd.DataFrame(
        {
            "A": [1.0, 1.1, 1.21],
            "B": [1.0, 0.9, 0.81],
        },
        index=dates,
    )
    weights = pd.DataFrame(
        {
            "A": [0.5, 0.5, 0.5],
            "B": [0.5, 0.5, 0.5],
        },
        index=dates,
    )

    result = EventDrivenBacktestEngine(
        data_portal=MarketDataPortal.from_prices(prices),
        commission_rate=0.0,
    ).run(
        TargetWeightStrategyAdapter(
            weights=weights,
            rebalance_dates=[pd.Timestamp("2024-01-01")],
        )
    )

    assert result.positions_df is not None
    assert round(result.positions_df.loc[pd.Timestamp("2024-01-02"), "A"], 6) == 0.55
    assert round(result.positions_df.loc[pd.Timestamp("2024-01-02"), "B"], 6) == 0.45
    assert result.turnover_series is not None
    assert round(result.turnover_series.loc[pd.Timestamp("2024-01-01")], 6) == 1.0
    assert result.turnover_series.loc[pd.Timestamp("2024-01-02")] == 0.0
    assert result.turnover_series.loc[pd.Timestamp("2024-01-03")] == 0.0


def test_target_weight_adapter_close_rebalance_matches_vectorized_lag1() -> None:
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    prices = pd.DataFrame(
        {
            "A": [1.0, 1.1, 1.21],
            "B": [1.0, 0.9, 0.81],
        },
        index=dates,
    )
    weights = pd.DataFrame(
        {
            "A": [1.0, 0.0, 0.0],
            "B": [0.0, 1.0, 1.0],
        },
        index=dates,
    )

    event_result = EventDrivenBacktestEngine(
        data_portal=MarketDataPortal.from_prices(prices),
        commission_rate=0.0,
    ).run(TargetWeightStrategyAdapter(weights=weights))

    vector_result = VectorizedBacktest(
        lag=1,
        fee_rate=0.0,
        trim_inactive=False,
    ).run(weights, prices.pct_change())
    vector_nav = vector_result.nav.loc[event_result.nav.index]
    vector_returns = vector_result.returns.loc[event_result.returns.index]

    pdt.assert_series_equal(event_result.nav, vector_nav, check_names=False)
    pdt.assert_series_equal(event_result.returns, vector_returns, check_names=False)


def test_target_weight_adapter_execution_lag_delays_rebalance_date() -> None:
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    prices = pd.DataFrame({"A": [1.0, 1.1, 1.21]}, index=dates)
    weights = pd.DataFrame({"A": [1.0, 1.0, 1.0]}, index=dates)

    result = EventDrivenBacktestEngine(
        data_portal=MarketDataPortal.from_prices(prices),
        commission_rate=0.0,
    ).run(
        TargetWeightStrategyAdapter(
            weights=weights,
            rebalance_dates=[pd.Timestamp("2024-01-01")],
            execution_lag=1,
        )
    )

    assert result.positions_df is not None
    assert result.positions_df.loc[pd.Timestamp("2024-01-01"), "A"] == 0.0
    assert result.positions_df.loc[pd.Timestamp("2024-01-02"), "A"] == 1.0
    assert result.turnover_series is not None
    assert result.turnover_series.loc[pd.Timestamp("2024-01-01")] == 0.0
    assert round(result.turnover_series.loc[pd.Timestamp("2024-01-02")], 6) == 1.0
