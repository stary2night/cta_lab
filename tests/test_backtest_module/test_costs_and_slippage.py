from __future__ import annotations

import pandas as pd
import pandas.testing as pdt

from backtest import (
    FixedBpsSlippage,
    ProportionalCostModel,
    VectorizedBacktest,
)
from backtest.event import EventDrivenBacktestEngine, MarketDataPortal, Order
from strategies.base import EventDrivenStrategy


class BuyOneTargetWeight(EventDrivenStrategy):
    name = "buy_one"

    def on_bar(self, context):
        if context.now == pd.Timestamp("2024-01-01"):
            return [Order.target_weight("A", 1.0, context.now)]
        return []


class RoundTripMarketOrder(EventDrivenStrategy):
    name = "round_trip"

    def on_bar(self, context):
        if context.now == pd.Timestamp("2024-01-01"):
            return [Order.market("A", "buy", 1.0, context.now)]
        if context.now == pd.Timestamp("2024-01-02"):
            return [Order.market("A", "sell", 1.0, context.now)]
        return []


def test_vectorized_cost_model_matches_legacy_fee_rate() -> None:
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    returns = pd.DataFrame({"A": [0.0, 0.01, 0.02]}, index=dates)
    weights = pd.DataFrame({"A": [1.0, 0.5, 0.5]}, index=dates)

    legacy = VectorizedBacktest(
        lag=0,
        fee_rate=0.001,
        trim_inactive=False,
    ).run(weights, returns)
    unified = VectorizedBacktest(
        lag=0,
        cost_model=ProportionalCostModel(0.001),
        trim_inactive=False,
    ).run(weights, returns)

    pdt.assert_series_equal(legacy.returns, unified.returns)
    pdt.assert_series_equal(legacy.nav, unified.nav)


def test_vectorized_cost_uses_vol_targeted_effective_turnover() -> None:
    dates = pd.bdate_range("2024-01-01", periods=8)
    returns = pd.DataFrame(
        {"A": [0.01, -0.005, 0.008, -0.004, 0.006, -0.003, 0.004, -0.002]},
        index=dates,
    )
    weights = pd.DataFrame(
        {"A": [1.0, 1.0, -1.0, -1.0, 0.5, 0.5, 0.0, 0.0]},
        index=dates,
    )

    no_cost = VectorizedBacktest(
        lag=0,
        vol_target=0.10,
        vol_halflife=2,
        vol_min_periods=2,
        trim_inactive=False,
    ).run(weights, returns)
    with_cost = VectorizedBacktest(
        lag=0,
        vol_target=0.10,
        vol_halflife=2,
        vol_min_periods=2,
        cost_model=ProportionalCostModel(0.001),
        trim_inactive=False,
    ).run(weights, returns)

    assert with_cost.positions_df is not None
    effective_turnover = with_cost.positions_df.diff().fillna(with_cost.positions_df).abs().sum(axis=1)
    expected_cost = effective_turnover * 0.001
    actual_cost = (no_cost.returns.loc[dates] - with_cost.returns.loc[dates]).rename(None)
    pdt.assert_series_equal(actual_cost, expected_cost.rename(None), check_freq=False)


def test_vectorized_vol_target_waits_for_active_warmup() -> None:
    dates = pd.bdate_range("2024-01-01", periods=8)
    returns = pd.DataFrame({"A": [0.0, 0.0, 0.10, -0.05, 0.04, -0.02, 0.03, 0.01]}, index=dates)
    weights = pd.DataFrame({"A": [0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]}, index=dates)

    result = VectorizedBacktest(
        lag=1,
        vol_target=0.10,
        vol_halflife=2,
        vol_min_periods=3,
        trim_inactive=False,
    ).run(weights, returns)

    assert result.positions_df is not None
    # The scale must not be backfilled into the warmup period; otherwise the
    # first active days can start with a large look-ahead exposure.
    assert result.positions_df.loc[dates[:5], "A"].eq(0.0).all()
    assert result.positions_df.loc[dates[5], "A"] > 0.0


def test_event_broker_uses_proportional_cost_model_for_commission() -> None:
    prices = pd.DataFrame(
        {"A": [1.0, 1.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )

    result = EventDrivenBacktestEngine(
        data_portal=MarketDataPortal.from_prices(prices),
        cost_model=ProportionalCostModel(0.01),
    ).run(BuyOneTargetWeight())

    assert round(result.nav.iloc[0], 6) == 0.99
    assert result.fee_log is not None
    assert round(result.fee_log.loc[pd.Timestamp("2024-01-01"), "commission"], 6) == 0.01
    assert round(result.fee_log.loc[pd.Timestamp("2024-01-01"), "total_cost"], 6) == 0.01


def test_fixed_bps_slippage_moves_fill_price_against_order_side() -> None:
    prices = pd.DataFrame(
        {"A": [100.0, 100.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )
    engine = EventDrivenBacktestEngine(
        data_portal=MarketDataPortal.from_prices(prices),
        slippage_model=FixedBpsSlippage(10.0),
    )

    engine.run(RoundTripMarketOrder())
    fills = engine.recorder.fills_frame()

    assert round(fills.iloc[0]["price"], 4) == 100.1
    assert round(fills.iloc[1]["price"], 4) == 99.9
    assert fills.iloc[0]["slippage"] > 0.0
    assert fills.iloc[1]["slippage"] > 0.0
