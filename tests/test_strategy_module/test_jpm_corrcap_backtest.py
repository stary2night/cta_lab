from __future__ import annotations

from backtest import ProportionalCostModel, VectorizedBacktest
from strategies.implementations.jpm_trend_trade.strategy import JPMTrendStrategy


def test_jpm_corrcap_backtest_disables_second_vol_targeting() -> None:
    base = VectorizedBacktest(
        lag=1,
        vol_target=0.10,
        vol_halflife=21,
        trading_days=252,
        cost_model=ProportionalCostModel(0.0005),
        trim_inactive=False,
    )

    corrcap = JPMTrendStrategy._make_corrcap_backtest(base)

    assert corrcap.lag == base.lag
    assert corrcap.vol_target is None
    assert corrcap.cost_model is base.cost_model
    assert corrcap.trim_inactive == base.trim_inactive
