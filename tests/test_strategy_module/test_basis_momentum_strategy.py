from __future__ import annotations

from datetime import date

import pandas as pd

from backtest import VectorizedBacktest
from data.model.contract import Contract
from strategies.implementations.basis_momentum_backtest import BasisMomentumStrategy


class _BarStub:
    def __init__(self, data: pd.DataFrame) -> None:
        self.data = data


def test_basis_momentum_select_far_contract_skips_inactive_low_oi_contract() -> None:
    trade_date = pd.Timestamp("2024-01-05")
    contracts = [
        Contract("M", "M2405.DCE", "DCE", date(2023, 11, 1), date(2024, 5, 15), date(2024, 5, 14)),
        Contract("M", "M2406.DCE", "DCE", date(2023, 12, 1), date(2024, 6, 15), date(2024, 6, 14)),
        Contract("M", "M2407.DCE", "DCE", date(2023, 12, 1), date(2024, 7, 15), date(2024, 7, 14)),
    ]
    bar_data = {
        "M2405.DCE": _BarStub(pd.DataFrame({"settle": [100.0], "open_interest": [100.0]}, index=[trade_date])),
        "M2406.DCE": _BarStub(pd.DataFrame({"settle": [101.0], "open_interest": [4.0]}, index=[trade_date])),
        "M2407.DCE": _BarStub(pd.DataFrame({"settle": [102.0], "open_interest": [12.0]}, index=[trade_date])),
    }
    strategy = BasisMomentumStrategy(config={"active_oi_pct_threshold": 0.05})

    far_contract, oi_share = strategy.select_far_contract(
        near_contract="M2405.DCE",
        date=trade_date,
        contracts=contracts,
        bar_data=bar_data,
    )

    assert far_contract == "M2407.DCE"
    assert oi_share > 0.05


def test_basis_momentum_signal_is_positive_when_near_leg_outperforms_far_leg() -> None:
    dates = pd.bdate_range("2024-01-01", periods=4)
    near_prices = pd.DataFrame({"A": [100.0, 103.0, 106.0, 109.0]}, index=dates)
    far_prices = pd.DataFrame({"A": [100.0, 101.0, 102.0, 103.0]}, index=dates)
    strategy = BasisMomentumStrategy(config={"signal_window": 2})

    _, basis_change, signal = strategy.compute_signal_matrices(near_prices, far_prices)

    assert basis_change.loc[dates[1], "A"] > 0.0
    assert signal.loc[dates[-1], "A"] > 0.0


def test_basis_momentum_staggered_rebalance_averages_tranches() -> None:
    dates = pd.bdate_range("2024-01-01", periods=4)
    daily_positions = pd.DataFrame({"A": [1.0, 0.0, 0.0, 0.0]}, index=dates)
    strategy = BasisMomentumStrategy(config={"rebalance_buckets": 2})

    positions = strategy.apply_staggered_rebalance(daily_positions)

    assert positions.loc[dates[0], "A"] == 0.5
    assert positions.loc[dates[1], "A"] == 0.5
    assert positions.loc[dates[2], "A"] == 0.0


def test_basis_momentum_build_daily_positions_is_zero_sum() -> None:
    """Weight formula must produce a zero-sum long-short portfolio.

    The ranking formula was previously off by a constant that produced all-negative
    weights (pure short portfolio).  Verify net exposure ≈ 0 and both long and short
    positions exist.
    """
    import numpy as np

    dates = pd.bdate_range("2024-01-01", periods=30)
    n_assets = 6
    rng = np.random.default_rng(0)
    signals = pd.DataFrame(
        rng.standard_normal((30, n_assets)),
        index=dates,
        columns=[f"X{i}" for i in range(n_assets)],
    )
    strategy = BasisMomentumStrategy(config={"selection_weighting": "equal", "max_abs_weight": 1.0})
    positions = strategy.build_daily_positions(signals)

    # Net exposure per day must be close to zero (market-neutral)
    net = positions.sum(axis=1)
    assert (net.abs() < 1e-9).all(), f"Positions are not zero-sum: max |net| = {net.abs().max()}"

    # Both long and short positions must exist
    assert (positions > 0).any().any(), "No long positions found (possible pure-short bug)"
    assert (positions < 0).any().any(), "No short positions found"


def test_basis_momentum_strategy_runs_on_prepared_matrices() -> None:
    dates = pd.bdate_range("2020-01-01", periods=80)
    near_prices = pd.DataFrame(
        {
            "A": 100.0 + pd.Series(range(80), index=dates) * 1.2,
            "B": 100.0 + pd.Series(range(80), index=dates) * 1.0,
            "C": 100.0 + pd.Series(range(80), index=dates) * 0.8,
            "D": 100.0 + pd.Series(range(80), index=dates) * 0.6,
        },
        index=dates,
    )
    far_prices = pd.DataFrame(
        {
            "A": 100.0 + pd.Series(range(80), index=dates) * 0.6,
            "B": 100.0 + pd.Series(range(80), index=dates) * 0.7,
            "C": 100.0 + pd.Series(range(80), index=dates) * 1.1,
            "D": 100.0 + pd.Series(range(80), index=dates) * 1.2,
        },
        index=dates,
    )
    returns = near_prices.pct_change().fillna(0.0)
    tradable_mask = pd.DataFrame(True, index=dates, columns=near_prices.columns)
    strategy = BasisMomentumStrategy(
        config={
            "signal_window": 5,
            "vol_scale_windows": [5, 10, 20],
            "rebalance_buckets": 5,
            "min_obs": 20,
            "max_abs_weight": 1.0,
            "apply_portfolio_vol_scale": False,
        }
    )

    result = strategy.run_vectorized(
        returns,
        backtest=VectorizedBacktest(lag=1, vol_target=None, trim_inactive=False),
        near_prices_df=near_prices,
        far_prices_df=far_prices,
        tradable_mask=tradable_mask,
    )

    assert result.positions_df is not None
    assert set(result.positions_df.columns) == {"A", "B", "C", "D"}
    assert result.turnover_series is not None
    assert len(result.returns) > 0
