"""Result object for the basis value (mean-reversion) strategy."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from backtest.result import BacktestResult


@dataclass
class BasisValueRunResult:
    """End-to-end run result for ``BasisValueStrategy``."""

    returns: pd.DataFrame
    near_returns: pd.DataFrame
    far_returns: pd.DataFrame
    near_prices: pd.DataFrame
    far_prices: pd.DataFrame
    near_open_interest: pd.DataFrame
    far_open_interest: pd.DataFrame
    far_oi_share: pd.DataFrame
    dominant_contracts: pd.DataFrame
    far_contracts: pd.DataFrame
    term_structure: pd.DataFrame
    basis_z: pd.DataFrame          # 252-day z-score of log(near/far)
    signal: pd.DataFrame           # mean-reversion signal = -basis_z (clipped)
    tradable_mask: pd.DataFrame
    sigma_max: pd.DataFrame
    raw_positions: pd.DataFrame
    positions: pd.DataFrame
    portfolio_vol_scale: pd.Series
    pnl: pd.Series
    backtest_result: BacktestResult | None = None
    metadata: dict = field(default_factory=dict)
