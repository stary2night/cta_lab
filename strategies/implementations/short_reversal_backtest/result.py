"""Result object for the short-term reversal strategy."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from backtest.result import BacktestResult


@dataclass
class ShortReversalRunResult:
    """End-to-end run result for ``ShortReversalStrategy``."""

    returns: pd.DataFrame           # continuous OI-max returns matrix
    settle_prices: pd.DataFrame     # continuous near-leg settle prices
    open_interest: pd.DataFrame     # continuous near-leg OI
    signal: pd.DataFrame            # -cumulative_log_return(window), clipped
    tradable_mask: pd.DataFrame
    sigma_max: pd.DataFrame
    raw_positions: pd.DataFrame
    positions: pd.DataFrame
    portfolio_vol_scale: pd.Series
    pnl: pd.Series
    backtest_result: BacktestResult | None = None
    metadata: dict = field(default_factory=dict)
