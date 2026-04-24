"""Result object for the skew reversal strategy."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from backtest.result import BacktestResult


@dataclass
class SkewReversalRunResult:
    """End-to-end run result for ``SkewReversalStrategy``."""

    returns: pd.DataFrame
    settle_returns: pd.DataFrame
    close_returns: pd.DataFrame
    open_interest: pd.DataFrame
    oi_change: pd.DataFrame
    settle_skew: pd.DataFrame
    close_skew: pd.DataFrame
    skew_factor: pd.DataFrame
    raw_positions: pd.DataFrame
    smoothed_positions: pd.DataFrame
    vol_scale: pd.DataFrame
    daily_positions: pd.DataFrame
    positions: pd.DataFrame
    pnl: pd.Series
    backtest_result: BacktestResult | None = None
    metadata: dict = field(default_factory=dict)
