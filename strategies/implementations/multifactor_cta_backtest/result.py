"""Result object for the multi-factor CTA strategy."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from backtest.result import BacktestResult


@dataclass
class MultiFactorCTARunResult:
    """End-to-end run result for ``MultiFactorCTAStrategy``."""

    returns: pd.DataFrame
    trend_signal: pd.DataFrame
    cross_signal: pd.DataFrame
    blended_signal: pd.DataFrame
    short_filter: pd.DataFrame
    filtered_signal: pd.DataFrame
    raw_positions: pd.DataFrame
    trend_positions: pd.DataFrame
    cross_positions: pd.DataFrame
    positions: pd.DataFrame
    pnl: pd.Series
    sigma: pd.DataFrame
    sector_map: dict[str, str]
    backtest_result: BacktestResult | None = None
    metadata: dict = field(default_factory=dict)
