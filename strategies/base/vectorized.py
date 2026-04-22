"""Base protocol for matrix/vectorized research strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from backtest.result import BacktestResult
    from backtest.vectorized import VectorizedBacktest


class VectorizedStrategy(ABC):
    """Strategy protocol for the matrix research path.

    Vectorized strategies transform matrix inputs into target weights and rely
    on ``VectorizedBacktest`` for fast PnL evaluation. This class makes that
    path explicit without changing the legacy ``StrategyBase`` API.
    """

    name: str = "vectorized_strategy"

    @abstractmethod
    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """Compute signal matrix from prices."""

    @abstractmethod
    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict | None = None,
    ) -> pd.DataFrame:
        """Convert signals into target weights."""

    def run_vectorized(
        self,
        returns_df: pd.DataFrame,
        backtest: "VectorizedBacktest | None" = None,
        vol_window: int = 20,
    ) -> "BacktestResult":
        """Run the strategy on the vectorized backtest path."""

        from backtest.vectorized import VectorizedBacktest as _VBT

        price_df = (1.0 + returns_df.fillna(0.0)).cumprod()
        vol_df = returns_df.rolling(vol_window).std() * np.sqrt(252)
        signal_df = self.generate_signals(price_df)
        weight_df = self.build_weights(signal_df, vol_df)

        bt = backtest if backtest is not None else _VBT()
        return bt.run(weight_df, returns_df)
