"""交易费用模型。"""

from __future__ import annotations

import pandas as pd

from .base import FeeModel


class TradingFee(FeeModel):
    """交易费：调仓日按换手量收费。

    fee = rate × Σ|holdings_new - holdings_prev|
    仅在 is_rebalance=True 时生效。
    """

    def __init__(self, rate: float = 0.0005) -> None:
        self.rate = rate

    def daily_fee(
        self,
        date: pd.Timestamp,
        nav: float,
        holdings: pd.Series,
        prev_holdings: pd.Series,
        is_rebalance: bool,
    ) -> float:
        if not is_rebalance:
            return 0.0
        turnover = (holdings - prev_holdings).abs().sum()
        return self.rate * turnover
