"""零费用模型。"""

from __future__ import annotations

import pandas as pd

from .base import FeeModel


class ZeroFee(FeeModel):
    """无费用（CTA 基准回测默认）。"""

    def daily_fee(
        self,
        date: pd.Timestamp,
        nav: float,
        holdings: pd.Series,
        prev_holdings: pd.Series,
        is_rebalance: bool,
    ) -> float:
        return 0.0
