"""Slippage models for lightweight event-driven simulations."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


class SlippageModel:
    """Base class for execution price adjustment models."""

    def fill_price(
        self,
        mid_price: float,
        side: object,
        *,
        timestamp: pd.Timestamp | None = None,
        symbol: str | None = None,
    ) -> float:
        """Return execution price from a reference snapshot price."""

        return float(mid_price)


@dataclass(frozen=True)
class NoSlippage(SlippageModel):
    """Execute at snapshot price."""


@dataclass(frozen=True)
class FixedBpsSlippage(SlippageModel):
    """Apply a fixed basis-point spread against the trader.

    Buy orders pay above the snapshot price; sell orders receive below it.
    """

    bps: float = 0.0

    def fill_price(
        self,
        mid_price: float,
        side: object,
        *,
        timestamp: pd.Timestamp | None = None,
        symbol: str | None = None,
    ) -> float:
        spread = self.bps / 10_000.0
        side_value = getattr(side, "value", side)
        if str(side_value).lower() == "buy":
            return float(mid_price * (1.0 + spread))
        return float(mid_price * (1.0 - spread))
