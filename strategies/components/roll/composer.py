"""Roll Strategy Layer 的组合与穿透组件。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class ValueComposer(ABC):
    """把 roll execution path 组合成资产级 value series。"""

    @abstractmethod
    def compose(
        self,
        *,
        schedule: pd.DataFrame,
        market_data: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        """
        返回组合结果。

        约定最少返回:
        - value_series: pd.Series
        可选返回:
        - roll_return
        - component_values
        - component_weights
        - metadata
        """


class LookThroughResolver(ABC):
    """把资产级结果穿透到底层可交易资产。"""

    @abstractmethod
    def resolve(
        self,
        *,
        schedule: pd.DataFrame,
        composition_result: dict[str, Any],
        context: dict[str, Any],
    ) -> pd.DataFrame:
        """返回底层资产穿透结果。"""


class SingleContractValueComposer(ValueComposer):
    """最小实现：用单合约持仓路径构造资产级净值。"""

    def compose(
        self,
        *,
        schedule: pd.DataFrame,
        market_data: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        prices = market_data.get("prices")
        if prices is None or getattr(prices, "empty", True):
            return {"value_series": pd.Series(dtype=float), "roll_return": pd.Series(dtype=float)}

        prices = prices.sort_index()
        if schedule.empty:
            value_series = pd.Series(1.0, index=prices.index, dtype=float)
            return {"value_series": value_series, "roll_return": pd.Series(0.0, index=prices.index, dtype=float)}

        weights = (
            schedule.pivot_table(index="trade_date", columns="contract_id", values="weight", aggfunc="sum")
            .reindex(prices.index)
            .ffill()
            .fillna(0.0)
        )
        weights = weights.reindex(columns=prices.columns, fill_value=0.0)
        returns = prices.pct_change().fillna(0.0)
        effective_weights = weights.shift(1).fillna(0.0)
        portfolio_returns = (effective_weights * returns).sum(axis=1)
        value_series = (1.0 + portfolio_returns).cumprod()
        if not value_series.empty:
            value_series.iloc[0] = 1.0

        return {
            "value_series": value_series,
            "roll_return": portfolio_returns,
            "component_weights": weights,
            "metadata": {"composer": "SingleContractValueComposer"},
        }


class SimpleLookThroughResolver(LookThroughResolver):
    """最小实现：直接把 schedule 视作底层持仓。"""

    def resolve(
        self,
        *,
        schedule: pd.DataFrame,
        composition_result: dict[str, Any],
        context: dict[str, Any],
    ) -> pd.DataFrame:
        if schedule.empty:
            return pd.DataFrame(columns=["trade_date", "contract_id", "exposure_weight"])
        lookthrough = schedule.copy()
        lookthrough = lookthrough.rename(columns={"weight": "exposure_weight"})
        return lookthrough.reset_index(drop=True)
