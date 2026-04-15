"""Roll Strategy Layer 的输出对象。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(slots=True)
class RollStrategyResult:
    """统一承载资产级 value 与可穿透结果。"""

    value_series: pd.Series
    contract_plan: pd.DataFrame = field(default_factory=pd.DataFrame)
    roll_schedule: pd.DataFrame = field(default_factory=pd.DataFrame)
    lookthrough_book: pd.DataFrame = field(default_factory=pd.DataFrame)
    roll_return: pd.Series | None = None
    component_values: pd.DataFrame = field(default_factory=pd.DataFrame)
    component_weights: pd.DataFrame = field(default_factory=pd.DataFrame)
    eligible_contracts: pd.DataFrame = field(default_factory=pd.DataFrame)
    lifecycle_state: pd.DataFrame = field(default_factory=pd.DataFrame)
    market_state_snapshot: pd.DataFrame = field(default_factory=pd.DataFrame)
    decision_trace: pd.DataFrame = field(default_factory=pd.DataFrame)
    metadata: dict[str, Any] = field(default_factory=dict)

    def performance_view(self) -> pd.DataFrame:
        """返回适合上层 signal/allocation 消费的性能视图。"""
        frame = pd.DataFrame({"value": self.value_series})
        if self.roll_return is not None:
            frame["roll_return"] = self.roll_return.reindex(frame.index)
        return frame

    def lookthrough_view(self, date: str | pd.Timestamp | None = None) -> pd.DataFrame:
        """返回底层资产穿透视图。"""
        if date is None or self.lookthrough_book.empty:
            return self.lookthrough_book.copy()
        ts = pd.Timestamp(date)
        if "trade_date" not in self.lookthrough_book.columns:
            return self.lookthrough_book.copy()
        mask = pd.to_datetime(self.lookthrough_book["trade_date"]) == ts
        return self.lookthrough_book.loc[mask].copy()

