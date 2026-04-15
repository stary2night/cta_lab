"""Roll Strategy Layer 基类骨架。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from .profile import RollStrategyProfile
from .result import RollStrategyResult


class RollStrategyBase(ABC):
    """资产级 roll strategy 的统一 orchestrator。"""

    def __init__(self, profile: RollStrategyProfile):
        self.profile = profile

    @abstractmethod
    def build_candidate_universe(
        self,
        *,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """构造候选合约集合。"""

    @abstractmethod
    def build_contract_plan(
        self,
        *,
        candidates: pd.DataFrame,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> dict[str, pd.DataFrame]:
        """
        构造目标合约计划与决策痕迹。

        约定返回可包含:
        - contract_plan
        - eligible_contracts
        - lifecycle_state
        - market_state_snapshot
        - decision_trace
        """

    @abstractmethod
    def build_roll_schedule(
        self,
        *,
        contract_plan: pd.DataFrame,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """把目标合约计划转成执行路径。"""

    @abstractmethod
    def compose_value(
        self,
        *,
        schedule: pd.DataFrame,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> dict[str, Any]:
        """
        组合资产级价值序列。

        约定最少返回:
        - value_series
        """

    @abstractmethod
    def resolve_lookthrough(
        self,
        *,
        schedule: pd.DataFrame,
        composition_result: dict[str, Any],
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """生成底层可交易资产的穿透结果。"""

    def run(
        self,
        *,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> RollStrategyResult:
        """运行完整 roll strategy pipeline。"""
        candidates = self.build_candidate_universe(market_data=market_data, start=start, end=end)
        plan_result = self.build_contract_plan(
            candidates=candidates,
            market_data=market_data,
            start=start,
            end=end,
        )
        contract_plan = plan_result.get("contract_plan", pd.DataFrame())
        schedule = self.build_roll_schedule(
            contract_plan=contract_plan,
            market_data=market_data,
            start=start,
            end=end,
        )
        composition_result = self.compose_value(
            schedule=schedule,
            market_data=market_data,
            start=start,
            end=end,
        )
        lookthrough_book = self.resolve_lookthrough(
            schedule=schedule,
            composition_result=composition_result,
            market_data=market_data,
            start=start,
            end=end,
        )
        value_series = composition_result.get("value_series", pd.Series(dtype=float))
        return RollStrategyResult(
            value_series=value_series,
            contract_plan=contract_plan,
            roll_schedule=schedule,
            lookthrough_book=lookthrough_book,
            roll_return=composition_result.get("roll_return"),
            component_values=composition_result.get("component_values", pd.DataFrame()),
            component_weights=composition_result.get("component_weights", pd.DataFrame()),
            eligible_contracts=plan_result.get("eligible_contracts", pd.DataFrame()),
            lifecycle_state=plan_result.get("lifecycle_state", pd.DataFrame()),
            market_state_snapshot=plan_result.get("market_state_snapshot", pd.DataFrame()),
            decision_trace=plan_result.get("decision_trace", pd.DataFrame()),
            metadata=composition_result.get("metadata", {}),
        )
