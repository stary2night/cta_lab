"""单品种 roll asset 的最小 concrete strategy。"""

from __future__ import annotations

from typing import Any

import pandas as pd

from .base import RollStrategyBase
from .composer import LookThroughResolver, SimpleLookThroughResolver, SingleContractValueComposer, ValueComposer
from .profile import RollStrategyProfile
from .rules import (
    ContractSelector,
    FieldMaxMarketStateRule,
    FixedDaysBeforeExpiryLifecycleRule,
    HybridContractSelector,
    ImmediateRollExecutor,
    LifecycleRule,
    MarketStateRule,
    RollExecutor,
)


class SingleAssetRollStrategy(RollStrategyBase):
    """第一版可运行的单品种 roll asset。"""

    def __init__(
        self,
        profile: RollStrategyProfile,
        *,
        lifecycle_rule: LifecycleRule | None = None,
        market_state_rule: MarketStateRule | None = None,
        selector: ContractSelector | None = None,
        executor: RollExecutor | None = None,
        composer: ValueComposer | None = None,
        lookthrough_resolver: LookThroughResolver | None = None,
    ) -> None:
        super().__init__(profile)
        self.lifecycle_rule = lifecycle_rule or FixedDaysBeforeExpiryLifecycleRule()
        self.market_state_rule = market_state_rule or FieldMaxMarketStateRule("open_interest")
        self.selector = selector or HybridContractSelector()
        self.executor = executor or ImmediateRollExecutor()
        self.composer = composer or SingleContractValueComposer()
        self.lookthrough_resolver = lookthrough_resolver or SimpleLookThroughResolver()

    @staticmethod
    def _normalize_dates(
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None,
        end: str | pd.Timestamp | None,
    ) -> pd.DatetimeIndex:
        prices = market_data.get("prices")
        if prices is None or getattr(prices, "empty", True):
            return pd.DatetimeIndex([])
        dates = pd.DatetimeIndex(pd.to_datetime(prices.index)).sort_values()
        if start is not None:
            dates = dates[dates >= pd.Timestamp(start)]
        if end is not None:
            dates = dates[dates <= pd.Timestamp(end)]
        return dates

    def build_candidate_universe(
        self,
        *,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        contracts = market_data.get("contracts")
        if contracts is None or getattr(contracts, "empty", True):
            return pd.DataFrame(columns=["contract_id", "last_trade_date", "last_holding_date"])

        frame = contracts.copy()
        if "last_trade_date" in frame.columns:
            frame["last_trade_date"] = pd.to_datetime(frame["last_trade_date"])
        if "last_holding_date" in frame.columns:
            frame["last_holding_date"] = pd.to_datetime(frame["last_holding_date"])
        if "contract_id" in frame.columns:
            frame = frame.sort_values("last_trade_date").reset_index(drop=True)
        return frame

    def build_contract_plan(
        self,
        *,
        candidates: pd.DataFrame,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> dict[str, pd.DataFrame]:
        trading_dates = self._normalize_dates(market_data, start, end)
        if candidates.empty or len(trading_dates) == 0:
            empty = pd.DataFrame()
            return {
                "contract_plan": empty,
                "eligible_contracts": empty,
                "lifecycle_state": empty,
                "market_state_snapshot": empty,
                "decision_trace": empty,
            }

        plan_rows: list[dict[str, Any]] = []
        lifecycle_rows: list[dict[str, Any]] = []
        market_rows: list[pd.DataFrame] = []
        eligible_rows: list[dict[str, Any]] = []
        current_plan: dict[str, Any] | None = None

        for date in trading_dates:
            active = candidates.loc[candidates["last_trade_date"] >= date].copy()
            if active.empty:
                continue
            for cid in active["contract_id"]:
                eligible_rows.append({"trade_date": date, "contract_id": cid})

            lifecycle_state = self.lifecycle_rule.evaluate(
                contracts=candidates,
                date=date,
                context={
                    "current_contract": None if current_plan is None else current_plan.get("target_contract"),
                    "trading_dates": trading_dates,
                },
            )
            market_state = self.market_state_rule.evaluate(
                candidates=active,
                date=date,
                market_data=market_data,
                context={
                    "profile": self.profile,
                    "current_contract": None if current_plan is None else current_plan.get("target_contract"),
                    "lifecycle_state": lifecycle_state,
                    "all_contracts": candidates,
                },
            )
            decision = self.selector.select(
                date=date,
                lifecycle_state=lifecycle_state,
                market_state=market_state,
                current_plan=current_plan,
                context={"profile": self.profile},
            )
            current_plan = decision
            plan_rows.append(decision)
            lifecycle_rows.append({"trade_date": date, **lifecycle_state})
            scores = market_state.get("scores")
            if isinstance(scores, pd.DataFrame) and not scores.empty:
                market_rows.append(scores)

        contract_plan = pd.DataFrame(plan_rows)
        lifecycle_state = pd.DataFrame(lifecycle_rows)
        market_state_snapshot = pd.concat(market_rows, ignore_index=True) if market_rows else pd.DataFrame()
        eligible_contracts = pd.DataFrame(eligible_rows)
        decision_trace = contract_plan.copy()

        return {
            "contract_plan": contract_plan,
            "eligible_contracts": eligible_contracts,
            "lifecycle_state": lifecycle_state,
            "market_state_snapshot": market_state_snapshot,
            "decision_trace": decision_trace,
        }

    def build_roll_schedule(
        self,
        *,
        contract_plan: pd.DataFrame,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        trading_dates = self._normalize_dates(market_data, start, end)
        return self.executor.build_schedule(
            target_plan=contract_plan,
            trading_calendar=trading_dates,
            context={"profile": self.profile},
        )

    def compose_value(
        self,
        *,
        schedule: pd.DataFrame,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> dict[str, Any]:
        return self.composer.compose(
            schedule=schedule,
            market_data=market_data,
            context={"profile": self.profile, "start": start, "end": end},
        )

    def resolve_lookthrough(
        self,
        *,
        schedule: pd.DataFrame,
        composition_result: dict[str, Any],
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        return self.lookthrough_resolver.resolve(
            schedule=schedule,
            composition_result=composition_result,
            context={"profile": self.profile, "start": start, "end": end},
        )
