"""Roll Strategy Layer 的规则型组件。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import numpy as np
import pandas as pd


class LifecycleRule(ABC):
    """合约生命周期相关规则。"""

    @abstractmethod
    def evaluate(self, *, contracts: pd.DataFrame, date: pd.Timestamp, context: dict[str, Any]) -> dict[str, Any]:
        """评估给定日期下的 lifecycle 状态。"""


class MarketStateRule(ABC):
    """合约市场状态相关规则。"""

    @abstractmethod
    def evaluate(
        self,
        *,
        candidates: pd.DataFrame,
        date: pd.Timestamp,
        market_data: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        """对候选合约集合打分或排序。"""


class ContractSelector(ABC):
    """综合 lifecycle 与 market-state 结果，决定目标合约计划。"""

    @abstractmethod
    def select(
        self,
        *,
        date: pd.Timestamp,
        lifecycle_state: dict[str, Any],
        market_state: dict[str, Any],
        current_plan: Any,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        """输出该日期下的目标合约决策。"""


class RollExecutor(ABC):
    """把目标合约变化转成逐日执行路径。"""

    @abstractmethod
    def build_schedule(
        self,
        *,
        target_plan: pd.DataFrame,
        trading_calendar: pd.Index | pd.DatetimeIndex | list[pd.Timestamp],
        context: dict[str, Any],
    ) -> pd.DataFrame:
        """生成 roll execution schedule。"""


def _extract_daily_field(
    frame: pd.DataFrame | None,
    *,
    date: pd.Timestamp,
    field_name: str,
) -> pd.Series:
    """从宽表或长表里提取某日的字段截面。"""
    if frame is None or frame.empty:
        return pd.Series(dtype=float)

    if isinstance(frame.index, pd.DatetimeIndex):
        if date not in frame.index:
            return pd.Series(dtype=float)
        row = frame.loc[date]
        if isinstance(row, pd.Series):
            return pd.to_numeric(row, errors="coerce")
        return pd.Series(dtype=float)

    required = {"trade_date", "contract_id", field_name}
    if required.issubset(frame.columns):
        subset = frame.loc[pd.to_datetime(frame["trade_date"]) == date, ["contract_id", field_name]]
        if subset.empty:
            return pd.Series(dtype=float)
        return pd.to_numeric(subset.set_index("contract_id")[field_name], errors="coerce")

    return pd.Series(dtype=float)


class FixedDaysBeforeExpiryLifecycleRule(LifecycleRule):
    """在到期前固定若干交易日进入 roll window。"""

    def __init__(self, roll_days: int = 5, date_field: str = "last_holding_date") -> None:
        self.roll_days = int(roll_days)
        self.date_field = date_field

    def evaluate(self, *, contracts: pd.DataFrame, date: pd.Timestamp, context: dict[str, Any]) -> dict[str, Any]:
        current_contract = context.get("current_contract")
        trading_dates = pd.DatetimeIndex(pd.to_datetime(context.get("trading_dates", [])))
        current_valid = False
        limit_date = pd.NaT
        days_to_limit: int | None = None

        if current_contract is not None and not contracts.empty and "contract_id" in contracts.columns:
            row = contracts.loc[contracts["contract_id"] == current_contract]
            if not row.empty:
                row = row.iloc[0]
                limit_col = self.date_field if self.date_field in contracts.columns else "last_trade_date"
                limit_date = pd.to_datetime(row.get(limit_col))
                if pd.notna(limit_date):
                    current_valid = bool(limit_date >= date)
                    if len(trading_dates) > 0:
                        future = trading_dates[(trading_dates >= date) & (trading_dates <= limit_date)]
                        days_to_limit = max(len(future) - 1, 0)

        must_roll = current_contract is None or not current_valid
        may_roll = must_roll
        if days_to_limit is not None and days_to_limit <= self.roll_days:
            must_roll = True
            may_roll = True

        return {
            "current_contract": current_contract,
            "current_contract_valid": current_valid,
            "limit_date": limit_date,
            "days_to_limit": days_to_limit,
            "must_roll": must_roll,
            "may_roll": may_roll,
            "roll_window_days": self.roll_days,
        }


class FieldMaxMarketStateRule(MarketStateRule):
    """基于某个市场状态字段选择值最大的合约。"""

    def __init__(self, field_name: str = "open_interest", fallback: str = "nearest") -> None:
        self.field_name = field_name
        self.fallback = fallback

    def evaluate(
        self,
        *,
        candidates: pd.DataFrame,
        date: pd.Timestamp,
        market_data: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        if candidates.empty:
            return {
                "selected_contract": None,
                "scores": pd.DataFrame(columns=["trade_date", "contract_id", "score", "rank"]),
                "field_name": self.field_name,
            }

        daily = _extract_daily_field(market_data.get(self.field_name), date=date, field_name=self.field_name)
        scored = candidates.copy()
        scored["score"] = scored["contract_id"].map(daily).astype(float)
        scored["score"] = scored["score"].replace({np.nan: -np.inf})
        scored = scored.sort_values(["score", "last_trade_date"], ascending=[False, True]).reset_index(drop=True)
        scored["rank"] = range(1, len(scored) + 1)
        selected = scored.iloc[0]["contract_id"] if not scored.empty else None

        if selected is None and self.fallback == "nearest":
            selected = candidates.sort_values("last_trade_date").iloc[0]["contract_id"]

        return {
            "selected_contract": selected,
            "scores": pd.DataFrame(
                {
                    "trade_date": date,
                    "contract_id": scored["contract_id"],
                    "score": scored["score"],
                    "rank": scored["rank"],
                }
            ),
            "field_name": self.field_name,
        }


class HybridContractSelector(ContractSelector):
    """结合 lifecycle 和 market-state，给出稳定的目标合约。"""

    def select(
        self,
        *,
        date: pd.Timestamp,
        lifecycle_state: dict[str, Any],
        market_state: dict[str, Any],
        current_plan: Any,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        current_contract = None
        if isinstance(current_plan, dict):
            current_contract = current_plan.get("target_contract")
        elif current_plan is not None:
            current_contract = current_plan

        selected = market_state.get("selected_contract")
        must_roll = bool(lifecycle_state.get("must_roll", False))
        current_valid = bool(lifecycle_state.get("current_contract_valid", False))

        reason = "init"
        target_contract = current_contract
        if current_contract is None:
            target_contract = selected
            reason = "initialize_from_market_state"
        elif must_roll or not current_valid:
            target_contract = selected or current_contract
            reason = "lifecycle_forced_roll"
        elif selected is not None and selected != current_contract and bool(lifecycle_state.get("may_roll", False)):
            target_contract = selected
            reason = "market_state_switch"
        else:
            reason = "keep_current"

        return {
            "trade_date": date,
            "current_contract": current_contract,
            "target_contract": target_contract,
            "reason": reason,
            "must_roll": must_roll,
        }


class PreferSelectedContractSelector(ContractSelector):
    """更贴近 GMAT3 主力逻辑：优先使用 market-state 选出的目标合约。"""

    def select(
        self,
        *,
        date: pd.Timestamp,
        lifecycle_state: dict[str, Any],
        market_state: dict[str, Any],
        current_plan: Any,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        current_contract = None
        if isinstance(current_plan, dict):
            current_contract = current_plan.get("target_contract")
        elif current_plan is not None:
            current_contract = current_plan

        selected = market_state.get("selected_contract")
        must_roll = bool(lifecycle_state.get("must_roll", False))

        if current_contract is None:
            target_contract = selected
            reason = "initialize_from_market_state"
        elif selected is None or selected == current_contract:
            target_contract = current_contract
            reason = "keep_current"
        elif must_roll:
            target_contract = selected
            reason = "lifecycle_forced_roll"
        else:
            target_contract = selected
            reason = "market_state_switch"

        return {
            "trade_date": date,
            "current_contract": current_contract,
            "target_contract": target_contract,
            "reason": reason,
            "must_roll": must_roll,
        }


class ImmediateRollExecutor(RollExecutor):
    """最小实现：目标变化当日立即切换。"""

    def build_schedule(
        self,
        *,
        target_plan: pd.DataFrame,
        trading_calendar: pd.Index | pd.DatetimeIndex | list[pd.Timestamp],
        context: dict[str, Any],
    ) -> pd.DataFrame:
        if target_plan.empty:
            return pd.DataFrame(columns=["trade_date", "contract_id", "weight"])

        schedule = target_plan[["trade_date", "target_contract"]].copy()
        schedule = schedule.rename(columns={"target_contract": "contract_id"})
        schedule["weight"] = 1.0
        return schedule.reset_index(drop=True)


class LinearRollExecutor(RollExecutor):
    """按固定交易日窗口线性过渡 old/new 合约。"""

    def __init__(self, roll_days: int = 3) -> None:
        self.roll_days = max(int(roll_days), 1)

    def build_schedule(
        self,
        *,
        target_plan: pd.DataFrame,
        trading_calendar: pd.Index | pd.DatetimeIndex | list[pd.Timestamp],
        context: dict[str, Any],
    ) -> pd.DataFrame:
        columns = ["trade_date", "contract_id", "weight", "leg"]
        if target_plan.empty:
            return pd.DataFrame(columns=columns)

        calendar = pd.DatetimeIndex(pd.to_datetime(trading_calendar))
        if len(calendar) == 0:
            return pd.DataFrame(columns=columns)

        plan = target_plan.copy()
        plan["trade_date"] = pd.to_datetime(plan["trade_date"])
        plan = plan.sort_values("trade_date").reset_index(drop=True)
        plan = plan.set_index("trade_date")

        rows: list[dict[str, Any]] = []
        active_contract = None
        transition: dict[str, Any] | None = None

        for date in calendar:
            if date not in plan.index:
                if transition is not None:
                    rows.extend(self._transition_rows(date, transition))
                    transition = self._advance_transition(transition)
                    if transition is None:
                        active_contract = rows[-1]["contract_id"] if rows else active_contract
                elif active_contract is not None:
                    rows.append(
                        {
                            "trade_date": date,
                            "contract_id": active_contract,
                            "weight": 1.0,
                            "leg": "active",
                        }
                    )
                continue

            decision = plan.loc[date]
            if isinstance(decision, pd.DataFrame):
                decision = decision.iloc[-1]

            current_contract = decision.get("current_contract")
            target_contract = decision.get("target_contract")

            if active_contract is None:
                active_contract = target_contract or current_contract
                if active_contract is not None:
                    rows.append(
                        {
                            "trade_date": date,
                            "contract_id": active_contract,
                            "weight": 1.0,
                            "leg": "active",
                        }
                    )
                continue

            if transition is None and target_contract is not None and target_contract != active_contract:
                transition = {
                    "old_contract": active_contract,
                    "new_contract": target_contract,
                    "step": 0,
                    "total": self.roll_days,
                }

            if transition is not None:
                rows.extend(self._transition_rows(date, transition))
                transition = self._advance_transition(transition)
                if transition is None:
                    active_contract = target_contract
            elif active_contract is not None:
                rows.append(
                    {
                        "trade_date": date,
                        "contract_id": active_contract,
                        "weight": 1.0,
                        "leg": "active",
                    }
                )

        return pd.DataFrame(rows, columns=columns)

    def _transition_rows(self, date: pd.Timestamp, transition: dict[str, Any]) -> list[dict[str, Any]]:
        old_contract = transition["old_contract"]
        new_contract = transition["new_contract"]
        step = int(transition["step"])
        total = int(transition["total"])

        if total <= 1:
            return [
                {"trade_date": date, "contract_id": new_contract, "weight": 1.0, "leg": "new"},
            ]

        if step == 0:
            old_weight = 1.0
            new_weight = 0.0
        else:
            old_weight = max((total - step) / total, 0.0)
            new_weight = min(step / total, 1.0)

        rows = []
        if old_contract is not None and old_weight > 0:
            rows.append(
                {
                    "trade_date": date,
                    "contract_id": old_contract,
                    "weight": float(old_weight),
                    "leg": "old",
                }
            )
        if new_contract is not None and new_weight > 0:
            rows.append(
                {
                    "trade_date": date,
                    "contract_id": new_contract,
                    "weight": float(new_weight),
                    "leg": "new",
                }
            )
        return rows

    def _advance_transition(self, transition: dict[str, Any]) -> dict[str, Any] | None:
        step = int(transition["step"]) + 1
        total = int(transition["total"])
        if step > total:
            return None
        if step == total:
            return None
        updated = dict(transition)
        updated["step"] = step
        return updated


class GMAT3DomesticCommodityMarketStateRule(MarketStateRule):
    """贴近 GMAT3 国内商品主力逻辑的 market-state rule。"""

    def evaluate(
        self,
        *,
        candidates: pd.DataFrame,
        date: pd.Timestamp,
        market_data: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        if candidates.empty:
            return {
                "selected_contract": None,
                "scores": pd.DataFrame(columns=["trade_date", "contract_id", "score", "rank"]),
                "field_name": "open_interest",
            }

        current_contract = context.get("current_contract")
        lifecycle_state = context.get("lifecycle_state", {})
        daily_oi = market_data.get("open_interest")
        prev_scores = pd.Series(dtype=float)

        if isinstance(daily_oi, pd.DataFrame) and not daily_oi.empty:
            idx = pd.DatetimeIndex(daily_oi.index)
            prev_dates = idx[idx < date]
            if len(prev_dates) > 0:
                prev_scores = pd.to_numeric(daily_oi.loc[prev_dates[-1]], errors="coerce")
            elif date in idx:
                prev_scores = pd.to_numeric(daily_oi.loc[date], errors="coerce")

        filtered = candidates.copy()
        filtered["score"] = filtered["contract_id"].map(prev_scores).astype(float)
        filtered["score"] = filtered["score"].replace({np.nan: -np.inf})

        current_ltd = None
        if current_contract is not None and "contract_id" in filtered.columns and "last_trade_date" in candidates.columns:
            row = candidates.loc[candidates["contract_id"] == current_contract]
            if not row.empty:
                current_ltd = pd.to_datetime(row.iloc[0]["last_trade_date"])

        days_left = lifecycle_state.get("days_to_limit")
        roll_days = lifecycle_state.get("roll_window_days")
        strict_gt = current_contract is not None and days_left is not None and roll_days is not None and days_left <= roll_days

        if current_ltd is not None and "last_trade_date" in filtered.columns:
            filtered["last_trade_date"] = pd.to_datetime(filtered["last_trade_date"])
            if strict_gt:
                filtered = filtered[filtered["last_trade_date"] > current_ltd]
            else:
                filtered = filtered[filtered["last_trade_date"] >= current_ltd]
            if filtered.empty:
                filtered = candidates.copy()
                filtered["score"] = filtered["contract_id"].map(prev_scores).astype(float)
                filtered["score"] = filtered["score"].replace({np.nan: -np.inf})

        filtered = filtered.sort_values(["score", "last_trade_date"], ascending=[False, True]).reset_index(drop=True)
        filtered["rank"] = range(1, len(filtered) + 1)
        selected = filtered.iloc[0]["contract_id"] if not filtered.empty else current_contract

        return {
            "selected_contract": selected,
            "scores": pd.DataFrame(
                {
                    "trade_date": date,
                    "contract_id": filtered["contract_id"],
                    "score": filtered["score"],
                    "rank": filtered["rank"],
                }
            ),
            "field_name": "open_interest",
        }
