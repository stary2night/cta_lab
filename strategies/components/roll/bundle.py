"""Bundle roll asset 的第一版最小实现。"""

from __future__ import annotations

from typing import Any

import pandas as pd

from .base import RollStrategyBase
from .profile import RollStrategyProfile
from .result import RollStrategyResult


class BundleRollStrategy(RollStrategyBase):
    """把多个 roll asset 组合成一个可分配 bundle 资产。"""

    def __init__(
        self,
        profile: RollStrategyProfile,
        *,
        component_strategies: dict[str, RollStrategyBase] | None = None,
    ) -> None:
        if profile.asset_mode != "bundle":
            raise ValueError("BundleRollStrategy requires a bundle-mode RollStrategyProfile")
        super().__init__(profile)
        self.component_strategies = component_strategies or {}

    def _load_component_results(
        self,
        *,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None,
        end: str | pd.Timestamp | None,
    ) -> dict[str, RollStrategyResult]:
        component_results = market_data.get("component_results")
        if isinstance(component_results, dict) and component_results:
            return component_results

        component_market_data = market_data.get("component_market_data", {})
        results: dict[str, RollStrategyResult] = {}
        for component in self.profile.components:
            strategy = self.component_strategies.get(component.component_key)
            if strategy is None:
                raise ValueError(
                    f"missing component strategy for {component.component_key}; "
                    "provide component_results or component_strategies"
                )
            results[component.component_key] = strategy.run(
                market_data=component_market_data.get(component.component_key, {}),
                start=start,
                end=end,
            )
        return results

    @staticmethod
    def _normalize_row_weights(weights: pd.DataFrame) -> pd.DataFrame:
        row_sum = weights.sum(axis=1).replace(0.0, pd.NA)
        normalized = weights.div(row_sum, axis=0)
        return normalized.fillna(0.0)

    def _apply_weight_constraints(self, weights: pd.DataFrame) -> pd.DataFrame:
        bundle_rule = self.profile.bundle_rule
        if bundle_rule is None or weights.empty:
            return weights

        adjusted = weights.copy()
        if bundle_rule.weight_min is not None:
            adjusted = adjusted.clip(lower=float(bundle_rule.weight_min))
        if bundle_rule.weight_max is not None:
            adjusted = adjusted.clip(upper=float(bundle_rule.weight_max))
        return self._normalize_row_weights(adjusted)

    @staticmethod
    def _rebalance_dates(index: pd.DatetimeIndex, frequency: str | None) -> pd.DatetimeIndex:
        if frequency is None or frequency in {"daily", "D"}:
            return index

        freq_map = {
            "weekly": "W",
            "monthly": "M",
            "annual": "Y",
        }
        if frequency not in freq_map:
            raise NotImplementedError(f"unsupported rebalance_frequency: {frequency}")

        periods = index.to_period(freq_map[frequency])
        first_dates = index.to_series().groupby(periods).min()
        return pd.DatetimeIndex(first_dates.tolist())

    def _apply_rebalance_and_smoothing(self, weights: pd.DataFrame) -> pd.DataFrame:
        bundle_rule = self.profile.bundle_rule
        if bundle_rule is None or weights.empty:
            return weights

        rebalance_dates = set(self._rebalance_dates(weights.index, bundle_rule.rebalance_frequency))
        if len(rebalance_dates) == len(weights.index) and (bundle_rule.smoothing_window or 0) <= 1:
            return weights

        target_weights = weights.copy()
        applied_rows: list[pd.Series] = []
        previous_applied: pd.Series | None = None
        current_target: pd.Series | None = None
        smoothing_window = max(int(bundle_rule.smoothing_window or 1), 1)
        step = 0

        for date in target_weights.index:
            if date in rebalance_dates or current_target is None:
                current_target = target_weights.loc[date]
                if previous_applied is None:
                    previous_applied = current_target.copy()
                    applied_rows.append(previous_applied.rename(date))
                    continue
                step = 1
            if previous_applied is None or current_target is None:
                previous_applied = target_weights.loc[date]
                applied_rows.append(previous_applied.rename(date))
                continue

            if date in rebalance_dates:
                ratio = min(step / smoothing_window, 1.0)
                applied = previous_applied * (1.0 - ratio) + current_target * ratio
                step += 1
            elif step > 1 and step <= smoothing_window:
                ratio = min(step / smoothing_window, 1.0)
                applied = previous_applied * (1.0 - ratio) + current_target * ratio
                step += 1
            else:
                applied = current_target.copy()
            applied_rows.append(applied.rename(date))
            previous_applied = applied

        applied_weights = pd.DataFrame(applied_rows)
        return self._normalize_row_weights(applied_weights)

    def _build_weight_frame(self, component_values: pd.DataFrame, market_data: dict[str, Any]) -> pd.DataFrame:
        if component_values.empty:
            return pd.DataFrame(index=component_values.index)

        bundle_rule = self.profile.bundle_rule
        if bundle_rule is None:
            raise ValueError("bundle strategy requires bundle_rule")

        if bundle_rule.weight_mode == "equal":
            base_weights = pd.Series(
                1.0 / len(component_values.columns),
                index=component_values.columns,
                dtype=float,
            )
            weights = pd.DataFrame(
                [base_weights.to_dict()] * len(component_values.index),
                index=component_values.index,
                columns=component_values.columns,
            ).astype(float)
        elif bundle_rule.weight_mode == "static":
            static_weights = bundle_rule.static_weights or {}
            base_weights = pd.Series(static_weights, dtype=float).reindex(component_values.columns).fillna(0.0)
            total = float(base_weights.sum())
            if total <= 0:
                raise ValueError("bundle static weights must sum to a positive value")
            base_weights = base_weights / total
            weights = pd.DataFrame(
                [base_weights.to_dict()] * len(component_values.index),
                index=component_values.index,
                columns=component_values.columns,
            ).astype(float)
        elif bundle_rule.weight_mode == "external":
            external = market_data.get("component_target_weights")
            if not isinstance(external, pd.DataFrame) or external.empty:
                raise ValueError("bundle external weight_mode requires market_data['component_target_weights']")
            weights = external.copy()
            weights.index = pd.to_datetime(weights.index)
            weights = weights.sort_index().reindex(component_values.index)
            weights = weights.reindex(columns=component_values.columns).ffill().fillna(0.0).astype(float)
        else:
            raise NotImplementedError(f"unsupported bundle weight_mode: {bundle_rule.weight_mode}")

        weights = self._apply_weight_constraints(weights)
        weights = self._apply_rebalance_and_smoothing(weights)
        return self._apply_weight_constraints(weights)

    def _bundle_sync_dates(
        self,
        schedule: pd.DataFrame,
        market_data: dict[str, Any],
    ) -> pd.DatetimeIndex:
        bundle_rule = self.profile.bundle_rule
        if bundle_rule is None or schedule.empty or bundle_rule.sync_mode == "none":
            return pd.DatetimeIndex([])

        trade_dates = pd.DatetimeIndex(pd.to_datetime(schedule["trade_date"]).sort_values().unique())
        if bundle_rule.sync_mode == "rebalance":
            frequency = bundle_rule.sync_frequency or bundle_rule.rebalance_frequency or "monthly"
            return self._rebalance_dates(trade_dates, frequency)
        if bundle_rule.sync_mode == "external_dates":
            raw_dates = market_data.get("bundle_sync_dates", [])
            return pd.DatetimeIndex(pd.to_datetime(list(raw_dates))).sort_values().unique()
        raise NotImplementedError(f"unsupported bundle sync_mode: {bundle_rule.sync_mode}")

    def _annotate_bundle_sync(self, schedule: pd.DataFrame, market_data: dict[str, Any]) -> pd.DataFrame:
        bundle_rule = self.profile.bundle_rule
        if bundle_rule is None or schedule.empty:
            return schedule

        annotated = schedule.copy()
        annotated["trade_date"] = pd.to_datetime(annotated["trade_date"])
        sync_dates = set(self._bundle_sync_dates(annotated, market_data))
        sync_components = bundle_rule.sync_components
        if sync_components is None:
            trigger_mask = annotated["trade_date"].isin(sync_dates)
            scope = "all"
            scope_components = "all"
        else:
            trigger_mask = annotated["trade_date"].isin(sync_dates) & annotated["component_key"].isin(sync_components)
            scope = "partial"
            scope_components = ",".join(sync_components)

        annotated["bundle_sync_mode"] = bundle_rule.sync_mode
        annotated["bundle_sync_trigger"] = trigger_mask.astype(bool)
        annotated["bundle_sync_scope"] = scope
        annotated["bundle_sync_components"] = scope_components
        trigger_dates = pd.Series(trigger_mask, index=annotated.index).groupby(annotated["trade_date"]).transform("max")
        annotated["bundle_sync_group"] = trigger_dates.astype(int).groupby(annotated["trade_date"]).transform("max").cumsum()
        return annotated

    def build_candidate_universe(
        self,
        *,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "component_key": [component.component_key for component in self.profile.components],
                "symbol": [component.symbol for component in self.profile.components],
                "contract_scope": [component.contract_scope for component in self.profile.components],
            }
        )

    def build_contract_plan(
        self,
        *,
        candidates: pd.DataFrame,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> dict[str, pd.DataFrame]:
        component_results = self._load_component_results(market_data=market_data, start=start, end=end)
        frames: list[pd.DataFrame] = []
        for component_key, result in component_results.items():
            frame = result.contract_plan.copy()
            if frame.empty:
                continue
            frame["component_key"] = component_key
            frames.append(frame)
        return {"contract_plan": pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()}

    def build_roll_schedule(
        self,
        *,
        contract_plan: pd.DataFrame,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        component_results = self._load_component_results(market_data=market_data, start=start, end=end)
        frames: list[pd.DataFrame] = []
        for component_key, result in component_results.items():
            frame = result.roll_schedule.copy()
            if frame.empty:
                continue
            frame["component_key"] = component_key
            frames.append(frame)
        schedule = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        return self._annotate_bundle_sync(schedule, market_data)

    def compose_value(
        self,
        *,
        schedule: pd.DataFrame,
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> dict[str, Any]:
        component_results = self._load_component_results(market_data=market_data, start=start, end=end)
        component_values = pd.DataFrame(
            {component_key: result.value_series for component_key, result in component_results.items()}
        ).sort_index()
        if component_values.empty:
            return {
                "value_series": pd.Series(dtype=float),
                "component_values": pd.DataFrame(),
                "component_weights": pd.DataFrame(),
                "metadata": {"asset_mode": "bundle", "component_count": 0},
            }

        component_weights = self._build_weight_frame(component_values, market_data)
        returns = component_values.pct_change().fillna(0.0)
        bundle_returns = (returns * component_weights).sum(axis=1)
        value_series = (1.0 + bundle_returns).cumprod()
        value_series.name = self.profile.asset_key
        return {
            "value_series": value_series,
            "roll_return": bundle_returns,
            "component_values": component_values,
            "component_weights": component_weights,
            "metadata": {
                "asset_mode": "bundle",
                "component_count": len(component_values.columns),
                "weight_mode": self.profile.bundle_rule.weight_mode if self.profile.bundle_rule else None,
                "sync_mode": self.profile.bundle_rule.sync_mode if self.profile.bundle_rule else None,
            },
        }

    def resolve_lookthrough(
        self,
        *,
        schedule: pd.DataFrame,
        composition_result: dict[str, Any],
        market_data: dict[str, Any],
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        component_results = self._load_component_results(market_data=market_data, start=start, end=end)
        component_weights = composition_result.get("component_weights", pd.DataFrame())
        frames: list[pd.DataFrame] = []
        for component_key, result in component_results.items():
            frame = result.lookthrough_book.copy()
            if frame.empty:
                continue
            frame["component_key"] = component_key
            if "trade_date" in frame.columns and component_key in component_weights.columns:
                weights = component_weights[[component_key]].reset_index().rename(
                    columns={"index": "trade_date", component_key: "bundle_weight"}
                )
                frame["trade_date"] = pd.to_datetime(frame["trade_date"])
                weights["trade_date"] = pd.to_datetime(weights["trade_date"])
                frame = frame.merge(weights, on="trade_date", how="left")
            frames.append(frame)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
