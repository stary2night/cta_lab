"""GMAT3 bundle 到 Roll Strategy Layer 的结构映射。"""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd

from ...implementations.gmat3.universe import (
    BLACK_COMPONENTS,
    BLACK_REBALANCE_HISTORY_DAYS,
    BLACK_WEIGHT_WINDOW,
    BLACK_WEIGHT_MAX,
    BLACK_WEIGHT_MIN,
    SUB_PORTFOLIOS,
)
from .bundle import BundleRollStrategy
from .profile import BundleRule, RollComponentProfile, RollStrategyProfile
from .result import RollStrategyResult


def build_gmat3_black_bundle_profile() -> RollStrategyProfile:
    """把 GMAT3 BLACK 的结构性业务规则映射成 bundle profile。"""

    components = [
        RollComponentProfile(component_key=key.lower(), symbol=key, contract_scope="single_symbol")
        for key in BLACK_COMPONENTS
    ]
    return RollStrategyProfile(
        name="gmat3_black_bundle",
        asset_key="BLACK",
        asset_mode="bundle",
        currency=str(SUB_PORTFOLIOS["BLACK"]["currency"]),
        components=components,
        bundle_rule=BundleRule(
            weight_mode="external",
            weight_min=float(BLACK_WEIGHT_MIN),
            weight_max=float(BLACK_WEIGHT_MAX),
            rebalance_frequency="annual",
            smoothing_window=10,
            sync_mode="rebalance",
            sync_frequency="annual",
            sync_components=[component.component_key for component in components],
            metadata={"history_days": int(BLACK_REBALANCE_HISTORY_DAYS)},
        ),
        metadata={
            "source": "gmat3",
            "bundle_type": "black_series",
        },
    )


def build_black_external_weight_template(index: pd.DatetimeIndex) -> pd.DataFrame:
    """生成 BLACK bundle 外部动态权重模板。"""

    columns = [key.lower() for key in BLACK_COMPONENTS]
    if len(index) == 0:
        return pd.DataFrame(columns=columns, dtype=float)
    init = 1.0 / len(columns)
    return pd.DataFrame(init, index=pd.DatetimeIndex(index), columns=columns, dtype=float)


def _normalize_black_weights(
    eligible: dict[str, float],
    component_keys: list[str],
) -> dict[str, float]:
    total = sum(eligible.values())
    if total <= 0:
        n = max(len(component_keys), 1)
        return {key: 1.0 / n for key in component_keys}

    raw_weights = {key: value / total for key, value in eligible.items()}
    raw_weights = {key: weight for key, weight in raw_weights.items() if weight >= BLACK_WEIGHT_MIN}
    if not raw_weights:
        raw_weights = {key: 1.0 / len(eligible) for key in eligible}

    total2 = sum(raw_weights.values())
    normalized = {key: weight / total2 for key, weight in raw_weights.items()}

    for _ in range(20):
        over = {key: weight for key, weight in normalized.items() if weight > BLACK_WEIGHT_MAX}
        if not over:
            break
        excess = sum(weight - BLACK_WEIGHT_MAX for weight in over.values())
        under = {key: weight for key, weight in normalized.items() if weight <= BLACK_WEIGHT_MAX}
        if not under:
            n_over = len(over)
            normalized = {key: 1.0 / n_over for key in over}
            break
        under_total = sum(under.values())
        new_weights: dict[str, float] = {}
        for key, weight in normalized.items():
            if key in over:
                new_weights[key] = BLACK_WEIGHT_MAX
            else:
                new_weights[key] = weight + excess * (weight / under_total)
        normalized = new_weights

    total_final = sum(normalized.values()) or 1.0
    return {key: normalized.get(key, 0.0) / total_final for key in component_keys}


def _resolve_black_component_results(
    component_results: dict[str, RollStrategyResult],
) -> dict[str, RollStrategyResult]:
    required_keys = {key.lower(): key for key in BLACK_COMPONENTS}
    resolved: dict[str, RollStrategyResult] = {}
    for key, result in component_results.items():
        lower_key = str(key).lower()
        if lower_key in required_keys:
            resolved[lower_key] = result
    missing = sorted(set(required_keys) - set(resolved))
    if missing:
        raise ValueError(f"missing BLACK component results for: {missing}")
    return resolved


def compute_gmat3_black_component_target_weights(
    access,
    component_results: dict[str, RollStrategyResult],
) -> tuple[pd.DataFrame, pd.DatetimeIndex]:
    """基于 GMAT3 旧 BLACK 规则计算 bundle 外部动态目标权重。

    这里保持实现尽量简洁：
    - 直接消费已经算好的 component `value_series`
    - 用原始日线里的 `open_interest * settle_price` 作为 base weight source
    - 输出的是可直接喂给 `BundleRollStrategy(weight_mode="external")` 的日频权重
    """

    resolved = _resolve_black_component_results(component_results)
    component_values = pd.DataFrame(
        {key: result.value_series for key, result in resolved.items()}
    ).sort_index()
    if component_values.empty:
        return pd.DataFrame(columns=list(resolved)), pd.DatetimeIndex([])

    component_keys = list(component_values.columns)
    returns = component_values.pct_change().fillna(0.0)
    all_dates = pd.DatetimeIndex(component_values.index)

    ha_dict: dict[str, pd.Series] = {}
    for component_key in component_keys:
        symbol = component_key.upper()
        daily = access.get_daily(symbol)
        daily = daily[daily["settle_price"].notna() & daily["open_interest"].notna()].copy()
        daily["ha"] = daily["open_interest"] * daily["settle_price"]
        ha_series = daily.groupby("trade_date")["ha"].sum()
        ha_dict[component_key] = ha_series

    ha_df = pd.DataFrame(ha_dict).sort_index().reindex(all_dates).ffill()

    april_calc_dates: list[pd.Timestamp] = []
    april_count: dict[int, int] = {}
    for date in all_dates:
        if date.month != 4:
            continue
        year = date.year
        april_count[year] = april_count.get(year, 0) + 1
        if april_count[year] == 3:
            april_calc_dates.append(date)

    n_smooth = 10
    eq_weight = 1.0 / len(component_keys)
    current_drifted = {key: eq_weight for key in component_keys}
    old_weights = current_drifted.copy()
    target_weights: dict[str, float] | None = None
    transition_start_i: int | None = None
    calc_date_set = set(april_calc_dates)
    weight_rows: list[dict[str, float]] = []
    effective_rebalance_dates: list[pd.Timestamp] = []

    for i, date in enumerate(all_dates):
        if date in calc_date_set:
            hist_full = ha_df.loc[:date]
            if len(hist_full) >= BLACK_REBALANCE_HISTORY_DAYS:
                avg_ha = hist_full.iloc[-BLACK_WEIGHT_WINDOW:].mean()
                eligible = {
                    key: float(value)
                    for key, value in avg_ha.items()
                    if not np.isnan(value) and value > 0
                }
                if eligible:
                    target_weights = _normalize_black_weights(eligible, component_keys)
                    old_weights = current_drifted.copy()
                    transition_start_i = i
                    effective_rebalance_dates.append(date)

        if target_weights is not None and transition_start_i is not None:
            elapsed = i - transition_start_i
            if elapsed < n_smooth:
                alpha = (elapsed + 1) / n_smooth
                use_weights = {
                    key: (1.0 - alpha) * old_weights.get(key, 0.0)
                    + alpha * target_weights.get(key, 0.0)
                    for key in component_keys
                }
                current_drifted = use_weights.copy()
                if elapsed == n_smooth - 1:
                    current_drifted = target_weights.copy()
                    target_weights = None
            else:
                current_drifted = target_weights.copy()
                target_weights = None
                use_weights = current_drifted.copy()
        else:
            use_weights = current_drifted.copy()

        weight_rows.append({key: use_weights.get(key, 0.0) for key in component_keys})

        if target_weights is None and i + 1 < len(all_dates):
            rets_today = {
                key: float(returns.at[date, key]) if not pd.isna(returns.at[date, key]) else 0.0
                for key in component_keys
            }
            scale = sum(use_weights.get(key, 0.0) * (1.0 + rets_today[key]) for key in component_keys)
            if scale > 0:
                current_drifted = {
                    key: use_weights.get(key, 0.0) * (1.0 + rets_today[key]) / scale
                    for key in component_keys
                }

    weights_df = pd.DataFrame(weight_rows, index=all_dates)
    valid_mask = component_values.notna()
    weights_df = weights_df.reindex(columns=component_keys).fillna(0.0)
    weights_df = weights_df * valid_mask
    row_sum = weights_df.sum(axis=1).replace(0.0, np.nan)
    weights_df = weights_df.div(row_sum, axis=0).fillna(0.0)
    return weights_df.astype(float), pd.DatetimeIndex(effective_rebalance_dates)


def build_gmat3_black_bundle_market_data(
    access,
    component_results: dict[str, RollStrategyResult],
) -> dict[str, object]:
    """把 BLACK 真实动态权重计算结果封装成 bundle 可直接消费的 market_data。"""

    resolved = _resolve_black_component_results(component_results)
    target_weights, rebalance_dates = compute_gmat3_black_component_target_weights(access, resolved)
    return {
        "component_results": resolved,
        "component_target_weights": target_weights,
        "bundle_sync_dates": rebalance_dates,
    }


def run_gmat3_black_bundle(
    access,
    component_results: dict[str, RollStrategyResult],
    *,
    profile: RollStrategyProfile | None = None,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
) -> RollStrategyResult:
    """基于现有 roll 组件，尽量简洁地运行一个真实 BLACK bundle。"""

    base_profile = profile or build_gmat3_black_bundle_profile()
    if base_profile.bundle_rule is None:
        raise ValueError("BLACK bundle profile requires bundle_rule")

    runtime_bundle_rule = replace(
        base_profile.bundle_rule,
        rebalance_frequency=None,
        smoothing_window=None,
        sync_mode="external_dates",
        sync_frequency=None,
    )
    runtime_profile = replace(base_profile, bundle_rule=runtime_bundle_rule)
    strategy = BundleRollStrategy(runtime_profile)
    market_data = build_gmat3_black_bundle_market_data(access, component_results)
    return strategy.run(market_data=market_data, start=start, end=end)
