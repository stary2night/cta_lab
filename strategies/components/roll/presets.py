"""Roll Strategy Layer 的轻量 preset / factory。"""

from __future__ import annotations

from typing import Callable

from .composer import SimpleLookThroughResolver, SingleContractValueComposer
from .profile import RollStrategyProfile
from .rules import (
    FieldMaxMarketStateRule,
    FixedDaysBeforeExpiryLifecycleRule,
    GMAT3DomesticCommodityMarketStateRule,
    HybridContractSelector,
    ImmediateRollExecutor,
    LinearRollExecutor,
    PreferSelectedContractSelector,
)
from .single_asset import SingleAssetRollStrategy


LifecycleBuilder = Callable[[RollStrategyProfile], object]
MarketStateBuilder = Callable[[RollStrategyProfile], object]
ExecutionBuilder = Callable[[RollStrategyProfile], object]
SelectorBuilder = Callable[[RollStrategyProfile], object]


def _build_lifecycle_fixed_days_before_expiry(profile: RollStrategyProfile):
    cfg = profile.lifecycle_rule_config
    assert cfg is not None
    return FixedDaysBeforeExpiryLifecycleRule(
        roll_days=int(cfg.params.get("roll_days", profile.roll_days)),
        date_field=str(cfg.params.get("date_field", profile.lifecycle_date_field)),
    )


def _build_market_state_field_max(profile: RollStrategyProfile):
    cfg = profile.market_state_rule_config
    assert cfg is not None
    return FieldMaxMarketStateRule(
        field_name=str(cfg.params.get("field_name", profile.market_state_field))
    )


def _build_market_state_gmat3_domestic_commodity(profile: RollStrategyProfile):
    return GMAT3DomesticCommodityMarketStateRule()


def _build_execution_immediate(profile: RollStrategyProfile):
    return ImmediateRollExecutor()


def _build_execution_linear(profile: RollStrategyProfile):
    cfg = profile.execution_rule_config
    assert cfg is not None
    return LinearRollExecutor(
        roll_days=int(cfg.params.get("roll_days", profile.roll_days))
    )


def _build_selector_hybrid(profile: RollStrategyProfile):
    return HybridContractSelector()


def _build_selector_prefer_selected(profile: RollStrategyProfile):
    return PreferSelectedContractSelector()


LIFECYCLE_RULE_BUILDERS: dict[str, LifecycleBuilder] = {
    "fixed_days_before_expiry": _build_lifecycle_fixed_days_before_expiry,
}

MARKET_STATE_RULE_BUILDERS: dict[str, MarketStateBuilder] = {
    "field_max": _build_market_state_field_max,
    "gmat3_domestic_commodity": _build_market_state_gmat3_domestic_commodity,
}

EXECUTION_RULE_BUILDERS: dict[str, ExecutionBuilder] = {
    "immediate": _build_execution_immediate,
    "linear": _build_execution_linear,
}

SELECTOR_RULE_BUILDERS: dict[str, SelectorBuilder] = {
    "hybrid": _build_selector_hybrid,
    "prefer_selected": _build_selector_prefer_selected,
}


def _build_lifecycle_rule(profile: RollStrategyProfile):
    cfg = profile.lifecycle_rule_config
    if cfg is None:
        raise ValueError("profile.lifecycle_rule_config must not be None")
    try:
        builder = LIFECYCLE_RULE_BUILDERS[cfg.kind]
    except KeyError as exc:
        raise ValueError(f"Unsupported lifecycle_rule_config.kind: {cfg.kind}") from exc
    return builder(profile)


def _build_market_state_rule(profile: RollStrategyProfile):
    cfg = profile.market_state_rule_config
    if cfg is None:
        raise ValueError("profile.market_state_rule_config must not be None")
    try:
        builder = MARKET_STATE_RULE_BUILDERS[cfg.kind]
    except KeyError as exc:
        raise ValueError(f"Unsupported market_state_rule_config.kind: {cfg.kind}") from exc
    return builder(profile)


def _build_execution_rule(profile: RollStrategyProfile):
    cfg = profile.execution_rule_config
    if cfg is None:
        raise ValueError("profile.execution_rule_config must not be None")
    try:
        builder = EXECUTION_RULE_BUILDERS[cfg.kind]
    except KeyError as exc:
        raise ValueError(f"Unsupported execution_rule_config.kind: {cfg.kind}") from exc
    return builder(profile)


def _build_selector_rule(profile: RollStrategyProfile):
    cfg = profile.selector_rule_config
    if cfg is None:
        raise ValueError("profile.selector_rule_config must not be None")
    try:
        builder = SELECTOR_RULE_BUILDERS[cfg.kind]
    except KeyError as exc:
        raise ValueError(f"Unsupported selector_rule_config.kind: {cfg.kind}") from exc
    return builder(profile)


def build_single_asset_strategy_from_profile(profile: RollStrategyProfile) -> SingleAssetRollStrategy:
    """从 profile 构造单资产 roll strategy。"""
    return SingleAssetRollStrategy(
        profile,
        lifecycle_rule=_build_lifecycle_rule(profile),
        market_state_rule=_build_market_state_rule(profile),
        selector=_build_selector_rule(profile),
        executor=_build_execution_rule(profile),
        composer=SingleContractValueComposer(),
        lookthrough_resolver=SimpleLookThroughResolver(),
    )
