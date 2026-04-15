"""Roll Strategy Layer 的配置对象。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


AssetMode = Literal["single", "bundle"]


@dataclass(slots=True)
class LifecycleRuleConfig:
    """生命周期规则配置。"""

    kind: str = "fixed_days_before_expiry"
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MarketStateRuleConfig:
    """市场状态规则配置。"""

    kind: str = "field_max"
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionRuleConfig:
    """执行规则配置。"""

    kind: str = "immediate"
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SelectorRuleConfig:
    """目标合约选择器配置。"""

    kind: str = "hybrid"
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BundleRule:
    """定义 bundle 资产的组件构成和权重规则。"""

    weight_mode: str = "static"
    static_weights: dict[str, float] | None = None
    weight_min: float | None = None
    weight_max: float | None = None
    rebalance_frequency: str | None = None
    smoothing_window: int | None = None
    sync_mode: str = "none"
    sync_frequency: str | None = None
    sync_components: list[str] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RollComponentProfile:
    """描述 bundle 内的单个滚动组件。"""

    component_key: str
    symbol: str | None = None
    contract_scope: str = "single_symbol"
    lifecycle_rule: Any = None
    market_state_rule: Any = None
    execution_rule: Any = None
    substitute_rule: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RollStrategyProfile:
    """描述一个资产级 roll strategy。"""

    name: str
    asset_key: str
    asset_mode: AssetMode = "single"
    currency: str = "CNY"
    rule_profile: str = "default_single_asset"
    roll_days: int = 3
    lifecycle_date_field: str = "last_holding_date"
    market_state_field: str = "open_interest"
    lifecycle_rule_config: LifecycleRuleConfig | None = None
    market_state_rule_config: MarketStateRuleConfig | None = None
    execution_rule_config: ExecutionRuleConfig | None = None
    selector_rule_config: SelectorRuleConfig | None = None
    components: list[RollComponentProfile] = field(default_factory=list)
    bundle_rule: BundleRule | None = None
    lifecycle_rule: Any = None
    market_state_rule: Any = None
    execution_rule: Any = None
    value_rule: Any = None
    substitute_rule: Any = None
    lookthrough_rule: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.asset_mode == "single" and len(self.components) > 1:
            raise ValueError("single mode roll strategy cannot define multiple components")
        if self.asset_mode == "bundle" and not self.components:
            raise ValueError("bundle mode roll strategy must define at least one component")
        if self.asset_mode == "bundle" and self.bundle_rule is None:
            self.bundle_rule = BundleRule()
        if self.asset_mode == "single" and self.bundle_rule is not None:
            raise ValueError("single mode roll strategy cannot define bundle_rule")
        if self.roll_days <= 0:
            raise ValueError("roll_days must be positive")
        if self.asset_mode == "bundle" and self.bundle_rule is not None:
            if self.bundle_rule.weight_mode == "static" and not self.bundle_rule.static_weights:
                raise ValueError("bundle static weight mode requires static_weights")
            if self.bundle_rule.static_weights is not None:
                component_keys = {component.component_key for component in self.components}
                unknown = set(self.bundle_rule.static_weights) - component_keys
                if unknown:
                    raise ValueError(f"bundle_rule contains unknown components: {sorted(unknown)}")
            if self.bundle_rule.sync_mode not in {"none", "rebalance", "external_dates"}:
                raise ValueError(f"unsupported bundle sync_mode: {self.bundle_rule.sync_mode}")
            if self.bundle_rule.sync_components is not None:
                component_keys = {component.component_key for component in self.components}
                unknown = set(self.bundle_rule.sync_components) - component_keys
                if unknown:
                    raise ValueError(f"bundle_rule sync_components contain unknown components: {sorted(unknown)}")
        if self.lifecycle_rule_config is None:
            self.lifecycle_rule_config = LifecycleRuleConfig(
                params={"roll_days": self.roll_days, "date_field": self.lifecycle_date_field}
            )
        if self.market_state_rule_config is None:
            self.market_state_rule_config = MarketStateRuleConfig(
                params={"field_name": self.market_state_field}
            )
        if self.execution_rule_config is None:
            self.execution_rule_config = ExecutionRuleConfig(
                kind="immediate",
                params={},
            )
        if self.selector_rule_config is None:
            self.selector_rule_config = SelectorRuleConfig(
                kind="hybrid",
                params={},
            )
