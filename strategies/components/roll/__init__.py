"""Roll Strategy 通用组件骨架。"""

from .base import RollStrategyBase
from .bundle import BundleRollStrategy
from .composer import (
    LookThroughResolver,
    SimpleLookThroughResolver,
    SingleContractValueComposer,
    ValueComposer,
)
from .gmat3_adapter import GMAT3SingleAssetRollStrategy, build_gmat3_single_asset_market_data
from .gmat3_bundle_adapter import (
    build_black_external_weight_template,
    build_gmat3_black_bundle_market_data,
    build_gmat3_black_bundle_profile,
    compute_gmat3_black_component_target_weights,
    run_gmat3_black_bundle,
)
from .presets import build_single_asset_strategy_from_profile
from .profile import (
    BundleRule,
    ExecutionRuleConfig,
    LifecycleRuleConfig,
    MarketStateRuleConfig,
    RollComponentProfile,
    RollStrategyProfile,
    SelectorRuleConfig,
)
from .result import RollStrategyResult
from .rules import (
    ContractSelector,
    FieldMaxMarketStateRule,
    FixedDaysBeforeExpiryLifecycleRule,
    GMAT3DomesticCommodityMarketStateRule,
    HybridContractSelector,
    LifecycleRule,
    LinearRollExecutor,
    MarketStateRule,
    ImmediateRollExecutor,
    PreferSelectedContractSelector,
    RollExecutor,
)
from .single_asset import SingleAssetRollStrategy

__all__ = [
    "BundleRule",
    "BundleRollStrategy",
    "ContractSelector",
    "ExecutionRuleConfig",
    "FieldMaxMarketStateRule",
    "FixedDaysBeforeExpiryLifecycleRule",
    "GMAT3DomesticCommodityMarketStateRule",
    "GMAT3SingleAssetRollStrategy",
    "HybridContractSelector",
    "ImmediateRollExecutor",
    "LifecycleRuleConfig",
    "LifecycleRule",
    "LinearRollExecutor",
    "LookThroughResolver",
    "MarketStateRuleConfig",
    "MarketStateRule",
    "PreferSelectedContractSelector",
    "RollComponentProfile",
    "RollExecutor",
    "RollStrategyBase",
    "RollStrategyProfile",
    "RollStrategyResult",
    "SelectorRuleConfig",
    "SimpleLookThroughResolver",
    "SingleAssetRollStrategy",
    "SingleContractValueComposer",
    "ValueComposer",
    "build_black_external_weight_template",
    "build_gmat3_black_bundle_market_data",
    "build_gmat3_black_bundle_profile",
    "build_gmat3_single_asset_market_data",
    "compute_gmat3_black_component_target_weights",
    "run_gmat3_black_bundle",
    "build_single_asset_strategy_from_profile",
]
