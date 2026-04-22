"""双动量策略回测包。

基于 Antonacci (2012/2016) "Risk Premia Harvesting Through Dual Momentum"，
适配中国期货市场：板块内相对动量 × 绝对动量过滤器。
"""

from typing import TYPE_CHECKING

from .config import DualMomentumConfig, coerce_config

if TYPE_CHECKING:
    from strategies.context import StrategyContext
    from .strategy import DualMomentumRunResult, DualMomentumStrategy

__all__ = [
    "DualMomentumConfig",
    "DualMomentumRunResult",
    "DualMomentumStrategy",
    "StrategyContext",
    "coerce_config",
]


def __getattr__(name: str):
    if name in {"DualMomentumRunResult", "DualMomentumStrategy"}:
        from .strategy import DualMomentumRunResult, DualMomentumStrategy

        return {
            "DualMomentumRunResult": DualMomentumRunResult,
            "DualMomentumStrategy": DualMomentumStrategy,
        }[name]
    if name == "StrategyContext":
        from strategies.context import StrategyContext

        return StrategyContext
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
