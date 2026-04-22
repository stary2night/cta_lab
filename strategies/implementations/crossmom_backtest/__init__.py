"""CrossMOM（相对动量/截面动量）策略回测包。"""

from strategies.context import StrategyContext

from .config import CrossMOMConfig, coerce_config
from .result import CrossMOMRunResult
from .strategy import CrossMOMStrategy

__all__ = [
    "CrossMOMConfig",
    "CrossMOMRunResult",
    "CrossMOMStrategy",
    "StrategyContext",
    "coerce_config",
]
