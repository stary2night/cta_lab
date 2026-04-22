"""JPM t-stat 国内期货趋势策略包。"""

from .config import JPMConfig, coerce_config, default_config
from .event_strategy import JPMEventDrivenConfig, JPMEventDrivenStrategy
from .strategy import JPMTrendStrategy, JPMRunResult

__all__ = [
    "JPMConfig",
    "JPMEventDrivenConfig",
    "JPMEventDrivenStrategy",
    "JPMTrendStrategy",
    "JPMRunResult",
    "coerce_config",
    "default_config",
]
