"""China futures carry (roll yield) strategy package."""

from .config import CarryConfig, coerce_config
from .result import CarryRunResult
from .strategy import CarryStrategy

__all__ = [
    "CarryConfig",
    "CarryRunResult",
    "CarryStrategy",
    "coerce_config",
]
