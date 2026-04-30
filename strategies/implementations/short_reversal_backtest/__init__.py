"""China futures short-term reversal strategy package."""

from .config import ShortReversalConfig, coerce_config
from .result import ShortReversalRunResult
from .strategy import ShortReversalStrategy

__all__ = [
    "ShortReversalConfig",
    "ShortReversalRunResult",
    "ShortReversalStrategy",
    "coerce_config",
]
