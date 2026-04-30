"""China futures basis value (mean-reversion) strategy package."""

from .config import BasisValueConfig, coerce_config
from .result import BasisValueRunResult
from .strategy import BasisValueStrategy

__all__ = [
    "BasisValueConfig",
    "BasisValueRunResult",
    "BasisValueStrategy",
    "coerce_config",
]
