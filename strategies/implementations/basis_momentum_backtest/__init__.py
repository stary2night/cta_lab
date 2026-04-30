"""China futures basis momentum strategy package."""

from .config import BasisMomentumConfig, coerce_config
from .data_access import BasisMomentumDataAccess, BasisMomentumMarketData
from .result import BasisMomentumRunResult
from .strategy import BasisMomentumStrategy

__all__ = [
    "BasisMomentumConfig",
    "BasisMomentumDataAccess",
    "BasisMomentumMarketData",
    "BasisMomentumRunResult",
    "BasisMomentumStrategy",
    "coerce_config",
]
