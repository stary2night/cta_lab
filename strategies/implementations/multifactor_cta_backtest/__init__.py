"""China multi-factor CTA strategy package."""

from .config import MultiFactorCTAConfig, coerce_config
from .result import MultiFactorCTARunResult
from .strategy import MultiFactorCTAStrategy

__all__ = [
    "MultiFactorCTAConfig",
    "MultiFactorCTARunResult",
    "MultiFactorCTAStrategy",
    "coerce_config",
]
