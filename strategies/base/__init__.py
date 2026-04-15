"""strategies/base 公共导出。"""

from .strategy import StrategyBase
from .trend import TrendFollowingStrategy
from .cross_sectional import CrossSectionalStrategy

__all__ = [
    "StrategyBase",
    "TrendFollowingStrategy",
    "CrossSectionalStrategy",
]
