"""GMAT3 策略实现包。"""

from .data_access import GMAT3DataAccess
from .index_builder import GMAT3IndexBuilder
from .main_contract import MainContractEngine
from .roll_return import RollReturnCalculator
from .signals import SignalCalculator
from .sub_portfolio import SubPortfolioEngine
from .strategy import GMAT3RunResult, GMAT3Strategy
from .weights import WeightCalculator

__all__ = [
    "GMAT3DataAccess",
    "GMAT3IndexBuilder",
    "MainContractEngine",
    "GMAT3RunResult",
    "RollReturnCalculator",
    "SignalCalculator",
    "SubPortfolioEngine",
    "WeightCalculator",
    "GMAT3Strategy",
]
