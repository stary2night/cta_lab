"""data.model — 领域对象模型层，re-export 所有公开类型。"""

from .instrument import Instrument, InstrumentRegistry
from .contract import Contract
from .bar import Bar, BarSeries
from .calendar import TradingCalendar, MultiExchangeCalendar
from .roll import RollRule, OIMaxRoll, VolumeMaxRoll, CalendarRoll, StabilizedRule, RollEvent, ContractSchedule
from .continuous import AdjustMethod, ContinuousSeries

__all__ = [
    # instrument
    "Instrument",
    "InstrumentRegistry",
    # contract
    "Contract",
    # bar
    "Bar",
    "BarSeries",
    # calendar
    "TradingCalendar",
    "MultiExchangeCalendar",
    # roll
    "RollRule",
    "OIMaxRoll",
    "VolumeMaxRoll",
    "CalendarRoll",
    "StabilizedRule",
    "RollEvent",
    "ContractSchedule",
    # continuous
    "AdjustMethod",
    "ContinuousSeries",
]
