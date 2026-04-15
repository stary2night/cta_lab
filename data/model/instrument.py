"""期货品种静态定义与注册管理。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Instrument:
    """期货品种的静态属性定义。"""

    symbol: str        # 品种代码，如 "RB"
    name: str          # 中文名，如 "螺纹钢"
    exchange: str      # 交易所，如 "SHFE"
    currency: str      # "CNY" 或 "USD"
    lot_size: float    # 每手合约乘数
    tick_size: float   # 最小报价单位
    margin_rate: float # 保证金比例


class InstrumentRegistry:
    """品种注册表单例，支持注册和多维度查询。"""

    _instance: InstrumentRegistry | None = None
    _instruments: dict[str, Instrument]

    def __new__(cls) -> "InstrumentRegistry":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._instruments = {}
        return cls._instance

    def register(self, inst: Instrument) -> None:
        """注册一个品种，已存在则覆盖。"""
        self._instruments[inst.symbol] = inst

    def get(self, symbol: str) -> Instrument:
        """按品种代码查询，不存在则抛出 KeyError。"""
        if symbol not in self._instruments:
            raise KeyError(f"Instrument '{symbol}' not found in registry.")
        return self._instruments[symbol]

    def list_all(self) -> list[Instrument]:
        """返回所有已注册品种列表。"""
        return list(self._instruments.values())

    def list_by_exchange(self, exchange: str) -> list[Instrument]:
        """返回指定交易所的所有品种。"""
        return [inst for inst in self._instruments.values() if inst.exchange == exchange]
