"""单个合约实例的属性与生命周期描述。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class Contract:
    """期货合约实例，包含完整生命周期属性。"""

    symbol: str           # 品种代码，如 "RB"
    code: str             # 合约代码，如 "RB2410"
    exchange: str         # 交易所
    list_date: date       # 上市日期
    expire_date: date     # 到期日期
    last_trade_date: date # 最后交易日

    def days_to_expiry(self, ref_date: date) -> int:
        """计算 ref_date 距最后交易日的自然日天数。"""
        return (self.last_trade_date - ref_date).days

    def is_active(self, ref_date: date) -> bool:
        """判断合约在 ref_date 是否处于活跃交易期间。"""
        return self.list_date <= ref_date <= self.last_trade_date

    def month_code(self) -> str:
        """返回合约月份代码，如 '2410'。"""
        return self.code[len(self.symbol):]
