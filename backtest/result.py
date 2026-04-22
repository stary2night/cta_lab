"""回测结果数据类。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd


@dataclass
class BacktestResult:
    """封装一次回测的全部输出。"""

    nav: pd.Series            # 每日 NAV，从 1.0 开始，DatetimeIndex
    returns: pd.Series        # 每日收益率（非对数）
    positions_df: Optional[pd.DataFrame] = field(default=None)   # 执行后持仓矩阵
    turnover_series: Optional[pd.Series] = field(default=None)   # 每日组合换手

    # verbose=True 时额外填充
    holdings_log: Optional[pd.DataFrame] = field(default=None)    # shape: (dates, symbols)，每日权重快照
    fee_log: Optional[pd.DataFrame] = field(default=None)         # shape: (dates,)，列: trading_fee / tracking_fee
    rebalance_log: Optional[pd.DataFrame] = field(default=None)   # 调仓记录

    def to_dict(self) -> dict:
        """返回 {nav, returns, ...} 字典，供 analysis 层使用。"""
        d: dict = {
            "nav": self.nav,
            "returns": self.returns,
        }
        if self.positions_df is not None:
            d["positions_df"] = self.positions_df
        if self.turnover_series is not None:
            d["turnover_series"] = self.turnover_series
        if self.holdings_log is not None:
            d["holdings_log"] = self.holdings_log
        if self.fee_log is not None:
            d["fee_log"] = self.fee_log
        if self.rebalance_log is not None:
            d["rebalance_log"] = self.rebalance_log
        return d
