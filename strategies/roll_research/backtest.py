"""展期规则对比评估。"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from data.model.bar import BarSeries
    from data.model.calendar import TradingCalendar
    from data.model.contract import Contract
    from data.model.roll import RollRule


def compare_roll_strategies(
    symbol: str,
    bar_data: dict[str, "BarSeries"],
    contracts: list["Contract"],
    rules: dict[str, "RollRule"],
    calendar: "TradingCalendar",
    start: str,
    end: str,
    adjust: str = "nav",
) -> pd.DataFrame:
    """对比不同展期规则的连续合约构建结果。

    返回：DataFrame，index=规则名，columns=[total_return, annual_vol, roll_count]
    """
    from data.model.continuous import AdjustMethod, ContinuousSeries

    results: dict[str, dict] = {}
    for name, rule in rules.items():
        try:
            cs = ContinuousSeries.build(
                symbol,
                bar_data,
                contracts,
                rule,
                AdjustMethod(adjust),
                calendar,
            )
            ret = cs.log_returns().loc[start:end]
            n = len(ret)
            total_ret = float(ret.sum())
            ann_vol = float(ret.std() * (252**0.5)) if n > 1 else 0.0
            roll_count = len(cs.schedule.events)
            results[name] = {
                "total_return": total_ret,
                "annual_vol": ann_vol,
                "roll_count": roll_count,
            }
        except Exception:
            results[name] = {
                "total_return": float("nan"),
                "annual_vol": float("nan"),
                "roll_count": 0,
            }

    return pd.DataFrame(results).T
