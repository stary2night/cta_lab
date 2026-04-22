"""CrossMOM 策略结果对象。"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass
class CrossMOMRunResult:
    """CrossMOM 端到端运行结果。"""

    returns: pd.DataFrame
    signal: pd.DataFrame
    positions: pd.DataFrame
    pnl: pd.Series
    sigma: pd.DataFrame
    sector_map: dict[str, str]
    metadata: dict = field(default_factory=dict)
