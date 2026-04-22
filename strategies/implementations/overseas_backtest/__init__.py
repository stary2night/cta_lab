"""境外期货三策略对比回测包。

在 overseas_daily_full/ 数据上并行运行：
  - JPM t-stat 多周期趋势策略
  - TSMOM Binary 时序动量
  - Dual Momentum L/S 双动量

入口：
    python scripts/run_overseas.py --data-dir ...
"""

from .config import OverseasTrendSuiteConfig
from .strategy import OverseasTrendSuite, OverseasTrendSuiteResult

__all__ = [
    "OverseasTrendSuite",
    "OverseasTrendSuiteConfig",
    "OverseasTrendSuiteResult",
]
