"""报告生成子包。"""

from .charts import (
    plot_nav,
    plot_performance_table,
    plot_crisis_alpha,
    plot_long_short,
    plot_sector_heatmap,
    plot_momentum_persistence,
    plot_convexity,
    plot_asset_contribution,
)
from .strategy_report import StrategyReport

__all__ = [
    "plot_nav",
    "plot_performance_table",
    "plot_crisis_alpha",
    "plot_long_short",
    "plot_sector_heatmap",
    "plot_momentum_persistence",
    "plot_convexity",
    "plot_asset_contribution",
    "StrategyReport",
]
