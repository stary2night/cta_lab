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
    plot_nav_with_drawdown,
    plot_annual_bar,
    plot_rolling_sharpe,
    plot_monthly_heatmap,
    plot_sector_nav,
)
from .strategy_report import StrategyReport
from .output import BacktestOutput

__all__ = [
    "plot_nav",
    "plot_performance_table",
    "plot_crisis_alpha",
    "plot_long_short",
    "plot_sector_heatmap",
    "plot_momentum_persistence",
    "plot_convexity",
    "plot_asset_contribution",
    "plot_nav_with_drawdown",
    "plot_annual_bar",
    "plot_rolling_sharpe",
    "plot_monthly_heatmap",
    "plot_sector_nav",
    "StrategyReport",
    "BacktestOutput",
]
