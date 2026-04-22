"""Analysis 分析层。

提供绩效指标计算、归因分析、危机分析、信号分析、费用分解和报告生成功能。

Decomposer 架构（推荐使用）
---------------------------
    from analysis.base import AnalysisContext
    from analysis.report.strategy_report import StrategyReport

    context = AnalysisContext(result=..., returns_df=..., ...)
    results = StrategyReport().run(context, output_dir="charts/")

扁平函数（向后兼容）
--------------------
所有原有函数仍可直接 import 使用。
"""

# ── 扁平函数（向后兼容）──────────────────────────────────────────────────────
from .metrics import (
    performance_summary,
    rolling_metrics,
    underwater_series,
    pnl_stats,
    annual_stats,
    decade_stats,
    monthly_pivot,
    sector_stats,
    asset_stats,
)
from .attribution import asset_contribution, annual_contribution, sector_performance
from .crisis import crisis_alpha_analysis, DEFAULT_CRISIS_EVENTS, convexity_analysis
from .signal import (
    forward_return,
    forward_log_return,
    build_forward_returns,
    SignalEvaluationReport,
    information_coefficient,
    information_ratio,
    evaluate_signal,
    momentum_persistence,
    long_short_asymmetry,
    correlation_analysis,
)
from .cost import fee_decomposition
from .report import (
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
    StrategyReport,
)

# ── Decomposer 架构 ───────────────────────────────────────────────────────────
from .base import (
    AnalysisContext,
    DecompositionResult,
    Decomposer,
    MissingContextError,
)
from .decomposers import (
    PerformanceDecomposer,
    AttributionDecomposer,
    SectorDecomposer,
    CrisisDecomposer,
    SignalDecomposer,
    LongShortDecomposer,
    PeriodicDecomposer,
)

__all__ = [
    # metrics
    "performance_summary",
    "rolling_metrics",
    "underwater_series",
    "pnl_stats",
    "annual_stats",
    "decade_stats",
    "monthly_pivot",
    "sector_stats",
    "asset_stats",
    # attribution
    "asset_contribution",
    "annual_contribution",
    "sector_performance",
    # crisis
    "crisis_alpha_analysis",
    "DEFAULT_CRISIS_EVENTS",
    "convexity_analysis",
    # signal
    "forward_return",
    "forward_log_return",
    "build_forward_returns",
    "SignalEvaluationReport",
    "information_coefficient",
    "information_ratio",
    "evaluate_signal",
    "momentum_persistence",
    "long_short_asymmetry",
    "correlation_analysis",
    # cost
    "fee_decomposition",
    # report
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
    # Decomposer 架构
    "AnalysisContext",
    "DecompositionResult",
    "Decomposer",
    "MissingContextError",
    "PerformanceDecomposer",
    "AttributionDecomposer",
    "SectorDecomposer",
    "CrisisDecomposer",
    "SignalDecomposer",
    "LongShortDecomposer",
    "PeriodicDecomposer",
]
