"""PerformanceDecomposer：绩效汇总 + 滚动指标。"""

from __future__ import annotations

import pandas as pd

from analysis.base import AnalysisContext, Decomposer, DecompositionResult
from analysis.metrics import performance_summary, rolling_metrics


class PerformanceDecomposer(Decomposer):
    """绩效分析维度。

    输出
    ----
    tables:
      summary   — pd.Series，performance_summary() 结果（Sharpe/MaxDD/…）
      rolling   — pd.DataFrame，滚动 Sharpe / Vol / MaxDD
    figures:
      nav         — NAV 曲线图
      performance — 绩效汇总表图
    """

    name = "performance"

    def __init__(self, window: int = 252, rf: float = 0.0) -> None:
        """
        Parameters
        ----------
        window:
            滚动窗口天数，默认 252。
        rf:
            无风险利率（年化），默认 0。
        """
        self.window = window
        self.rf = rf

    def compute(self, context: AnalysisContext) -> DecompositionResult:
        nav = context.result.nav

        summary_dict = performance_summary(nav, rf=self.rf)
        summary = pd.Series(summary_dict, name="value")
        rolling = rolling_metrics(nav, window=self.window)

        figures: dict = {}
        try:
            from analysis.report.charts import plot_nav, plot_performance_table
            figures["nav"] = plot_nav({"strategy": nav})
            figures["performance"] = plot_performance_table(summary_dict)
        except Exception:
            pass

        return DecompositionResult(
            name=self.name,
            tables={"summary": summary, "rolling": rolling},
            figures=figures,
        )
