"""PeriodicDecomposer：分周期绩效分解（年度 / 十年期 / 月度热图）。"""

from __future__ import annotations

from analysis.base import AnalysisContext, Decomposer, DecompositionResult
from analysis.metrics import annual_stats, decade_stats, monthly_pivot


class PeriodicDecomposer(Decomposer):
    """分周期绩效分解维度。

    需要
    ----
    context.result（含 .returns 日收益率序列）

    输出
    ----
    tables:
      annual_stats  — pd.DataFrame，年度绩效（index=Year）
      decade_stats  — pd.DataFrame，十年期分段（index=Period）
      monthly_pivot — pd.DataFrame，月度收益透视表（index=Year，单位 %）
    figures:
      annual_bar       — 年度收益柱状图
      monthly_heatmap  — 月度收益热图
    """

    name = "periodic"

    def __init__(self, decade_starts: list[int] | None = None) -> None:
        """
        Parameters
        ----------
        decade_starts:
            十年期各段起始年份，默认 [1995, 2000, 2005, 2010, 2015, 2020]。
        """
        self.decade_starts = decade_starts

    def compute(self, context: AnalysisContext) -> DecompositionResult:
        self._require(context, "result")
        pnl = context.result.returns

        ann   = annual_stats(pnl)
        dec   = decade_stats(pnl, starts=self.decade_starts)
        mpiv  = monthly_pivot(pnl)

        figures: dict = {}
        try:
            from analysis.report.charts import plot_annual_bar, plot_monthly_heatmap
            figures["annual_bar"]      = plot_annual_bar({"Strategy": ann})
            figures["monthly_heatmap"] = plot_monthly_heatmap(mpiv)
        except Exception:
            pass

        return DecompositionResult(
            name=self.name,
            tables={
                "annual_stats":  ann,
                "decade_stats":  dec,
                "monthly_pivot": mpiv,
            },
            figures=figures,
        )
