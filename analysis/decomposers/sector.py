"""SectorDecomposer：板块绩效分析。"""

from __future__ import annotations

from analysis.base import AnalysisContext, Decomposer, DecompositionResult
from analysis.attribution.sector import sector_performance


class SectorDecomposer(Decomposer):
    """板块绩效分析维度。

    需要
    ----
    context.returns_df, context.sector_map

    输出
    ----
    tables:
      sector_perf — pd.DataFrame，各板块绩效指标（index=sector，columns=Sharpe/MaxDD/…）
    figures:
      sector — 板块绩效热图
    """

    name = "sector"

    def compute(self, context: AnalysisContext) -> DecompositionResult:
        self._require(context, "returns_df", "sector_map")

        sector_df = sector_performance(
            context.returns_df,
            context.sector_map,
            weights_df=context.weights_df,  # 有则加权，无则等权
        )

        figures: dict = {}
        if not sector_df.empty:
            try:
                from analysis.report.charts import plot_sector_heatmap
                figures["sector"] = plot_sector_heatmap(sector_df)
            except Exception:
                pass

        return DecompositionResult(
            name=self.name,
            tables={"sector_perf": sector_df},
            figures=figures,
        )
