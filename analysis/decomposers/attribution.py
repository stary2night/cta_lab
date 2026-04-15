"""AttributionDecomposer：品种收益归因（全期 + 年度）。"""

from __future__ import annotations

from analysis.base import AnalysisContext, Decomposer, DecompositionResult
from analysis.attribution.asset import asset_contribution, annual_contribution


class AttributionDecomposer(Decomposer):
    """品种收益归因维度。

    需要
    ----
    context.returns_df, context.weights_df

    输出
    ----
    tables:
      total_contrib  — pd.Series，各品种全期贡献（降序）
      annual_contrib — pd.DataFrame，年度贡献矩阵（index=year, columns=symbol）
    figures:
      attribution — 品种归因热图
    """

    name = "attribution"

    def compute(self, context: AnalysisContext) -> DecompositionResult:
        self._require(context, "returns_df", "weights_df")

        total_contrib = asset_contribution(context.returns_df, context.weights_df)
        annual_contrib = annual_contribution(context.returns_df, context.weights_df)

        figures: dict = {}
        try:
            from analysis.report.charts import plot_asset_contribution
            figures["attribution"] = plot_asset_contribution(total_contrib, annual_contrib)
        except Exception:
            pass

        return DecompositionResult(
            name=self.name,
            tables={"total_contrib": total_contrib, "annual_contrib": annual_contrib},
            figures=figures,
        )
