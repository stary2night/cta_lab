"""LongShortDecomposer：多空不对称分析。"""

from __future__ import annotations

from analysis.base import AnalysisContext, Decomposer, DecompositionResult
from analysis.signal.long_short import long_short_asymmetry


class LongShortDecomposer(Decomposer):
    """多空不对称分析维度。

    构建纯多头 / 纯空头 / 多空双向三条 NAV，对比多头贡献和空头贡献。

    需要
    ----
    context.returns_df, context.signal_df, context.vol_df

    输出
    ----
    tables:
      ls_nav — pd.DataFrame，columns=[long_only, short_only, long_short]
    figures:
      long_short — 三条 NAV 对比图
    """

    name = "long_short"

    def __init__(self, target_vol: float = 0.40) -> None:
        self.target_vol = target_vol

    def compute(self, context: AnalysisContext) -> DecompositionResult:
        self._require(context, "returns_df", "signal_df", "vol_df")

        ls_nav = long_short_asymmetry(
            context.returns_df,
            context.signal_df,
            context.vol_df,
            target_vol=self.target_vol,
        )

        figures: dict = {}
        try:
            from analysis.report.charts import plot_long_short
            figures["long_short"] = plot_long_short(ls_nav)
        except Exception:
            pass

        return DecompositionResult(
            name=self.name,
            tables={"ls_nav": ls_nav},
            figures=figures,
        )
