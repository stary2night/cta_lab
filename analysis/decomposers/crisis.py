"""CrisisDecomposer：危机 Alpha + 凸性分析。"""

from __future__ import annotations

from analysis.base import AnalysisContext, Decomposer, DecompositionResult
from analysis.crisis.alpha import crisis_alpha_analysis, DEFAULT_CRISIS_EVENTS
from analysis.crisis.convexity import convexity_analysis


class CrisisDecomposer(Decomposer):
    """危机分析维度：危机 Alpha + 微笑曲线凸性。

    需要
    ----
    context.benchmark_returns

    输出
    ----
    tables:
      crisis_alpha — pd.DataFrame，各危机事件策略 vs 基准收益及 alpha
      convexity    — pd.DataFrame，基准收益分位数 vs 策略平均收益
    figures:
      crisis_alpha — 危机 alpha 柱状图
      convexity    — 微笑曲线图
    """

    name = "crisis"

    def __init__(
        self,
        crisis_events: dict | None = None,
        n_bins: int = 20,
    ) -> None:
        """
        Parameters
        ----------
        crisis_events:
            危机事件字典 {事件名: (start, end)}；None 使用默认事件表。
        n_bins:
            凸性分析分位数组数，默认 20。
        """
        self.crisis_events = crisis_events
        self.n_bins = n_bins

    def compute(self, context: AnalysisContext) -> DecompositionResult:
        self._require(context, "benchmark_returns")

        nav = context.result.nav
        strategy_returns = context.result.returns
        bm = context.benchmark_returns

        # 危机 Alpha
        crisis_df = crisis_alpha_analysis(
            nav, bm, crisis_events=self.crisis_events or DEFAULT_CRISIS_EVENTS
        )
        crisis_df = crisis_df.dropna(how="all")

        # 凸性分析
        conv_df = convexity_analysis(strategy_returns, bm, n_bins=self.n_bins)

        figures: dict = {}
        try:
            from analysis.report.charts import plot_crisis_alpha, plot_convexity
            if not crisis_df.empty:
                figures["crisis_alpha"] = plot_crisis_alpha(crisis_df)
            if not conv_df.empty:
                figures["convexity"] = plot_convexity(conv_df)
        except Exception:
            pass

        return DecompositionResult(
            name=self.name,
            tables={"crisis_alpha": crisis_df, "convexity": conv_df},
            figures=figures,
        )
