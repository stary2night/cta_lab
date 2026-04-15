"""SignalDecomposer：IC/Rank IC + 动量持续性检验。"""

from __future__ import annotations

from analysis.base import AnalysisContext, Decomposer, DecompositionResult
from analysis.signal.evaluator import evaluate_signal
from analysis.signal.persistence import momentum_persistence


class SignalDecomposer(Decomposer):
    """信号评估维度：IC / Rank IC / IR + 动量持续性。

    需要
    ----
    context.signal_df, context.returns_df

    输出
    ----
    tables:
      ic_summary       — pd.DataFrame，各 horizon 的 IC 均值 / 标准差 / IR
      persistence      — pd.DataFrame，各滞后期 beta / t_stat / R²
      ic_series        — dict，{horizon: IC 序列}（存入 tables["ic_series"]）
      rank_ic_series   — dict，同上，Rank IC
    figures:
      persistence — 动量持续性折线图
    """

    name = "signal"

    def __init__(
        self,
        horizons: tuple[int, ...] = (1, 5, 20, 60),
        max_lag: int = 12,
    ) -> None:
        """
        Parameters
        ----------
        horizons:
            IC 评估的前瞻期（交易日），默认 (1, 5, 20, 60)。
        max_lag:
            动量持续性最大滞后月数，默认 12。
        """
        self.horizons = horizons
        self.max_lag = max_lag

    def compute(self, context: AnalysisContext) -> DecompositionResult:
        self._require(context, "signal_df", "returns_df")

        # 前瞻收益用 returns_df 的 cumprod 重建价格
        import pandas as pd
        price_df = (1.0 + context.returns_df.fillna(0.0)).cumprod()

        eval_report = evaluate_signal(
            context.signal_df,
            prices=price_df,
            horizons=list(self.horizons),
        )

        persistence_df = momentum_persistence(context.returns_df, max_lag=self.max_lag)

        figures: dict = {}
        try:
            from analysis.report.charts import plot_momentum_persistence
            figures["persistence"] = plot_momentum_persistence(persistence_df)
        except Exception:
            pass

        return DecompositionResult(
            name=self.name,
            tables={
                "ic_summary": eval_report.summary,
                "persistence": persistence_df,
                "ic_series": eval_report.ic_series,           # type: ignore[dict-item]
                "rank_ic_series": eval_report.rank_ic_series, # type: ignore[dict-item]
            },
            figures=figures,
        )
