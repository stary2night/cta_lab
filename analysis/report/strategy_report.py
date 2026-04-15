"""StrategyReport：基于 Decomposer 的策略分析报告编排器。

使用方式
--------
    from analysis.base import AnalysisContext
    from analysis.report.strategy_report import StrategyReport

    context = AnalysisContext(
        result=backtest_result,
        returns_df=returns,
        weights_df=weights,
        signal_df=signal,
        vol_df=sigma,
        sector_map={"RB": "Ferrous", ...},
        benchmark_returns=benchmark_series,
    )

    report = StrategyReport()                    # 使用默认 6 个 Decomposer
    results = report.run(context, output_dir="charts/")

    # 获取绩效汇总
    perf = results["performance"].tables["summary"]
    # 获取危机 alpha 表
    crisis = results["crisis"].tables["crisis_alpha"]

设计原则
--------
- 每个 Decomposer 独立运行，互不依赖。
- MissingContextError：context 数据不足时静默跳过该维度（非错误）。
- 其他 Exception：发出 warning，不中断整体流程。
- figures 自动保存到 output_dir（若指定）。
"""

from __future__ import annotations

import os
import warnings
from typing import TYPE_CHECKING

from analysis.base import (
    AnalysisContext,
    Decomposer,
    DecompositionResult,
    MissingContextError,
)
from analysis.decomposers import (
    PerformanceDecomposer,
    AttributionDecomposer,
    SectorDecomposer,
    CrisisDecomposer,
    SignalDecomposer,
    LongShortDecomposer,
)


def default_decomposers() -> list[Decomposer]:
    """构造默认的 6 个 Decomposer 列表（策略标准报告套件）。"""
    return [
        PerformanceDecomposer(),
        AttributionDecomposer(),
        SectorDecomposer(),
        CrisisDecomposer(),
        SignalDecomposer(),
        LongShortDecomposer(),
    ]


class StrategyReport:
    """策略分析报告编排器。

    将一组 Decomposer 顺序运行，统一收集 DecompositionResult。
    数据不足时优雅跳过（MissingContextError），非预期错误输出 warning。

    Parameters
    ----------
    decomposers:
        Decomposer 实例列表；None 时使用 default_decomposers()（6 个维度）。
    """

    def __init__(self, decomposers: list[Decomposer] | None = None) -> None:
        self.decomposers: list[Decomposer] = (
            decomposers if decomposers is not None else default_decomposers()
        )

    def run(
        self,
        context: AnalysisContext,
        output_dir: str | None = None,
        save_tables: bool = False,
    ) -> dict[str, DecompositionResult]:
        """运行所有 Decomposer，收集并返回结果。

        Parameters
        ----------
        context:
            统一分析输入容器（BacktestResult + 可选的各类面板数据）。
        output_dir:
            若不为 None，将所有 figures 保存为 PNG 到该目录。
            文件名格式：{decomposer_name}_{figure_name}.png。
        save_tables:
            True 时同步将 tables 保存为 CSV（与 figures 同目录）。

        Returns
        -------
        dict[str, DecompositionResult]
            key 为 decomposer.name，value 为对应的分析结果。
            只包含成功运行的维度。
        """
        results: dict[str, DecompositionResult] = {}

        for dec in self.decomposers:
            try:
                result = dec.compute(context)
                results[result.name] = result
            except MissingContextError:
                # context 数据不足，跳过该维度（预期情况）
                pass
            except Exception as exc:
                warnings.warn(
                    f"[StrategyReport] Decomposer '{dec.name}' 运行失败: {exc}",
                    stacklevel=2,
                )

        if output_dir is not None:
            self._save_outputs(results, output_dir, save_tables=save_tables)

        return results

    # ── 文件输出 ───────────────────────────────────────────────────────────────

    def _save_outputs(
        self,
        results: dict[str, DecompositionResult],
        output_dir: str,
        save_tables: bool = False,
    ) -> None:
        os.makedirs(output_dir, exist_ok=True)

        for name, res in results.items():
            # 保存图表
            for fig_name, fig in res.figures.items():
                try:
                    filepath = os.path.join(output_dir, f"{name}_{fig_name}.png")
                    fig.savefig(filepath, dpi=150, bbox_inches="tight")
                except Exception as exc:
                    warnings.warn(f"保存图表 {name}/{fig_name} 失败: {exc}", stacklevel=2)

            # 保存表格（可选）
            if save_tables:
                for tbl_name, tbl in res.tables.items():
                    import pandas as pd
                    if not isinstance(tbl, (pd.DataFrame, pd.Series)):
                        continue
                    try:
                        filepath = os.path.join(output_dir, f"{name}_{tbl_name}.csv")
                        tbl.to_csv(filepath)
                    except Exception:
                        pass

    # ── 便捷访问 ───────────────────────────────────────────────────────────────

    @staticmethod
    def summary_table(results: dict[str, DecompositionResult]) -> "dict[str, object]":
        """从结果集中提取关键指标，返回简洁的汇总字典。

        当前提取：performance.summary 中的核心指标。
        """
        import pandas as pd

        out: dict[str, object] = {}
        if "performance" in results:
            summary = results["performance"].tables.get("summary")
            if isinstance(summary, pd.Series):
                out.update(summary.to_dict())
        return out
