"""分析层基类：AnalysisContext / DecompositionResult / Decomposer ABC。

设计原则
--------
- AnalysisContext：统一输入容器，所有字段均可选（None = 未提供）。
  Decomposer 通过 _require() 声明自己需要哪些字段，缺失时抛 MissingContextError
  而非静默跳过，让调用方清楚地知道哪些数据不足。

- DecompositionResult：统一输出容器，包含 tables（计算结果）和 figures（图表）。
  调用方可自行选择读取哪部分。

- Decomposer ABC：compute(context) 是唯一抽象方法，子类实现一个维度的分析。
  StrategyReport 作为编排器，持有 Decomposer 列表，按序运行并收集结果。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from backtest.result import BacktestResult


# ── 输入容器 ──────────────────────────────────────────────────────────────────

@dataclass
class AnalysisContext:
    """分析层统一输入容器。

    Attributes
    ----------
    result:
        回测结果（nav + returns），必填。
    returns_df:
        品种日收益率矩阵，shape=(dates, symbols)。
        归因、板块、信号评估、多空不对称等维度需要。
    weights_df:
        持仓权重矩阵，shape=(dates, symbols)。
        品种归因、板块分析需要。
    signal_df:
        信号矩阵，shape=(dates, symbols)。
        信号评估、多空不对称需要。
    vol_df:
        年化波动率矩阵，shape=(dates, symbols)。
        多空不对称需要。
    sector_map:
        品种到板块的映射，{symbol: sector_name}。
    benchmark_returns:
        基准日收益率序列（用于危机 alpha、凸性分析）。
    """

    result: "BacktestResult"
    returns_df: pd.DataFrame | None = None
    weights_df: pd.DataFrame | None = None
    signal_df: pd.DataFrame | None = None
    vol_df: pd.DataFrame | None = None
    sector_map: dict[str, str] | None = None
    benchmark_returns: pd.Series | None = None


# ── 输出容器 ──────────────────────────────────────────────────────────────────

@dataclass
class DecompositionResult:
    """Decomposer 统一输出容器。

    Attributes
    ----------
    name:
        分析维度名称（与 Decomposer.name 一致）。
    tables:
        计算结果表，{表名: DataFrame/Series}。
    figures:
        图表字典，{图名: matplotlib.figure.Figure}。
        不需要图表时为空 dict。
    """

    name: str
    tables: dict[str, pd.DataFrame | pd.Series] = field(default_factory=dict)
    figures: dict[str, Any] = field(default_factory=dict)   # Any = matplotlib Figure


# ── 异常 ─────────────────────────────────────────────────────────────────────

class MissingContextError(ValueError):
    """所需 AnalysisContext 字段缺失时抛出。

    由 StrategyReport 捕获后静默跳过（数据不足的维度不生成结果），
    其余异常仍会被上报，避免静默吞掉真实错误。
    """


# ── Decomposer 抽象基类 ───────────────────────────────────────────────────────

class Decomposer(ABC):
    """分析维度基类。

    每个子类代表一个独立的分析维度（绩效、归因、危机、信号等），
    通过 compute(context) 接收统一输入，返回 DecompositionResult。

    子类约定
    --------
    - 覆盖类属性 `name`（字符串）作为维度标识。
    - 在 compute() 开头调用 self._require(context, "field1", "field2", ...)
      显式声明所需字段；字段为 None 时自动抛 MissingContextError。
    - compute() 应返回完整的 tables 和 figures（图表生成失败可捕获后跳过）。
    """

    #: 维度名称，子类必须覆盖
    name: str = "base"

    def _require(self, context: AnalysisContext, *fields: str) -> None:
        """断言 context 中指定字段均不为 None。

        Parameters
        ----------
        context:
            当前分析上下文。
        *fields:
            需要非 None 的字段名列表。

        Raises
        ------
        MissingContextError
            若任意字段为 None。
        """
        missing = [f for f in fields if getattr(context, f, None) is None]
        if missing:
            raise MissingContextError(
                f"[{self.name}] 缺少必要的 context 字段: {missing}"
            )

    @abstractmethod
    def compute(self, context: AnalysisContext) -> DecompositionResult:
        """执行分析，返回结果。

        Parameters
        ----------
        context:
            统一输入容器。

        Returns
        -------
        DecompositionResult
            包含 tables（DataFrame/Series 字典）和 figures（Figure 字典）。

        Raises
        ------
        MissingContextError
            context 中缺少该 Decomposer 所需的字段时。
        """
