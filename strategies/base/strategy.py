"""策略基类：组装层，将 signals/ portfolio/ backtest/ 按策略逻辑组合。"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Optional

import numpy as np
import pandas as pd

from .vectorized import VectorizedStrategy

if TYPE_CHECKING:
    from backtest.engine import BacktestEngine
    from backtest.result import BacktestResult
    from backtest.vectorized import VectorizedBacktest


class StrategyBase(VectorizedStrategy):
    """策略基类：组装层，将 signals/ portfolio/ backtest/ 按策略逻辑组合。

    提供两条矩阵型兼容路径：
    - run()：旧 BacktestEngine 兼容路径，输入 price_df/adjust_dates 后
      生成权重矩阵，再交给 BacktestEngine 逐日推进持仓状态。
      这不是 callback/order/broker 风格的 backtest.event 事件驱动范式。
    - run_vectorized()：向量化路径，纯矩阵运算无 Python 循环，
      适合日度信号连续更新的 paper-portfolio 研究模拟。

    新的事件驱动策略应继承 EventDrivenStrategy，并通过
    run_event_backtest() 接入 EventDrivenBacktestEngine。
    """

    def __init__(self, config: dict) -> None:
        self.config = config

    @abstractmethod
    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """计算信号矩阵。输入价格矩阵(dates×symbols)，输出信号矩阵(dates×symbols)。"""

    @abstractmethod
    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: "dict | None" = None,
    ) -> pd.DataFrame:
        """计算目标权重矩阵。

        Parameters
        ----------
        signal_df:
            信号矩阵，shape=(dates, symbols)。
        vol_df:
            年化波动率矩阵，shape=(dates, symbols)。
        corr_cache:
            可选，{date: 相关性矩阵}；协方差感知型子类（如 JPMTrendStrategy）
            据此切换至 CorrCapSizer 路径，其余子类忽略此参数。
        """

    # ── 旧 BacktestEngine 兼容路径 ───────────────────────────────────────────

    def run(
        self,
        price_df: pd.DataFrame,
        adjust_dates: set[pd.Timestamp],
        engine: "BacktestEngine",
    ) -> "BacktestResult":
        """权重矩阵状态推进回测：价格 → 信号 → 权重 → BacktestEngine。

        这条路径保留给旧版 `BacktestEngine` 调用方式，适用场景是已经
        生成目标权重矩阵、但仍希望按 adjust_dates 逐日推进持仓状态的策略。
        它不同于 `backtest.event.EventDrivenBacktestEngine` 的
        callback/order/broker 事件驱动范式。
        """
        returns_df = price_df.pct_change()
        vol_df = returns_df.rolling(20).std() * np.sqrt(252)
        signal_df = self.generate_signals(price_df)
        weight_df = self.build_weights(signal_df, vol_df)
        return engine.run(weight_df, price_df, adjust_dates)

    # ── 向量化路径 ────────────────────────────────────────────────────────────

    def run_vectorized(
        self,
        returns_df: pd.DataFrame,
        backtest: Optional["VectorizedBacktest"] = None,
        vol_window: int = 20,
    ) -> "BacktestResult":
        """向量化回测：收益率 → 信号 → 权重 → VectorizedBacktest。

        适用场景：日度信号连续更新、无复杂持仓状态机的研究型策略。

        Parameters
        ----------
        returns_df : DataFrame, shape (dates, symbols)
            品种日收益率矩阵。
        backtest : VectorizedBacktest | None
            向量化回测器实例；为 None 时使用默认配置（lag=1，无 vol-targeting）。
        vol_window : int
            计算信号所需的滚动波动率窗口（交易日），默认 20。

        Returns
        -------
        BacktestResult
            与 run() 输出接口相同。

        Notes
        -----
        默认实现从收益率重建价格序列后调用 generate_signals()，
        对于信号内部已基于收益率计算的子类，建议重写此方法以避免
        价格→收益率→价格的往返转换。
        """
        from backtest.vectorized import VectorizedBacktest as _VBT

        # 重建合成价格（供 generate_signals 使用）
        # pct_change 可逆：(1+r).cumprod() → price，信号相对值不受影响
        price_df = (1.0 + returns_df.fillna(0.0)).cumprod()

        vol_df = returns_df.rolling(vol_window).std() * np.sqrt(252)
        signal_df = self.generate_signals(price_df)
        weight_df = self.build_weights(signal_df, vol_df)

        bt = backtest if backtest is not None else _VBT()
        return bt.run(weight_df, returns_df)

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "StrategyBase":
        """从 YAML 配置文件构造策略实例。"""
        import yaml

        with open(yaml_path) as f:
            config = yaml.safe_load(f)
        return cls(config)
