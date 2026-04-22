"""多子策略权重融合模块。

blend()：低层 API，将多个权重矩阵加权融合为单一权重矩阵。
StrategyBlender：高层 API，管理多个 (StrategyBase, weight) 对，
    一键生成融合权重并走 VectorizedBacktest，完成"研究单策略 → 组合多策略"闭环。

用法示例
--------
>>> from portfolio.blender import StrategyBlender
>>> from backtest.vectorized import VectorizedBacktest
>>>
>>> blender = StrategyBlender(vol_target=0.10)
>>> blender.add(jpm_strategy, blend_weight=0.6)
>>> blender.add(tsmom_strategy, blend_weight=0.4)
>>>
>>> result = blender.run(returns_df, verbose=True)
>>> result.nav.plot()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from backtest.result import BacktestResult
    from backtest.vectorized import VectorizedBacktest
    from strategies.base.strategy import StrategyBase


# ── 低层：权重矩阵融合 ────────────────────────────────────────────────────────


def blend(
    sub_weights: dict[int, pd.DataFrame],
    weights: list[float] | None = None,
) -> pd.DataFrame:
    """将多个子组合的权重矩阵加权融合为最终目标权重。

    Parameters
    ----------
    sub_weights:
        {任意整数 key → weight_df (dates × symbols)}
    weights:
        各子组合融合权重列表，None 表示等权。

    Returns
    -------
    融合后的 DataFrame(dates × symbols)，对齐日期取并集、缺失日 ffill。
    """
    keys = sorted(sub_weights.keys())
    n = len(keys)
    if n == 0:
        return pd.DataFrame()

    # 归一化融合权重
    if weights is None:
        w_list = [1.0 / n] * n
    else:
        total = sum(weights)
        w_list = [x / total for x in weights]

    # 取日期并集
    all_dates = pd.DatetimeIndex(
        sorted(set().union(*[set(sub_weights[k].index) for k in keys]))
    )

    # 取列并集（保序）
    all_symbols: list[str] = []
    seen: set[str] = set()
    for k in keys:
        for col in sub_weights[k].columns:
            if col not in seen:
                all_symbols.append(col)
                seen.add(col)

    blended = pd.DataFrame(0.0, index=all_dates, columns=all_symbols)
    for idx, k in enumerate(keys):
        source = sub_weights[k]
        first_date = source.index.min()
        last_date = source.index.max()
        df = source.reindex(index=all_dates, columns=all_symbols).ffill()
        valid_range = (df.index >= first_date) & (df.index <= last_date)
        df = df.mul(valid_range.astype(float), axis=0).fillna(0.0)
        blended = blended + df * w_list[idx]

    return blended


# ── 高层：StrategyBlender ─────────────────────────────────────────────────────


@dataclass
class _StrategyEntry:
    strategy: "StrategyBase"
    blend_weight: float
    run_kwargs: dict = field(default_factory=dict)


class StrategyBlender:
    """多策略组合管理器：统一调度多个 StrategyBase、融合权重、执行 VectorizedBacktest。

    Parameters
    ----------
    vol_target:
        组合目标年化波动率；None 时跳过 vol-targeting，默认 0.10。
    vol_halflife:
        vol-targeting EWMA 半衰期（交易日），默认 21。
    trading_days:
        年交易日数，默认 252。
    lag:
        执行延迟（T+lag），默认 1。
    """

    def __init__(
        self,
        vol_target: Optional[float] = 0.10,
        vol_halflife: int = 21,
        trading_days: int = 252,
        lag: int = 1,
    ) -> None:
        self.vol_target = vol_target
        self.vol_halflife = vol_halflife
        self.trading_days = trading_days
        self.lag = lag
        self._entries: list[_StrategyEntry] = []

    # ── 构建接口 ─────────────────────────────────────────────────────────────

    def add(
        self,
        strategy: "StrategyBase",
        blend_weight: float = 1.0,
        **run_kwargs,
    ) -> "StrategyBlender":
        """注册一个子策略。

        Parameters
        ----------
        strategy:
            StrategyBase 子类实例，需实现 run_vectorized()。
        blend_weight:
            该策略在组合中的相对权重（归一化前，支持任意正数）。
        **run_kwargs:
            透传给 strategy.run_vectorized() 的额外关键字参数
            （如 corr_cache、backtest 等）。

        Returns
        -------
        self（支持链式调用）。
        """
        if blend_weight <= 0:
            raise ValueError("blend_weight must be > 0")
        self._entries.append(
            _StrategyEntry(strategy=strategy, blend_weight=blend_weight, run_kwargs=run_kwargs)
        )
        return self

    # ── 执行接口 ─────────────────────────────────────────────────────────────

    def run(
        self,
        returns_df: pd.DataFrame,
        backtest: Optional["VectorizedBacktest"] = None,
        verbose: bool = False,
    ) -> "BacktestResult":
        """执行多策略组合回测。

        流程：
          1. 对每个子策略调用 run_vectorized(returns_df)，取 positions_df。
          2. 用 blend() 按 blend_weight 融合权重矩阵。
          3. 将融合后的权重矩阵传入 VectorizedBacktest 执行回测。

        Parameters
        ----------
        returns_df:
            品种日收益率矩阵，shape=(dates, symbols)。
        backtest:
            向量化回测器；None 时用 Blender 自身参数构造默认配置。
        verbose:
            True 时打印每个子策略的绩效摘要。

        Returns
        -------
        BacktestResult（与单策略接口相同）。
        """
        from backtest.result import BacktestResult
        from backtest.vectorized import VectorizedBacktest as _VBT

        if not self._entries:
            raise ValueError("No strategies registered. Call add() first.")

        bt = backtest or _VBT(
            lag=self.lag,
            vol_target=self.vol_target,
            vol_halflife=self.vol_halflife,
            trading_days=self.trading_days,
        )

        sub_weights: dict[int, pd.DataFrame] = {}
        blend_weights: list[float] = []

        for i, entry in enumerate(self._entries):
            sub_result = entry.strategy.run_vectorized(
                returns_df, backtest=_VBT(lag=1, trim_inactive=False), **entry.run_kwargs
            )
            # positions_df holds the lagged executed weights
            w = sub_result.positions_df
            if w is None or w.empty:
                if verbose:
                    print(f"  [strategy {i}] no positions — skipped")
                continue

            sub_weights[i] = w
            blend_weights.append(entry.blend_weight)

            if verbose:
                r = sub_result.returns.iloc[1:]
                _print_sub_summary(i, entry.strategy, r, self.trading_days)

        if not sub_weights:
            raise RuntimeError("All strategies produced empty positions.")

        blended_w = blend(sub_weights, blend_weights)

        if verbose:
            print(f"\n  Blended weight matrix: {blended_w.shape}")

        return bt.run(blended_w, returns_df)

    def blend_weights_only(
        self,
        returns_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """仅返回融合后的权重矩阵，不执行回测。

        用于调试或传入自定义 VectorizedBacktest。
        """
        from backtest.vectorized import VectorizedBacktest as _VBT

        sub_weights: dict[int, pd.DataFrame] = {}
        bw: list[float] = []

        for i, entry in enumerate(self._entries):
            sub_result = entry.strategy.run_vectorized(
                returns_df, backtest=_VBT(lag=1, trim_inactive=False), **entry.run_kwargs
            )
            w = sub_result.positions_df
            if w is not None and not w.empty:
                sub_weights[i] = w
                bw.append(entry.blend_weight)

        return blend(sub_weights, bw)


# ── 打印辅助 ──────────────────────────────────────────────────────────────────


def _print_sub_summary(
    idx: int, strategy: "StrategyBase", r: pd.Series, trading_days: int
) -> None:
    if r.empty or r.std() == 0:
        print(f"  [strategy {idx}] {type(strategy).__name__}: [empty]")
        return
    ann_r = r.mean() * trading_days
    ann_v = r.std() * np.sqrt(trading_days)
    sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
    print(
        f"  [strategy {idx}] {type(strategy).__name__}:  "
        f"Sharpe={sharpe:.3f}  Ret={ann_r*100:.1f}%  Vol={ann_v*100:.1f}%"
    )
