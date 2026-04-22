"""WalkForwardEngine：滚动窗口样本外验证框架。

设计目标
--------
让每一次策略评估都有真正 OOS 的 PnL 序列，而不是全样本 in-sample。

两种运行模式
-----------
causal（默认，refit=False）
    strategy_fn 接收全量 returns_df，内部以因果滚动窗口计算信号/权重。
    WalkForwardEngine 仅切片各折 test 区间评估 OOS 性能。
    适用场景：JPM t-stat 等参数固定的滚动信号策略。

strict（refit=True）
    每折只把 train 数据传给 strategy_fn，strategy_fn 返回的权重必须
    覆盖 test 日期（例如最后一行权重持仓延续，或在 test 上重新计算因果信号）。
    适用场景：需要参数估计的策略（回归系数、机器学习模型等）。

用法示例
--------
>>> from backtest.walk_forward import WalkForwardEngine
>>> from backtest.vectorized import VectorizedBacktest
>>>
>>> def strategy_fn(returns_df):
...     strat = JPMTrendStrategy()
...     sigma = strat._compute_sigma(returns_df)
...     signal = strat.generate_signals_from_returns(returns_df)
...     return strat.build_weights(signal, sigma)
>>>
>>> engine = WalkForwardEngine(train_window=756, test_window=252)
>>> result = engine.run(strategy_fn, returns_df, verbose=True)
>>> print(result.summary())
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

import numpy as np
import pandas as pd

from .result import BacktestResult
from .vectorized import VectorizedBacktest


@dataclass
class WalkForwardFold:
    """单个折叠的边界信息。"""

    fold_idx: int
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp

    @property
    def train_days(self) -> int:
        """训练区间长度（index 行数，非日历天数）。"""
        return -1  # filled by engine when needed

    def __repr__(self) -> str:
        return (
            f"Fold({self.fold_idx}: "
            f"train={self.train_start.date()}~{self.train_end.date()}, "
            f"test={self.test_start.date()}~{self.test_end.date()})"
        )


@dataclass
class WalkForwardResult:
    """Walk-forward 验证的全部输出。"""

    folds: list[WalkForwardFold]
    oos_returns: pd.Series              # 拼接的 OOS 日收益率（各折 test 区间）
    oos_nav: pd.Series                  # OOS 累计净值（从 1.0 开始）
    fold_results: list[BacktestResult]  # 每折 BacktestResult
    metadata: dict = field(default_factory=dict)

    def summary(self) -> pd.DataFrame:
        """返回每折及总体的 Sharpe / AnnReturn / AnnVol / MaxDD 摘要表。

        Returns
        -------
        DataFrame，index=fold_idx（最后一行 index='overall'）。
        """
        trading_days = self.metadata.get("trading_days", 252)
        rows = []

        for fold, result in zip(self.folds, self.fold_results):
            r = result.returns.iloc[1:]  # 去掉起始 0.0
            rows.append(
                _metrics_row(
                    r,
                    label=str(fold.fold_idx),
                    train_start=fold.train_start,
                    train_end=fold.train_end,
                    test_start=fold.test_start,
                    test_end=fold.test_end,
                    trading_days=trading_days,
                )
            )

        # 总体 OOS
        oos_r = self.oos_returns
        rows.append(
            _metrics_row(
                oos_r,
                label="overall",
                train_start=pd.NaT,
                train_end=pd.NaT,
                test_start=oos_r.index[0] if not oos_r.empty else pd.NaT,
                test_end=oos_r.index[-1] if not oos_r.empty else pd.NaT,
                trading_days=trading_days,
            )
        )

        df = pd.DataFrame(rows).set_index("fold")
        return df


def _metrics_row(
    r: pd.Series,
    label: str,
    train_start: pd.Timestamp,
    train_end: pd.Timestamp,
    test_start: pd.Timestamp,
    test_end: pd.Timestamp,
    trading_days: int = 252,
) -> dict:
    if r.empty or r.std() == 0:
        return {
            "fold": label,
            "train_start": _date(train_start),
            "train_end": _date(train_end),
            "test_start": _date(test_start),
            "test_end": _date(test_end),
            "sharpe": float("nan"),
            "ann_return": float("nan"),
            "ann_vol": float("nan"),
            "max_dd": float("nan"),
            "n_days": len(r),
        }
    ann_r = r.mean() * trading_days
    ann_v = r.std() * np.sqrt(trading_days)
    sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
    nav = (1 + r).cumprod()
    mdd = float(((nav - nav.cummax()) / nav.cummax()).min())
    return {
        "fold": label,
        "train_start": _date(train_start),
        "train_end": _date(train_end),
        "test_start": _date(test_start),
        "test_end": _date(test_end),
        "sharpe": round(sharpe, 4),
        "ann_return": round(ann_r, 4),
        "ann_vol": round(ann_v, 4),
        "max_dd": round(mdd, 4),
        "n_days": len(r),
    }


def _date(ts: pd.Timestamp):
    return ts.date() if pd.notna(ts) else None


class WalkForwardEngine:
    """滚动窗口 Walk-forward / 样本外验证引擎。

    Parameters
    ----------
    train_window:
        训练窗口长度（交易日数）。
    test_window:
        每折测试窗口长度（交易日数）。
    step:
        两折之间的步进天数；None 时等于 test_window（各折 test 不重叠）。
    expanding:
        True → 扩展窗口（train 从 t=0 开始，逐折增长）；
        False（默认）→ 滑动固定长度训练窗口。
    trading_days:
        年交易日数，用于绩效年化，默认 252。
    """

    def __init__(
        self,
        train_window: int,
        test_window: int,
        step: Optional[int] = None,
        expanding: bool = False,
        trading_days: int = 252,
    ) -> None:
        if train_window <= 0:
            raise ValueError("train_window must be > 0")
        if test_window <= 0:
            raise ValueError("test_window must be > 0")
        self.train_window = train_window
        self.test_window = test_window
        self.step = step if step is not None else test_window
        self.expanding = expanding
        self.trading_days = trading_days

    # ── 折叠拆分 ─────────────────────────────────────────────────────────────

    def split(self, index: pd.DatetimeIndex) -> list[WalkForwardFold]:
        """按 index 生成所有折叠边界。

        Parameters
        ----------
        index:
            品种收益率矩阵的 DatetimeIndex（交易日序列）。

        Returns
        -------
        list[WalkForwardFold]，按时间顺序排列。
        """
        n = len(index)
        folds: list[WalkForwardFold] = []
        fold_idx = 0
        train_end_pos = self.train_window - 1

        while train_end_pos + self.test_window <= n - 1:
            test_start_pos = train_end_pos + 1
            test_end_pos = min(test_start_pos + self.test_window - 1, n - 1)

            train_start_pos = 0 if self.expanding else max(
                0, train_end_pos - self.train_window + 1
            )

            folds.append(
                WalkForwardFold(
                    fold_idx=fold_idx,
                    train_start=index[train_start_pos],
                    train_end=index[train_end_pos],
                    test_start=index[test_start_pos],
                    test_end=index[test_end_pos],
                )
            )
            train_end_pos += self.step
            fold_idx += 1

        return folds

    # ── 主入口 ───────────────────────────────────────────────────────────────

    def run(
        self,
        strategy_fn: Callable[[pd.DataFrame], pd.DataFrame],
        returns_df: pd.DataFrame,
        backtest: Optional[VectorizedBacktest] = None,
        refit: bool = False,
        verbose: bool = False,
    ) -> WalkForwardResult:
        """执行 walk-forward 验证。

        Parameters
        ----------
        strategy_fn:
            ``(returns_df: DataFrame) → weights_df: DataFrame``

            - refit=False：接收全量 returns_df，返回因果权重矩阵（全日期）。
              WalkForwardEngine 按折叠切片 test 区间的权重用于评估。
            - refit=True：每折只接收 train_returns，必须返回覆盖 test 日期的
              权重（例如 ffill 或在 test 上继续计算因果信号）。

        returns_df:
            品种日收益率矩阵，shape=(dates, symbols)。

        backtest:
            向量化回测器；None 时使用 lag=1、无 vol-targeting、
            trim_inactive=False 的默认配置。

        refit:
            False（默认）：全量一次性计算权重，按折叠切片 OOS 评估（causal 模式）。
            True：每折重新调用 strategy_fn(train_returns)（strict 模式）。

        verbose:
            True 时打印每折绩效摘要及总体 OOS 摘要。

        Returns
        -------
        WalkForwardResult
        """
        bt = backtest or VectorizedBacktest(lag=1, trim_inactive=False)
        folds = self.split(returns_df.index)

        if not folds:
            raise ValueError(
                f"No folds generated: need at least {self.train_window + self.test_window} "
                f"rows, got {len(returns_df)}. Reduce train/test windows."
            )

        # causal 模式：一次性全量计算
        weights_full: Optional[pd.DataFrame] = None
        if not refit:
            if verbose:
                print(f"[WalkForward] Computing weights on full dataset ({len(returns_df)} rows)...")
            weights_full = strategy_fn(returns_df)

        fold_results: list[BacktestResult] = []
        all_oos_rets: list[pd.Series] = []

        for fold in folds:
            if refit:
                train_rets = returns_df.loc[:fold.train_end]
                weights = strategy_fn(train_rets)
            else:
                weights = weights_full  # type: ignore[assignment]

            # 切片 test 区间
            test_idx = returns_df.loc[fold.test_start : fold.test_end].index
            w_test = weights.reindex(test_idx).fillna(0.0)
            r_test = returns_df.loc[fold.test_start : fold.test_end]

            result = bt.run(w_test, r_test)
            fold_results.append(result)

            # 去掉起始哨兵行（returns=0.0），拼接 OOS PnL
            oos_r = result.returns.iloc[1:]
            all_oos_rets.append(oos_r)

            if verbose:
                _print_fold(fold, oos_r, self.trading_days)

        # 拼接全 OOS 收益率（各折 test 区间应不重叠）
        oos_returns = pd.concat(all_oos_rets).sort_index()
        oos_returns.name = "oos_returns"

        # 构建 OOS NAV（前置 1.0 哨兵）
        oos_nav = (1.0 + oos_returns).cumprod()
        start_date = oos_returns.index[0] - pd.tseries.offsets.BDay(1)
        oos_nav = pd.concat([pd.Series([1.0], index=[start_date]), oos_nav])
        oos_nav.name = "oos_nav"

        if verbose:
            _print_overall(oos_returns, len(folds), self.trading_days)

        return WalkForwardResult(
            folds=folds,
            oos_returns=oos_returns,
            oos_nav=oos_nav,
            fold_results=fold_results,
            metadata={
                "train_window": self.train_window,
                "test_window": self.test_window,
                "step": self.step,
                "expanding": self.expanding,
                "refit": refit,
                "n_folds": len(folds),
                "trading_days": self.trading_days,
            },
        )


# ── 打印辅助 ──────────────────────────────────────────────────────────────────


def _print_fold(fold: WalkForwardFold, oos_r: pd.Series, trading_days: int) -> None:
    if oos_r.empty or oos_r.std() == 0:
        print(f"  Fold {fold.fold_idx}: test={fold.test_start.date()}~{fold.test_end.date()}  [empty]")
        return
    ann_r = oos_r.mean() * trading_days
    ann_v = oos_r.std() * np.sqrt(trading_days)
    sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
    print(
        f"  Fold {fold.fold_idx}: "
        f"train={fold.train_start.date()}~{fold.train_end.date()}  "
        f"test={fold.test_start.date()}~{fold.test_end.date()}  "
        f"Sharpe={sharpe:.3f}  Ret={ann_r*100:.1f}%  Vol={ann_v*100:.1f}%"
    )


def _print_overall(oos_r: pd.Series, n_folds: int, trading_days: int) -> None:
    if oos_r.empty:
        print("\n  [Overall OOS] No data.")
        return
    ann_r = oos_r.mean() * trading_days
    ann_v = oos_r.std() * np.sqrt(trading_days)
    sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
    nav = (1 + oos_r).cumprod()
    mdd = float(((nav - nav.cummax()) / nav.cummax()).min())
    print(
        f"\n  [Overall OOS | {n_folds} folds]  "
        f"Sharpe={sharpe:.3f}  Ret={ann_r*100:.1f}%  "
        f"Vol={ann_v*100:.1f}%  MaxDD={mdd*100:.1f}%  "
        f"({oos_r.index[0].date()} ~ {oos_r.index[-1].date()})"
    )
