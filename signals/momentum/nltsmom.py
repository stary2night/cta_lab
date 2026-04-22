"""非线性时序动量信号（Nonlinear TSMOM）。

参考：Moskowitz, Sabbatucci, Tamoni & Uhl (2024)
      "Nonlinear Time Series Momentum"

核心非线性权重函数（Ferson & Siegel, 2001 推导）：

    s_t  = Σ log(1 + r_{t-i})  i=1..lookback     # 累计对数收益
    z_t  = s_t / σ_t                               # vol 标准化信号
    f(z) = z / (z² + 1)                            # FS 非线性变换，值域 (-0.5, 0.5)

性质：
    - |z| < 1 时近线性（弱信号延续性高）
    - |z| > 1 时权重回滚（极端信号不可持续，自动缩减头寸）
    - f(-z) = -f(z)（对称），f(0) = 0

三种变体对比（均通过 SignalMode 枚举选择）：
    binary  : sign(s_t)               → {-1, 0, +1}  （传统 TSMOM）
    linear  : z_t = s_t / σ_t         → ℝ            （线性 vol-scaled）
    nonlinear: f(z_t) = z_t/(z_t²+1)  → (-0.5, 0.5) （FS 非线性，本论文）

头寸（传入 VectorizedBacktest 之前）：
    weight = signal_variant / σ_t
"""

from __future__ import annotations

from enum import Enum

import numpy as np
import pandas as pd

from signals.base import CrossSectionalSignal


class SignalMode(str, Enum):
    """TSMOM 信号变体。"""

    BINARY = "binary"          # 传统：sign(cum_log_ret)
    LINEAR = "linear"          # 线性：cum_log_ret / sigma
    NONLINEAR = "nonlinear"    # FS 非线性：z / (z² + 1)，z = cum_log_ret / sigma


def _fs_nonlinear(z: np.ndarray | pd.DataFrame) -> np.ndarray | pd.DataFrame:
    """Ferson-Siegel 非线性变换 f(z) = z / (z² + 1)。

    对 DataFrame 和 ndarray 均适用。
    """
    return z / (z ** 2 + 1)


class NLTSMOMSignal(CrossSectionalSignal):
    """三模式时序动量信号：binary / linear / nonlinear(FS)。

    Parameters
    ----------
    lookback:
        累计对数收益的回望窗口（交易日），默认 252（≈12 个月）。
    sigma_halflife:
        EWMA 波动率估计的半衰期（交易日），默认 60。
    mode:
        信号模式：'binary'（传统）/'linear'/'nonlinear'（FS，默认）。
    min_periods:
        rolling 窗口最少有效天数，默认等于 lookback。

    Outputs
    -------
    compute() 返回**原始信号矩阵**（未除以 sigma），形状 (dates, symbols)。
    compute_weights() 返回已除以 sigma 的权重矩阵，可直接送入 VectorizedBacktest。
    """

    def __init__(
        self,
        lookback: int = 252,
        sigma_halflife: int = 60,
        mode: SignalMode | str = SignalMode.NONLINEAR,
        min_periods: int | None = None,
        trading_days: int = 252,
    ) -> None:
        self.lookback = lookback
        self.sigma_halflife = sigma_halflife
        self.mode = SignalMode(mode)
        self.min_periods = min_periods if min_periods is not None else max(lookback // 2, 1)
        self.trading_days = trading_days

    # ── 内部辅助 ──────────────────────────────────────────────────────────────

    def _log_ret(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """收益率 → 对数收益率（log(1+r)）。"""
        return np.log1p(returns_df)

    def _sigma(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """EWMA 年化波动率矩阵。"""
        return (
            returns_df.ewm(halflife=self.sigma_halflife, min_periods=30).std()
            * np.sqrt(self.trading_days)
        )

    def _cum_log_ret(self, log_ret: pd.DataFrame) -> pd.DataFrame:
        """滚动 lookback 窗口内累计对数收益。"""
        return log_ret.rolling(self.lookback, min_periods=self.min_periods).sum()

    # ── CrossSectionalSignal 接口 ─────────────────────────────────────────────

    def compute(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """计算原始信号矩阵（shape = (dates, symbols)）。

        binary    → {-1, 0, +1}
        linear    → z = cum_log_ret / sigma   (unbounded)
        nonlinear → f(z) = z / (z²+1)         ∈ (-0.5, 0.5)

        Parameters
        ----------
        returns_df:
            日收益率宽表，shape=(dates, symbols)。
        """
        log_ret = self._log_ret(returns_df)
        cum = self._cum_log_ret(log_ret)
        sigma = self._sigma(returns_df)

        if self.mode == SignalMode.BINARY:
            return np.sign(cum)

        z = cum / sigma.replace(0, np.nan)

        if self.mode == SignalMode.LINEAR:
            return z

        # NONLINEAR (FS)
        return _fs_nonlinear(z)

    def compute_weights(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """计算持仓权重矩阵（信号 / sigma），可直接送入 VectorizedBacktest。

        对三种模式均施加 w = signal / sigma 标准化，使单品种
        头寸随波动率自动缩放，与 JPM baseline 定仓规范一致。

        Returns
        -------
        shape=(dates, symbols)，已填充 0（NaN→0）。
        """
        signal = self.compute(returns_df)
        sigma = self._sigma(returns_df).replace(0, np.nan)
        return (signal / sigma).fillna(0.0)
