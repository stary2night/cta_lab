"""JPM t-stat 多周期动量信号。

论文：Kolanovic & Wei, "Systematic Strategies Across Asset Classes" (J.P.Morgan, 2012).

信号定义（单周期 T）：
    t_stat(T) = sqrt(T) * mean(ret_T) / std(ret_T)
    signal(T) = 2 * Phi(t_stat(T)) - 1        ∈ (-1, 1)

多周期合成：
    signal = mean over T in lookbacks of signal(T)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import norm

from signals.base import CrossSectionalSignal


class JPMTstatSignal(CrossSectionalSignal):
    """JPM t-stat 多周期趋势信号（截面型，输入收益率矩阵）。

    Parameters
    ----------
    lookbacks:
        信号窗口列表（交易日），默认 [32, 64, 126, 252, 504]。
    min_periods_ratio:
        每个窗口要求的最少有效天数比例，默认 0.5
        （min_periods = max(1, int(T * min_periods_ratio))）。
    """

    def __init__(
        self,
        lookbacks: list[int] | None = None,
        min_periods_ratio: float = 0.5,
    ) -> None:
        self.lookbacks: list[int] = lookbacks or [32, 64, 126, 252, 504]
        self.min_periods_ratio = min_periods_ratio

    def _single_period_signal(
        self, returns_df: pd.DataFrame, T: int
    ) -> pd.DataFrame:
        """计算单周期 JPM t-stat 信号并变换到 (-1, 1)。"""
        min_p = max(1, int(T * self.min_periods_ratio))
        avg = returns_df.rolling(T, min_periods=min_p).mean()
        vol = returns_df.rolling(T, min_periods=min_p).std()

        tstat = np.sqrt(T) * avg / vol.replace(0, np.nan)

        # 2 * Phi(tstat) - 1，tstat 为 NaN 时结果也为 NaN
        sig = pd.DataFrame(
            2 * norm.cdf(tstat.values) - 1,
            index=returns_df.index,
            columns=returns_df.columns,
        )
        sig[tstat.isna()] = np.nan
        return sig

    def compute(self, price_matrix: pd.DataFrame) -> pd.DataFrame:
        """计算多周期合成信号。

        Parameters
        ----------
        price_matrix:
            shape=(dates, symbols) 的价格矩阵（与基类接口兼容）。
            内部先转换为收益率再计算信号。

        Returns
        -------
        shape=(dates, symbols) 的信号矩阵，值域 (-1, 1)，NaN 处填 0。
        """
        returns_df = price_matrix.pct_change()
        return self.compute_from_returns(returns_df)

    def compute_from_returns(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """直接从收益率矩阵计算多周期合成信号。

        Parameters
        ----------
        returns_df:
            shape=(dates, symbols) 的收益率矩阵。

        Returns
        -------
        shape=(dates, symbols) 的信号矩阵，NaN 处填 0。
        """
        signals = [
            self._single_period_signal(returns_df, T) for T in self.lookbacks
        ]
        combined = sum(signals) / len(signals)  # type: ignore[arg-type]
        return combined.fillna(0.0)
