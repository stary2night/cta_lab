"""CorrCapSizer：相关性截断（Correlation Cap）协方差感知定仓器。

实现 JPM 论文中的相关性截断优化：将相关性矩阵中绝对值超过 cap 的元素截断，
再通过线性系统求解确定最优头寸，并缩放到目标波动率。

算法：
  1. 对活跃品种（信号非零且波动率有效）取相关性矩阵 C
  2. C_capped[i,j] = sign(C[i,j]) × min(|C[i,j]|, cap)，对角线保持 1
  3. 方向调整协方差矩阵 Sigma_adj = outer(sign_s, sign_s) × outer(sigma_d, sigma_d) × C_capped
  4. 解线性系统 Sigma_adj @ u = |s|，u >= 0
  5. w = sign(s) × u，缩放使组合年化波动率 = target_vol

使用方法：
    corr_cache = CorrCapSizer.build_corr_cache(returns_df)
    sizer = CorrCapSizer(cap=0.25, target_vol=0.10)
    weights = sizer.compute(signal_df, vol_df, corr_cache=corr_cache)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .base import Sizer


class CorrCapSizer(Sizer):
    """相关性截断（CorrCap）定仓器。

    继承自 Sizer，通过 compute() 的 corr_cache 参数接受滚动相关性矩阵。
    当 corr_cache 为 None 或某日期不在缓存中时，该日返回零权重。

    Parameters
    ----------
    cap:
        相关性截断上限，默认 0.25。
    target_vol:
        目标年化波动率，默认 0.10。
    trading_days:
        年交易日数，默认 252。
    """

    def __init__(
        self,
        cap: float = 0.25,
        target_vol: float = 0.10,
        trading_days: int = 252,
    ) -> None:
        self.cap = cap
        self.target_vol = target_vol
        self.trading_days = trading_days

    # ── 公开接口 ──────────────────────────────────────────────────────────────

    @staticmethod
    def build_corr_cache(
        returns_df: pd.DataFrame,
        window: int = 252,
        min_periods: int = 63,
    ) -> dict[pd.Timestamp, np.ndarray]:
        """计算滚动相关性矩阵并缓存为 {date: ndarray}。

        Parameters
        ----------
        returns_df:
            品种日收益率矩阵，shape=(dates, symbols)。
        window:
            滚动窗口天数，默认 252。
        min_periods:
            最少有效天数，默认 63。

        Returns
        -------
        dict mapping pd.Timestamp → shape=(n_sym, n_sym) 相关性矩阵。
        """
        assets = returns_df.columns.tolist()
        rolling_corr = returns_df.rolling(window, min_periods=min_periods).corr()

        corr_cache: dict[pd.Timestamp, np.ndarray] = {}
        for dt, grp in rolling_corr.groupby(level=0):
            mat = (
                grp.droplevel(0)
                .reindex(index=assets, columns=assets)
                .values.astype(float)
            )
            mat = np.where(np.isnan(mat), 0.0, mat)
            np.fill_diagonal(mat, 1.0)
            corr_cache[dt] = mat

        return corr_cache

    def compute(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict[pd.Timestamp, np.ndarray] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """计算全时间序列的 CorrCap 头寸矩阵。

        Parameters
        ----------
        signal_df:
            信号矩阵，shape=(dates, symbols)，值域通常为 (-1, 1)。
        vol_df:
            年化 EWMA 波动率，shape=(dates, symbols)。
        corr_cache:
            {date: 相关性矩阵}，由 build_corr_cache() 生成。
            为 None 时所有日期返回零权重。
        verbose:
            True 时每 1000 日打印进度。

        Returns
        -------
        shape=(dates, symbols) 的头寸权重矩阵。
        """
        dates = signal_df.index
        assets = signal_df.columns.tolist()
        pos_out = pd.DataFrame(0.0, index=dates, columns=assets)

        if corr_cache is None:
            return pos_out

        for i_t, t in enumerate(dates):
            if verbose and i_t % 1000 == 0:
                print(f"    CorrCap: {i_t}/{len(dates)} dates...")

            if t not in corr_cache:
                continue

            s = signal_df.loc[t].values.astype(float)
            sigma = vol_df.loc[t].values.astype(float)
            active = ~np.isnan(sigma) & (sigma > 1e-8) & (np.abs(s) > 1e-6)

            w = self._solve_one_date(s, sigma, corr_cache[t], active)
            pos_out.loc[t] = w

        return pos_out

    # ── 内部：单日 CorrCap 头寸计算 ───────────────────────────────────────────

    def _solve_one_date(
        self,
        s: np.ndarray,
        sigma: np.ndarray,
        C_full: np.ndarray,
        active: np.ndarray,
    ) -> np.ndarray:
        """计算单日 CorrCap 权重向量（全资产维度）。"""
        n_total = len(s)
        w_out = np.zeros(n_total)

        if active.sum() < 2:
            return w_out

        s_a = s[active]
        sigma_a = sigma[active]
        C_a = C_full[np.ix_(active, active)]

        # 截断相关性
        C_capped = np.sign(C_a) * np.minimum(np.abs(C_a), self.cap)
        np.fill_diagonal(C_capped, 1.0)

        # 日度协方差矩阵（sigma 为年化，转为日度）
        sigma_d = sigma_a / np.sqrt(self.trading_days)
        Sigma = np.outer(sigma_d, sigma_d) * C_capped

        # 方向调整协方差
        sign_s = np.sign(s_a)
        Sigma_adj = np.outer(sign_s, sign_s) * Sigma

        abs_s = np.abs(s_a)
        try:
            u = np.linalg.solve(Sigma_adj, abs_s)
        except np.linalg.LinAlgError:
            u, *_ = np.linalg.lstsq(Sigma_adj, abs_s, rcond=None)

        u = np.clip(u, 0, None)
        if u.sum() < 1e-12:
            return w_out

        w = sign_s * u

        # 缩放到目标波动率
        port_var = float(w @ Sigma @ w) * self.trading_days
        if port_var <= 0:
            return w_out

        w *= self.target_vol / np.sqrt(port_var)

        col_idx = np.where(active)[0]
        for k, ci in enumerate(col_idx):
            w_out[ci] = w[k]

        return w_out
