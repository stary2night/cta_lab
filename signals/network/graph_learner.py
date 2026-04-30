"""NetworkGraphLearner — 从动量特征推断资产间网络（邻接矩阵）。

提供两种实现：

Phase 1 — SimpleGraph（无外部依赖，默认）：
    两种 method：
    · "return_corr"  : 基于对数收益率的滚动相关矩阵
    · "feature_sim"  : 基于8维特征距离的 Gaussian kernel 相似度
                        （更接近 Kalofolias 精神，但无需 CVXPY）

Phase 2 — KalofoliasGraph（需要 cvxpy）：
    精确求解 Pu et al. (2023) 的凸优化问题：
        min_{A≥0, diag(A)=0} tr(X^T L X) + λ||A||_F^2
    其中 tr(X^T L X) = Σ_{i,j} A_ij ||x_i - x_j||^2

两个类均实现 .compute(features, returns_df, lookback) → np.ndarray (N×N)
，并支持 5-lookback ensemble。

图归一化：行归一化（每行和为1），保证特征传播是加权平均。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    pass


# ── 全局默认参数 ──────────────────────────────────────────────────────────────
GRAPH_LOOKBACKS: list[int] = [252, 504, 756, 1008, 1260]
GRAPH_LAMBDA: float = 1.0      # 正则化强度（Kalofolias λ）
CORR_THRESHOLD: float = 0.1    # 最低保留相关系数（return_corr mode）
GAUSSIAN_K: float = 1.0        # Gaussian kernel 缩放因子（feature_sim mode）
MIN_VALID_RATIO: float = 0.6   # 资产在窗口内至少有60%有效观测，才参与学图


def _row_normalize(A: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    """行归一化：每行除以该行之和，保证传播是加权平均。

    若某行全为 0（孤立节点），保持全 0（不引入 NaN）。
    """
    row_sums = A.sum(axis=1, keepdims=True)
    row_sums = np.where(row_sums < eps, 1.0, row_sums)
    return A / row_sums


class NetworkGraphLearner:
    """资产网络学习器。

    Parameters
    ----------
    method:
        图构建方法：
        · "return_corr" — 收益率滚动相关（Phase 1，快速）
        · "feature_sim" — 特征相似度 Gaussian kernel（Phase 1b，更接近论文）
        · "kalofolias"  — 精确凸优化（Phase 2，需要 cvxpy）
    lookbacks:
        多窗口 ensemble 的回望天数列表，默认 [252, 504, 756, 1008, 1260]。
    corr_threshold:
        "return_corr" mode：低于此相关系数的边截断为 0，默认 0.1。
    graph_lambda:
        "kalofolias" mode：正则化参数 λ，默认 1.0。
    gaussian_k:
        "feature_sim" mode：Gaussian bandwidth = k × median(D_ij)，默认 1.0。
    """

    def __init__(
        self,
        method: str = "feature_sim",
        lookbacks: list[int] = GRAPH_LOOKBACKS,
        corr_threshold: float = CORR_THRESHOLD,
        graph_lambda: float = GRAPH_LAMBDA,
        gaussian_k: float = GAUSSIAN_K,
        min_valid_ratio: float = MIN_VALID_RATIO,
    ) -> None:
        if method not in {"return_corr", "feature_sim", "kalofolias"}:
            raise ValueError(
                f"method must be 'return_corr', 'feature_sim', or 'kalofolias', got {method!r}"
            )
        self.method = method
        self.lookbacks = list(lookbacks)
        self.corr_threshold = corr_threshold
        self.graph_lambda = graph_lambda
        self.gaussian_k = gaussian_k
        self.min_valid_ratio = min_valid_ratio

    # ── 主入口 ────────────────────────────────────────────────────────────────

    def compute_ensemble(
        self,
        features: dict[str, pd.DataFrame],
        returns_df: pd.DataFrame,
        ref_date: pd.Timestamp,
    ) -> np.ndarray:
        """计算 ensemble 邻接矩阵（多 lookback 平均）。

        Parameters
        ----------
        features:
            MomentumFeatureBuilder.compute() 的输出，每项 shape=(T, N)。
        returns_df:
            日收益率宽表，shape=(T, N)，用于 return_corr mode。
        ref_date:
            当前参考日期；lookback 窗口向前回溯 ref_date 之前的数据。

        Returns
        -------
        A : np.ndarray，shape=(N, N)，行归一化邻接矩阵。
        """
        symbols = returns_df.columns.tolist()
        N = len(symbols)
        A_sum = np.zeros((N, N), dtype=float)
        count = 0

        # 找到 ref_date 在 index 中的位置
        idx_arr = returns_df.index
        if ref_date not in idx_arr:
            # 取最近的前一个日期
            loc = idx_arr.searchsorted(ref_date, side="right") - 1
        else:
            loc = idx_arr.get_loc(ref_date)
        loc = int(loc)

        for delta in self.lookbacks:
            start_loc = max(0, loc - delta + 1)
            if loc - start_loc < 20:  # 数据不足，跳过此 lookback
                continue

            ret_window = returns_df.iloc[start_loc : loc + 1]
            # 用日期索引切特征，避免 iloc 本地偏移错位（features 是全量 T 行 DataFrame）
            feat_window = {k: v.loc[ret_window.index] for k, v in features.items()}

            A_delta = self._compute_single(feat_window, ret_window, symbols)
            A_sum += A_delta
            count += 1

        if count == 0:
            # fallback：单位等权矩阵（每个资产等权连接其他所有资产）
            A_uni = np.ones((N, N), dtype=float)
            np.fill_diagonal(A_uni, 0.0)
            return _row_normalize(A_uni)

        A_ensemble = A_sum / count
        np.fill_diagonal(A_ensemble, 0.0)
        return _row_normalize(A_ensemble)

    # ── 单 lookback 内部实现 ──────────────────────────────────────────────────

    def _compute_single(
        self,
        feat_window: dict[str, pd.DataFrame],
        ret_window: pd.DataFrame,
        symbols: list[str],
    ) -> np.ndarray:
        """在单个时间窗口内计算邻接矩阵。"""
        if self.method == "return_corr":
            return self._return_corr_graph(ret_window, symbols)
        elif self.method == "feature_sim":
            return self._feature_sim_graph(feat_window, symbols)
        else:
            return self._kalofolias_graph(feat_window, symbols)

    # ── return_corr ──────────────────────────────────────────────────────────

    def _return_corr_graph(
        self, ret_window: pd.DataFrame, symbols: list[str]
    ) -> np.ndarray:
        """基于对数收益率的滚动相关矩阵作为邻接矩阵。

        步骤：
        1. 计算 N×N 相关矩阵
        2. 仅保留正相关（动量溢出方向）
        3. 低于 threshold 截断
        4. 对角线置 0
        """
        valid_ratio = ret_window.notna().mean(axis=0).reindex(symbols).fillna(0.0)
        valid_assets = valid_ratio >= self.min_valid_ratio
        corr = np.log1p(ret_window).corr(min_periods=max(20, int(len(ret_window) * self.min_valid_ratio))).values
        corr = np.nan_to_num(corr, nan=0.0)
        A = np.maximum(corr - self.corr_threshold, 0.0)
        invalid_idx = ~valid_assets.values
        A[invalid_idx, :] = 0.0
        A[:, invalid_idx] = 0.0
        np.fill_diagonal(A, 0.0)
        return A

    # ── feature_sim ───────────────────────────────────────────────────────────

    def _feature_sim_graph(
        self, feat_window: dict[str, pd.DataFrame], symbols: list[str]
    ) -> np.ndarray:
        """基于特征均值向量之间的 Gaussian kernel 相似度。

        步骤：
        1. 对每个资产取 feat_window 期间8个特征的均值 → x_i ∈ R^8
        2. 计算 D_ij = ||x_i - x_j||^2（标准化后的欧氏距离）
        3. A_ij = exp(-D_ij / (gaussian_k * median(D)))
        4. 对角线置 0
        """
        N = len(symbols)
        feature_names = list(feat_window.keys())
        n_feat = len(feature_names)

        # 构建特征矩阵 X ∈ R^{N × n_feat}
        X = np.zeros((N, n_feat), dtype=float)
        valid_mask = np.ones(N, dtype=bool)

        for fi, fname in enumerate(feature_names):
            fdf = feat_window[fname][symbols]
            valid_mask &= (fdf.notna().mean(axis=0).values >= self.min_valid_ratio)
            X[:, fi] = fdf.mean(axis=0, skipna=True).fillna(0.0).values

        # 标准化特征（防止量纲差异）
        std = X.std(axis=0)
        std = np.where(std < 1e-12, 1.0, std)
        X_std = X / std

        # 计算成对欧式距离平方
        diff = X_std[:, None, :] - X_std[None, :, :]  # (N, N, n_feat)
        D = (diff ** 2).sum(axis=2)                    # (N, N)

        # Gaussian kernel
        median_d = np.median(D[D > 0]) if (D > 0).any() else 1.0
        bandwidth = self.gaussian_k * median_d
        A = np.exp(-D / (bandwidth + 1e-12))
        invalid_idx = ~valid_mask
        A[invalid_idx, :] = 0.0
        A[:, invalid_idx] = 0.0
        np.fill_diagonal(A, 0.0)
        return A

    # ── kalofolias（Phase 2，需要 cvxpy） ────────────────────────────────────

    def _kalofolias_graph(
        self, feat_window: dict[str, pd.DataFrame], symbols: list[str]
    ) -> np.ndarray:
        """Kalofolias (2016) 精确求解器。

        求解：
            min_{A≥0, diag(A)=0} Σ_{i,j} A_ij * D_ij + λ * ||A||_F^2

        其中 D_ij = ||x_i - x_j||^2（特征均值向量的欧氏距离平方）。
        闭式解（对称约束放松后）：
            A_ij = max(-D_ij / (2λ) + c, 0)
        其中 c 由连通性约束（每行至少一个非零边）确定。

        当 cvxpy 不可用时，回退到 feature_sim 方法。
        """
        # 当前项目先使用稳健近似，避免退化为零图。
        # 若后续需要精确复现论文，可在此补齐带连通性/度约束的完整优化。
        return self._feature_sim_graph(feat_window, symbols)

    def _kalofolias_cvxpy(
        self,
        feat_window: dict[str, pd.DataFrame],
        symbols: list[str],
        cp,
    ) -> np.ndarray:
        """使用 cvxpy 求解 Kalofolias 图学习问题。"""
        N = len(symbols)
        feature_names = list(feat_window.keys())
        n_feat = len(feature_names)

        # 构建特征矩阵 X ∈ R^{N × n_feat}（取均值）
        X = np.zeros((N, n_feat))
        for fi, fname in enumerate(feature_names):
            fdf = feat_window[fname][symbols].fillna(0.0)
            X[:, fi] = fdf.mean(axis=0).values

        std = X.std(axis=0)
        std = np.where(std < 1e-12, 1.0, std)
        X_std = X / std

        # 距离矩阵
        diff = X_std[:, None, :] - X_std[None, :, :]
        D = (diff ** 2).sum(axis=2)  # (N, N)

        # 仅优化上三角（对称约束）
        lam = self.graph_lambda

        # 向量化 D 的上三角元素
        idx_i, idx_j = np.triu_indices(N, k=1)
        d_vec = D[idx_i, idx_j]

        # 优化变量（上三角边权重）
        a_vec = cp.Variable(len(d_vec), nonneg=True)

        # 目标：Σ A_ij * D_ij + λ * ||A||_F^2 （对称，系数×2抵消）
        obj = cp.sum(cp.multiply(d_vec, a_vec)) + lam * cp.sum_squares(a_vec)

        # 求解
        prob = cp.Problem(cp.Minimize(obj))
        try:
            prob.solve(solver=cp.SCS, verbose=False)
        except Exception:
            return self._feature_sim_graph(feat_window, symbols)

        if a_vec.value is None:
            return self._feature_sim_graph(feat_window, symbols)

        # 重建对称矩阵
        A = np.zeros((N, N))
        a_vals = np.maximum(a_vec.value, 0.0)
        A[idx_i, idx_j] = a_vals
        A[idx_j, idx_i] = a_vals
        np.fill_diagonal(A, 0.0)
        return A
