"""NetworkMomentumSignal — 网络动量信号。

基于 Pu et al. (2023) "Network Momentum across Asset Classes"：

工作流（每隔 RETRAIN_FREQ 天重新训练）：
  1. 计算8维动量特征矩阵
  2. 图学习：构建 ensemble 邻接矩阵 A（5个 lookback 平均）
  3. 网络特征传播：ũ_i = Σ_j A_ij · u_j （矩阵乘法 A @ features_today）
  4. Ridge 回归：用过去 TRAIN_WINDOW 天的 (网络特征, 目标收益) 训练
  5. 预测：signal_t = model.predict(ũ_t)

支持两种模式：
  · "net_only"  — 仅用网络动量特征（GMOM 变体）
  · "combo"     — 个体特征 + 网络特征拼接后回归（RegCombo，论文最优）

输出 signal 为 Ridge 预测值（连续值），由 Strategy 层做 sign() 和 vol-scaling。
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from signals.base import CrossSectionalSignal

from .features import MomentumFeatureBuilder
from .graph_learner import NetworkGraphLearner


# ── 默认参数（与伪代码全局参数对齐） ───────────────────────────────────────────
TRAIN_WINDOW: int = 1260          # 训练窗口（约5年）
RETRAIN_FREQ: int = 21            # 再训练频率（约1个月）
RIDGE_ALPHA: float = 1.0
GRAPH_LOOKBACKS: list[int] = [252, 504, 756, 1008, 1260]
SIGMA_HALFLIFE: int = 63
TRADING_DAYS: int = 252
TARGET_SIGMA_MIN_PERIODS: int = 5


class NetworkMomentumSignal(CrossSectionalSignal):
    """网络动量信号（跨资产动量溢出）。

    Parameters
    ----------
    mode:
        "net_only"  — 仅网络动量特征（GMOM）
        "combo"     — 个体 + 网络特征拼接（RegCombo，默认，论文最优）
    graph_method:
        图学习方法："return_corr" / "feature_sim"（默认）/ "kalofolias"
    graph_lookbacks:
        ensemble 的 lookback 窗口列表（天），默认 [252,504,756,1008,1260]。
    train_window:
        Ridge 回归训练窗口（天），默认 1260（约5年）。
    retrain_freq:
        重新训练频率（天），默认 21（约1个月）。
    ridge_alpha:
        Ridge 正则化强度，默认 1.0。
    sigma_halflife:
        EWMA 波动率半衰期，默认 63（与特征构建保持一致）。
    trading_days:
        每年交易日数，默认 252。
    verbose:
        打印训练进度，默认 False。
    """

    def __init__(
        self,
        mode: Literal["net_only", "combo"] = "combo",
        graph_method: str = "feature_sim",
        graph_lookbacks: list[int] = GRAPH_LOOKBACKS,
        train_window: int = TRAIN_WINDOW,
        retrain_freq: int = RETRAIN_FREQ,
        ridge_alpha: float = RIDGE_ALPHA,
        sigma_halflife: int = SIGMA_HALFLIFE,
        trading_days: int = TRADING_DAYS,
        verbose: bool = False,
    ) -> None:
        if mode not in {"net_only", "combo"}:
            raise ValueError(f"mode must be 'net_only' or 'combo', got {mode!r}")

        self.mode = mode
        self.graph_method = graph_method
        self.graph_lookbacks = list(graph_lookbacks)
        self.train_window = train_window
        self.retrain_freq = retrain_freq
        self.ridge_alpha = ridge_alpha
        self.sigma_halflife = sigma_halflife
        self.trading_days = trading_days
        self.verbose = verbose

        self._feature_builder = MomentumFeatureBuilder(
            sigma_halflife=sigma_halflife,
            trading_days=trading_days,
        )
        self._graph_learner = NetworkGraphLearner(
            method=graph_method,
            lookbacks=graph_lookbacks,
        )

    # ── CrossSectionalSignal 接口 ─────────────────────────────────────────────

    def compute(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """计算网络动量信号矩阵。

        Parameters
        ----------
        returns_df:
            日收益率宽表，shape=(T, N)，index=DatetimeIndex。
            需要至少 max(graph_lookbacks) + train_window 天数据才能开始产生信号。

        Returns
        -------
        signal_df : pd.DataFrame，shape=(T, N)
            Ridge 回归的预测值（连续型信号）。
            前导期（训练数据不足）填充 0.0。
            由 Strategy 层负责 sign() 和 vol-scaling。
        """
        from sklearn.linear_model import Ridge
        from sklearn.preprocessing import StandardScaler

        symbols = returns_df.columns.tolist()
        dates = returns_df.index
        T, N = len(dates), len(symbols)

        # 1. 预计算全量特征（向量化，O(T·N·F)）
        if self.verbose:
            print("  [NetMOM] Computing momentum features...")
        features = self._feature_builder.compute(returns_df)
        sigma = self._feature_builder.compute_sigma(returns_df)

        # 2. 预计算目标变量：未来1天 vol-scaled 收益（向前位移1步）
        # 对目标端保留真实缺失，但给早期样本一个 expanding-vol 兜底，避免样本量过度收缩。
        target_df = self._build_target_df(returns_df, sigma)

        # 3. 预构建全量特征三维数组 feat_3d: (T, N, n_feat)，避免内层按日遍历
        feat_names = self._feature_builder.feature_names
        n_feat = len(feat_names)
        feat_3d = np.stack(
            [features[fn].values for fn in feat_names], axis=-1
        )  # (T, N, n_feat)；NaN 保留，后续 valid_mask 过滤
        target_arr = target_df.values  # (T, N)

        # 4. 初始化输出矩阵
        signal_arr = np.zeros((T, N), dtype=float)
        graph_cache: dict[int, np.ndarray] = {}

        # 5. 滚动拟合 + 预测
        model: Ridge | None = None
        scaler: StandardScaler | None = None
        A_current: np.ndarray | None = None  # (N, N) 行归一化邻接矩阵
        last_refit_idx: int = -self.retrain_freq  # 首次到 warmup 立即 refit

        warmup = self.train_window

        for t_idx in range(warmup, T - 1):

            need_refit = (t_idx - last_refit_idx) >= self.retrain_freq

            if need_refit:
                date_t = dates[t_idx]
                train_start = max(0, t_idx - self.train_window)
                train_end = t_idx  # [train_start, train_end)

                A_current = self._get_graph_for_index(
                    t_idx=train_end - 1,
                    dates=dates,
                    features=features,
                    returns_df=returns_df,
                    cache=graph_cache,
                )

                # --- 分段构建训练矩阵：每段仅使用当时可见的网络结构 ---
                # 训练窗内按 retrain_freq 分段更新图，避免前视的同时控制计算量。
                X_train_blocks: list[np.ndarray] = []
                y_train_blocks: list[np.ndarray] = []

                for block_start in range(train_start, train_end, self.retrain_freq):
                    block_end = min(block_start + self.retrain_freq, train_end)
                    graph_idx = block_end - 1
                    A_hist = self._get_graph_for_index(
                        t_idx=graph_idx,
                        dates=dates,
                        features=features,
                        returns_df=returns_df,
                        cache=graph_cache,
                    )
                    ind_block = feat_3d[block_start:block_end]  # (W, N, F)
                    ind_block_filled = np.where(np.isnan(ind_block), 0.0, ind_block)
                    net_block = np.einsum("ij,wjf->wif", A_hist, ind_block_filled)

                    if self.mode == "combo":
                        X_block = np.concatenate([ind_block, net_block], axis=-1)
                    else:
                        X_block = net_block

                    y_block = target_arr[block_start:block_end]
                    X_flat = X_block.reshape(-1, X_block.shape[-1])
                    y_flat = y_block.reshape(-1)
                    valid_hist = ~(np.isnan(X_flat).any(axis=1) | np.isnan(y_flat))
                    if valid_hist.any():
                        X_train_blocks.append(X_flat[valid_hist])
                        y_train_blocks.append(y_flat[valid_hist])

                if not X_train_blocks:
                    last_refit_idx = t_idx
                    continue

                X_valid = np.vstack(X_train_blocks)
                y_valid = np.concatenate(y_train_blocks)

                if len(X_valid) < 50:
                    last_refit_idx = t_idx
                    continue

                # 标准化 + Ridge 拟合
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X_valid)
                model = Ridge(alpha=self.ridge_alpha)
                model.fit(X_scaled, y_valid)
                last_refit_idx = t_idx

                if self.verbose:
                    print(
                        f"  [NetMOM] Refit at {date_t.date()}: "
                        f"n_samples={len(y_valid)}, n_assets={N}"
                    )

            if model is None or A_current is None or scaler is None:
                continue

            # --- 当日预测（向量化，N 个品种同时处理）---
            ind_today = feat_3d[t_idx]                    # (N, F)
            ind_today_filled = np.where(np.isnan(ind_today), 0.0, ind_today)
            net_today = A_current @ ind_today_filled       # (N, F)

            if self.mode == "combo":
                X_today = np.concatenate([ind_today, net_today], axis=-1)  # (N, 2F)
            else:
                X_today = net_today                        # (N, F)

            # 标记含 NaN 的品种
            valid_today = ~np.isnan(X_today).any(axis=1)  # (N,)
            if valid_today.any():
                X_pred = scaler.transform(X_today[valid_today])
                preds = model.predict(X_pred)              # (n_valid,)
                signal_arr[t_idx, valid_today] = preds

        signal_df = pd.DataFrame(signal_arr, index=dates, columns=symbols)
        return signal_df

    def _build_target_df(
        self,
        returns_df: pd.DataFrame,
        sigma_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """构建训练目标。

        原则：
        1. 仅在未来1天收益真实存在时保留样本；
        2. 优先使用与特征一致的 EWMA sigma；
        3. 对新品种早期阶段，用 expanding std 作为保守兜底，减少样本浪费。
        """
        future_ret = returns_df.shift(-1)

        fallback_sigma = (
            returns_df.expanding(min_periods=TARGET_SIGMA_MIN_PERIODS).std()
            * np.sqrt(self.trading_days)
        )
        sigma_target = sigma_df.combine_first(fallback_sigma).replace(0.0, np.nan)
        return future_ret / sigma_target

    def _get_graph_for_index(
        self,
        t_idx: int,
        dates: pd.DatetimeIndex,
        features: dict[str, pd.DataFrame],
        returns_df: pd.DataFrame,
        cache: dict[int, np.ndarray],
    ) -> np.ndarray:
        """返回指定日期可见信息下的邻接矩阵，并做缓存复用。"""
        if t_idx not in cache:
            cache[t_idx] = self._graph_learner.compute_ensemble(
                features=features,
                returns_df=returns_df.iloc[: t_idx + 1],
                ref_date=dates[t_idx],
            )
        return cache[t_idx]

    # ── 便捷方法 ──────────────────────────────────────────────────────────────

    def compute_features_only(
        self, returns_df: pd.DataFrame
    ) -> dict[str, pd.DataFrame]:
        """仅计算特征矩阵（用于调试和可视化）。"""
        return self._feature_builder.compute(returns_df)

    def compute_graph_at(
        self,
        features: dict[str, pd.DataFrame],
        returns_df: pd.DataFrame,
        ref_date: pd.Timestamp,
    ) -> pd.DataFrame:
        """在指定日期计算邻接矩阵（用于网络可视化）。

        Returns
        -------
        pd.DataFrame，shape=(N, N)，index=columns=品种代码。
        """
        A = self._graph_learner.compute_ensemble(features, returns_df, ref_date)
        symbols = returns_df.columns.tolist()
        return pd.DataFrame(A, index=symbols, columns=symbols)

    def propagate_features(
        self,
        features: dict[str, pd.DataFrame],
        A: np.ndarray | pd.DataFrame,
        date: pd.Timestamp,
    ) -> pd.DataFrame:
        """在指定日期执行单步特征传播 ũ = A @ u（用于调试）。

        Returns
        -------
        pd.DataFrame，shape=(N, n_features)，index=品种代码。
        """
        if isinstance(A, pd.DataFrame):
            A_arr = A.values
            symbols = A.index.tolist()
        else:
            symbols = list(features[next(iter(features))].columns)
            A_arr = A

        feat_names = self._feature_builder.feature_names
        feat_row = np.column_stack(
            [features[fn].loc[date].values for fn in feat_names]
        )  # (N, n_feat)

        net_feat = A_arr @ feat_row  # (N, n_feat)
        return pd.DataFrame(net_feat, index=symbols, columns=feat_names)
