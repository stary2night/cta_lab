"""MomentumFeatureBuilder — 构建网络动量策略所需的动量特征矩阵。

基于 Pu et al. (2023) 第2节：

特征定义（每个资产、每个交易日）：
  (1)-(5) 波动率缩放收益：
      f_ret_w = log(P_t / P_{t-w}) / sigma_t
      w ∈ {5, 21, 63, 126, 252}（对应1周/1月/3月/6月/1年）

  (6)-(8) 标准化 MACD：
      macd_raw = EMA(P, fast) - EMA(P, slow)
      f_macd = macd_raw / sigma_t
      (fast, slow) ∈ {(8,24), (16,48), (32,96)}

sigma 使用 EWMA 年化波动率，与框架内其他策略保持一致。

Input:  returns_df — 日收益率宽表，shape=(T, N)
Output: dict[str, pd.DataFrame]，每个 value 的 shape 均为 (T, N)
        keys: "ret_5", "ret_21", "ret_63", "ret_126", "ret_252",
              "macd_8_24", "macd_16_48", "macd_32_96"
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ── 默认参数（中国市场调优版） ─────────────────────────────────────────────────
RET_WINDOWS: list[int] = [5, 21, 63, 126, 252]  # 以周频替代1日，降低短反转噪音
MACD_PARAMS: list[tuple[int, int]] = [(8, 24), (16, 48), (32, 96)]
SIGMA_HALFLIFE: int = 63          # 约3个月，与伪代码 VOL_SCALE_WINDOW=3*21 对齐
TRADING_DAYS: int = 252
FEATURE_NAMES: list[str] = (
    [f"ret_{w}" for w in RET_WINDOWS]   # ret_21, ret_63, ret_126, ret_252
    + [f"macd_{f}_{s}" for f, s in MACD_PARAMS]
)


class MomentumFeatureBuilder:
    """构建动量特征矩阵。

    Parameters
    ----------
    ret_windows:
        vol-scaled returns 的回望窗口（交易日），默认 [5, 21, 63, 126, 252]。
    macd_params:
        MACD (fast, slow) EMA 参数对，默认 [(8,24), (16,48), (32,96)]。
    sigma_halflife:
        EWMA 波动率估计的半衰期（交易日），默认 63（≈3个月）。
    trading_days:
        每年交易日数，用于年化波动率，默认 252。
    """

    def __init__(
        self,
        ret_windows: list[int] = RET_WINDOWS,
        macd_params: list[tuple[int, int]] = MACD_PARAMS,
        sigma_halflife: int = SIGMA_HALFLIFE,
        trading_days: int = TRADING_DAYS,
    ) -> None:
        self.ret_windows = list(ret_windows)
        self.macd_params = list(macd_params)
        self.sigma_halflife = sigma_halflife
        self.trading_days = trading_days

    # ── 公开接口 ───────────────────────────────────────────────────────────────

    def compute(self, returns_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """计算8维特征矩阵。

        Parameters
        ----------
        returns_df:
            日收益率宽表，shape=(T, N)，index=DatetimeIndex，columns=品种代码。

        Returns
        -------
        dict[feature_name, DataFrame(T, N)]
            共 len(ret_windows) + len(macd_params) 个特征。
            NaN 代表数据不足（前导期），下游模块负责处理。
        """
        sigma = self._compute_sigma(returns_df)
        prices = self._reconstruct_prices(returns_df)
        log_prices = np.log(prices.clip(lower=1e-12))

        features: dict[str, pd.DataFrame] = {}

        # (1)-(5) vol-scaled log returns
        for w in self.ret_windows:
            log_ret_w = log_prices - log_prices.shift(w)
            features[f"ret_{w}"] = (log_ret_w / sigma).replace([np.inf, -np.inf], np.nan)

        # (6)-(8) normalized MACD
        for fast, slow in self.macd_params:
            ema_fast = prices.ewm(span=fast, min_periods=fast).mean()
            ema_slow = prices.ewm(span=slow, min_periods=slow).mean()
            macd_raw = ema_fast - ema_slow
            features[f"macd_{fast}_{slow}"] = (macd_raw / sigma).replace(
                [np.inf, -np.inf], np.nan
            )

        return features

    def compute_sigma(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """单独暴露 sigma 计算，供外部模块复用。"""
        return self._compute_sigma(returns_df)

    @property
    def feature_names(self) -> list[str]:
        """按计算顺序排列的特征名列表。"""
        return (
            [f"ret_{w}" for w in self.ret_windows]
            + [f"macd_{f}_{s}" for f, s in self.macd_params]
        )

    # ── 内部辅助 ───────────────────────────────────────────────────────────────

    def _compute_sigma(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """EWMA 年化截面波动率。"""
        sigma = (
            returns_df.ewm(halflife=self.sigma_halflife, min_periods=20).std()
            * np.sqrt(self.trading_days)
        )
        return sigma.replace(0.0, np.nan)

    @staticmethod
    def _reconstruct_prices(returns_df: pd.DataFrame) -> pd.DataFrame:
        """从收益率反向重建合成价格序列（锚定至 1.0）。

        对于 MACD EMA 计算，价格的绝对水平不影响结果（EMA 差值除以 sigma），
        所以从 1.0 起始的合成序列即可。
        """
        prices = (1.0 + returns_df.fillna(0.0)).cumprod()
        # 上市前保持 NaN，避免把缺失历史误当成平稳价格路径。
        started = returns_df.notna().cumsum().gt(0)
        return prices.where(started)

    def to_stacked_matrix(
        self,
        features: dict[str, pd.DataFrame],
        date: pd.Timestamp,
        min_valid_assets: int = 2,
    ) -> tuple[pd.DataFrame, list[str]]:
        """将 features 在指定 date 截面展开为 (N_valid, n_features) 矩阵。

        Parameters
        ----------
        features:
            compute() 的输出。
        date:
            目标日期。
        min_valid_assets:
            保留的品种至少有多少非 NaN 特征，默认 2。

        Returns
        -------
        X : DataFrame，shape=(N_valid, n_features)，行为品种，列为特征
        valid_symbols : 对应的品种代码列表
        """
        feature_names = self.feature_names
        rows: dict[str, list[float]] = {}

        for sym in features[feature_names[0]].columns:
            row = [features[f].at[date, sym] for f in feature_names]
            n_valid = sum(1 for v in row if not np.isnan(v))
            if n_valid >= min_valid_assets:
                rows[sym] = row

        if not rows:
            return pd.DataFrame(columns=feature_names), []

        X = pd.DataFrame.from_dict(rows, orient="index", columns=feature_names)
        return X, list(X.index)
