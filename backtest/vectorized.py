"""VectorizedBacktest：无事件循环的向量化回测器。

适用场景
--------
- 日度信号连续更新（每日均为"调仓日"）
- 不需要持仓状态机（无 FX 双轨、无 VRS 触发）
- 以研究效率为优先，追求毫秒级完成

与 BacktestEngine 的对比
------------------------
BacktestEngine：事件驱动，逐日推进状态机，支持稀疏调仓日、FX 重估、VRS。
VectorizedBacktest：纯矩阵运算，no Python loop，适合 paper-portfolio 研究模拟。

两者共享相同的 BacktestResult 输出接口，上层代码无感知。
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from .result import BacktestResult


class VectorizedBacktest:
    """向量化回测器。

    Parameters
    ----------
    lag : int
        执行延迟天数（T+lag 执行），默认 1（T+1）。
    vol_target : float | None
        组合目标年化波动率。不为 None 时在 PnL 序列上施加 EWMA vol-targeting，
        即 pnl_scaled[t] = pnl[t] * (target_vol / ewma_vol[t-1])。
        为 None 时跳过 vol-targeting，原始权重直接对应 PnL。
    vol_halflife : int
        EWMA vol-targeting 的半衰期（交易日），默认 21。
        仅 vol_target 不为 None 时生效。
    vol_min_periods : int
        EWMA 启动所需的最少有效样本数，默认与 vol_halflife 相同。
    trading_days : int
        年交易日数，用于年化换算，默认 252。
    fee_rate : float
        单边换手成本率，默认 0（无费用）。
        费用 = fee_rate × |Δweights| 按列求和，以 lag 对齐到执行日。
    trim_inactive : bool
        True（默认）时，从首个非零 PnL 日开始截断，使 NAV 从第一个活跃日起算，
        去掉因 lag 和信号热身期带来的前置零值段。
    """

    def __init__(
        self,
        lag: int = 1,
        vol_target: Optional[float] = None,
        vol_halflife: int = 21,
        vol_min_periods: Optional[int] = None,
        trading_days: int = 252,
        fee_rate: float = 0.0,
        trim_inactive: bool = True,
    ) -> None:
        self.lag = lag
        self.vol_target = vol_target
        self.vol_halflife = vol_halflife
        self.vol_min_periods = vol_min_periods if vol_min_periods is not None else vol_halflife
        self.trading_days = trading_days
        self.fee_rate = fee_rate
        self.trim_inactive = trim_inactive

    # ── 公开接口 ──────────────────────────────────────────────────────────────

    def run(
        self,
        weights_df: pd.DataFrame,
        returns_df: pd.DataFrame,
    ) -> BacktestResult:
        """执行向量化回测。

        Parameters
        ----------
        weights_df : DataFrame, shape (dates, symbols)
            目标权重矩阵。今日权重在 lag 日后生效（T+lag 执行）。
        returns_df : DataFrame, shape (dates, symbols)
            品种日收益率矩阵。

        Returns
        -------
        BacktestResult
            与 BacktestEngine.run() 相同的输出接口。
        """
        # 1. 对齐列与索引
        cols = weights_df.columns.intersection(returns_df.columns)
        idx  = weights_df.index.intersection(returns_df.index)
        w = weights_df.loc[idx, cols]
        r = returns_df.loc[idx, cols]

        # 2. 执行延迟：shift(lag)，前 lag 行填 0
        w_exec = w.shift(self.lag).fillna(0.0)

        # 3. 原始 PnL（未含费用）
        pnl_raw: pd.Series = (w_exec * r.fillna(0.0)).sum(axis=1)

        # 4. 换手费用（若 fee_rate > 0）
        if self.fee_rate > 0.0:
            turnover = w.diff().abs().sum(axis=1)
            fees = (self.fee_rate * turnover).shift(self.lag).fillna(0.0)
            pnl_raw = pnl_raw - fees

        # 5. 截断非活跃前缀
        if self.trim_inactive:
            first_active_mask = pnl_raw.abs() > 0
            if first_active_mask.any():
                pnl_raw = pnl_raw.loc[first_active_mask.idxmax():]

        # 6. EWMA vol-targeting（组合层面事后定标）
        if self.vol_target is not None:
            pnl_returns = self._apply_vol_targeting(pnl_raw)
        else:
            pnl_returns = pnl_raw

        # 7. 构建 NAV（从 1.0 开始）
        nav = (1.0 + pnl_returns).cumprod()
        # 在序列前插入 NAV=1.0 的起始点，与 BacktestEngine 保持一致
        start_date = pnl_returns.index[0] - pd.tseries.offsets.BDay(1)
        nav = pd.concat([pd.Series([1.0], index=[start_date]), nav])
        ret_with_start = pd.concat([pd.Series([0.0], index=[start_date]), pnl_returns])

        nav.name = "nav"
        ret_with_start.name = "returns"

        return BacktestResult(nav=nav, returns=ret_with_start)

    # ── 内部方法 ──────────────────────────────────────────────────────────────

    def _apply_vol_targeting(self, pnl: pd.Series) -> pd.Series:
        """对 PnL 序列施加 EWMA 波动率定标，使组合年化波动率趋近 vol_target。

        定标规则：scale[t] = vol_target / ewma_vol[t-1]
        ewma_vol 以 PnL 的 EWMA 标准差乘以 sqrt(trading_days) 年化。
        scale 的前导 NaN 用第一个有效值回填，确保全序列有定标系数。
        """
        ann_factor = np.sqrt(self.trading_days)
        ewma_vol = (
            pnl.ewm(halflife=self.vol_halflife, min_periods=self.vol_min_periods)
            .std()
            * ann_factor
        )
        scale = (self.vol_target / ewma_vol.shift(1)).replace(
            [np.inf, -np.inf], np.nan
        )
        # 前导 NaN 用首个有效值回填（保持初期定标稳定）
        first_valid = scale.first_valid_index()
        if first_valid is not None:
            scale = scale.ffill()
            scale.loc[:first_valid] = scale.loc[first_valid]

        return pnl * scale.fillna(1.0)
