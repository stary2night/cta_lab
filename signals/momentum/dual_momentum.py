"""双动量信号（Dual Momentum）。

参考：Gary Antonacci, "Risk Premia Harvesting Through Dual Momentum" (2012/2016)

核心逻辑（两步筛选）：
    1. 相对动量（Relative Momentum）：
       在同一板块内，按 lookback 期累计对数收益排名，
       前 top_pct → 相对强势（+1），后 bottom_pct → 相对弱势（-1）

    2. 绝对动量（Absolute Momentum）：
       自身累计对数收益 > abs_threshold 则趋势向上（正），否则趋势向下（负）

三种合成模式（mode）：
    'dual_ls'  : 双动量多空
                 +1 if 相对强 AND 绝对正
                 -1 if 相对弱 AND 绝对负
                  0 otherwise（信号不一致 → 空仓）
    'dual_lo'  : 双动量纯多（论文原始精神，用于期货=有趋势才做多，否则平仓）
                 +1 if 相对强 AND 绝对正
                  0 otherwise
    'relative' : 仅相对动量（板块内排名，无绝对动量过滤）
                 +1 top_pct, -1 bottom_pct
    'absolute' : 仅绝对动量（= 传统 TSMOM binary 信号）
                 sign(cum_log_ret)

说明：
    - 论文原始 T-bills 门槛在期货中等价为"累计收益 > 0"（abs_threshold=0）
    - 板块外的品种（sector_map 无对应条目）单独构成 "Other" 组
    - top_pct / bottom_pct 默认 0.5（各取一半），沿用 CrossMOM 惯例可调为 0.3
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from signals.base import CrossSectionalSignal


class DualMomentumSignal(CrossSectionalSignal):
    """板块内双动量信号。

    Parameters
    ----------
    sector_map:
        {symbol: sector_name}，用于板块内排名。
        未在 map 中的品种归入 "Other" 组。
    lookback:
        累计对数收益回望窗口（交易日），默认 252。
    top_pct:
        相对强势阈值（前 top_pct），默认 0.5（前 50%）。
    bottom_pct:
        相对弱势阈值（后 bottom_pct），默认 0.5（后 50%）。
        通常令 top_pct + bottom_pct ≤ 1；等于 1 时全部品种都有信号。
    abs_threshold:
        绝对动量门槛（累计对数收益），默认 0（类似跑赢 T-bills）。
    mode:
        'dual_ls' / 'dual_lo' / 'relative' / 'absolute'。
    min_periods:
        rolling 最少有效天数，默认 = lookback。
    sigma_halflife:
        EWMA 波动率半衰期（compute_weights 使用），默认 60。
    trading_days:
        年交易日数，默认 252。
    """

    MODES = {"dual_ls", "dual_lo", "relative", "absolute"}

    def __init__(
        self,
        sector_map: dict[str, str],
        lookback: int = 252,
        top_pct: float = 0.5,
        bottom_pct: float = 0.5,
        abs_threshold: float = 0.0,
        mode: str = "dual_ls",
        min_periods: int | None = None,
        sigma_halflife: int = 60,
        trading_days: int = 252,
    ) -> None:
        if mode not in self.MODES:
            raise ValueError(f"mode must be one of {self.MODES}, got '{mode}'")
        self.sector_map = dict(sector_map)
        self.lookback = lookback
        self.top_pct = top_pct
        self.bottom_pct = bottom_pct
        self.abs_threshold = abs_threshold
        self.mode = mode
        self.min_periods = min_periods if min_periods is not None else max(lookback // 2, 1)
        self.sigma_halflife = sigma_halflife
        self.trading_days = trading_days

    # ── 内部辅助 ──────────────────────────────────────────────────────────────

    def _cum_log_ret(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        log_ret = np.log1p(returns_df)
        return log_ret.rolling(self.lookback, min_periods=self.min_periods).sum()

    def _relative_signal(self, cum_ret: pd.DataFrame) -> pd.DataFrame:
        """板块内按累计收益排名 → top_pct=+1, bottom_pct=-1, middle=0。"""
        symbols = [s for s in cum_ret.columns if not cum_ret[s].isna().all()]
        rel = pd.DataFrame(np.nan, index=cum_ret.index, columns=cum_ret.columns)

        # 按板块分组
        groups: dict[str, list[str]] = {}
        for sym in symbols:
            sec = self.sector_map.get(sym, "Other")
            groups.setdefault(sec, []).append(sym)

        for syms in groups.values():
            if not syms:
                continue
            sec_ret = cum_ret[syms]
            # 百分位排名（跳过 NaN 行）
            pct_rank = sec_ret.rank(axis=1, pct=True, na_option="keep")
            top_mask = pct_rank > (1 - self.top_pct)
            bot_mask = pct_rank <= self.bottom_pct

            sec_sig = pd.DataFrame(0.0, index=cum_ret.index, columns=syms)
            sec_sig[top_mask[syms]] = 1.0
            sec_sig[bot_mask[syms]] = -1.0
            # 所有品种均为 NaN 的行保持 NaN
            all_nan = sec_ret.isna().all(axis=1)
            sec_sig[all_nan] = np.nan
            rel[syms] = sec_sig

        return rel

    def _absolute_signal(self, cum_ret: pd.DataFrame) -> pd.DataFrame:
        """累计收益 > abs_threshold → +1，< -abs_threshold → -1，else 0。"""
        return np.sign(cum_ret - self.abs_threshold)

    # ── CrossSectionalSignal 接口 ─────────────────────────────────────────────

    def compute(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """计算双动量信号矩阵，shape=(dates, symbols)。

        输出值域：
          dual_ls  → {-1, 0, +1}
          dual_lo  → {0, +1}
          relative → {-1, 0, +1}
          absolute → {-1, 0, +1}
        """
        cum_ret = self._cum_log_ret(returns_df)

        if self.mode == "absolute":
            return self._absolute_signal(cum_ret)

        rel = self._relative_signal(cum_ret)

        if self.mode == "relative":
            return rel

        # dual 模式
        abs_sig = self._absolute_signal(cum_ret)

        if self.mode == "dual_lo":
            # 多头：相对强 AND 绝对正
            long_mask = (rel == 1) & (abs_sig > 0)
            sig = pd.DataFrame(0.0, index=rel.index, columns=rel.columns)
            sig[long_mask] = 1.0
            sig[rel.isna()] = np.nan
            return sig

        # dual_ls：双向信号，不一致则平仓
        sig = pd.DataFrame(0.0, index=rel.index, columns=rel.columns)
        sig[(rel == 1) & (abs_sig > 0)] = 1.0    # 相对强 + 绝对正 → 多
        sig[(rel == -1) & (abs_sig < 0)] = -1.0  # 相对弱 + 绝对负 → 空
        sig[rel.isna()] = np.nan
        return sig

    def compute_weights(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """信号 / sigma_ewma，直接送入 VectorizedBacktest。"""
        signal = self.compute(returns_df)
        sigma = (
            returns_df.ewm(halflife=self.sigma_halflife, min_periods=30).std()
            * np.sqrt(self.trading_days)
        ).replace(0, np.nan)
        return (signal / sigma).fillna(0.0)
