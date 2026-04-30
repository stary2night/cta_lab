"""NetMOM 策略运行结果对象。"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from backtest.result import BacktestResult


@dataclass
class NetMOMRunResult:
    """NetMOMStrategy 端到端运行结果。

    Attributes
    ----------
    returns:
        品种日收益率宽表，shape=(T, N)。
    signal:
        Ridge 回归预测值矩阵（连续，未经 sign 处理），shape=(T, N)。
    sigma:
        EWMA 年化波动率，shape=(T, N)。
    positions:
        波动率缩放后的目标持仓矩阵，shape=(T, N)。
    pnl:
        组合日收益率序列（vol-targeted, 扣费后）。
    sector_map:
        {symbol: sector} 板块映射。
    backtest_result:
        VectorizedBacktest 完整结果对象（含 NAV / 持仓明细等）。
    metadata:
        策略运行元数据（参数快照、样本区间等）。
    """

    returns: pd.DataFrame
    signal: pd.DataFrame
    sigma: pd.DataFrame
    positions: pd.DataFrame
    pnl: pd.Series
    sector_map: dict[str, str]
    backtest_result: BacktestResult | None = None
    metadata: dict = field(default_factory=dict)

    # ── 便捷统计 ──────────────────────────────────────────────────────────────

    def summary(self, trading_days: int = 252) -> dict[str, float]:
        """返回 Sharpe / 年化收益 / 波动率 / 最大回撤。"""
        pnl = self.pnl.dropna()
        if pnl.empty:
            return {}

        ann_ret = float(pnl.mean() * trading_days)
        ann_vol = float(pnl.std() * np.sqrt(trading_days))
        sharpe = ann_ret / ann_vol if ann_vol > 0 else float("nan")
        nav = (1 + pnl).cumprod()
        mdd = float(((nav - nav.cummax()) / nav.cummax()).min())

        return {
            "ann_return": ann_ret,
            "ann_vol": ann_vol,
            "sharpe": sharpe,
            "max_drawdown": mdd,
        }

    def signal_coverage(self) -> float:
        """信号有效覆盖率（非 NaN 且非 0 比例）。"""
        total = self.signal.size
        if total == 0:
            return 0.0
        active = (self.signal.notna() & (self.signal != 0.0)).sum().sum()
        return float(active / total)

    def long_short_ratio(self) -> tuple[float, float]:
        """多空各品种占总有效信号的比例（忽略 0 仓位）。"""
        pos = self.positions
        active = pos[pos != 0.0]
        if active.size == 0:
            return 0.0, 0.0
        long_frac = float((active > 0).sum().sum() / active.size)
        short_frac = float((active < 0).sum().sum() / active.size)
        return long_frac, short_frac
