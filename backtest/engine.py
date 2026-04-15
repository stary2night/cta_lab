"""回测引擎：每日模拟循环。"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from .execution.lag import apply_lag
from .execution.vrs import VRS
from .fees.base import FeeModel
from .fees.trading import TradingFee
from .position import PositionTracker
from .result import BacktestResult


class BacktestEngine:
    """事件驱动回测引擎。

    Parameters
    ----------
    position_tracker:
        持仓追踪器（SimpleTracker 或 FXTracker）。
    fee_models:
        费用模型列表，每日依次计算并累加。
    vrs:
        波动率重置信号（可选，仅 FXTracker 场景使用）。
    lag:
        执行延迟天数，默认 1（T+1 执行）。
    """

    def __init__(
        self,
        position_tracker: PositionTracker,
        fee_models: list[FeeModel],
        vrs: Optional[VRS] = None,
        lag: int = 1,
    ) -> None:
        self.tracker = position_tracker
        self.fee_models = fee_models
        self.vrs = vrs
        self.lag = lag

    def run(
        self,
        weight_df: pd.DataFrame,
        price_df: pd.DataFrame,
        adjust_dates: set[pd.Timestamp],
        fx_series: Optional[pd.Series] = None,
        verbose: bool = False,
    ) -> BacktestResult:
        """执行回测主循环。

        Parameters
        ----------
        weight_df:
            portfolio 层产出的权重矩阵，shape: (dates, symbols)。
        price_df:
            连续合约价格矩阵，shape: (dates, symbols)。
        adjust_dates:
            调仓日集合（从 schedule 提取 .adjust_date 字段）。
        fx_series:
            汇率序列，FXTracker 需要；为 None 时默认 fx=1.0。
        verbose:
            True 时额外记录 holdings_log / fee_log / rebalance_log。

        Returns
        -------
        BacktestResult
        """
        # 重置追踪器，确保每次 run() 从零开始
        self.tracker.reset()

        # 1. 计算日收益率
        returns_df: pd.DataFrame = price_df.pct_change()

        # 2. 对权重施加 lag
        lagged_weights: pd.DataFrame = apply_lag(weight_df, self.lag)

        # 对齐 index：取 returns_df 和 lagged_weights 的公共日期
        common_dates = returns_df.index.intersection(lagged_weights.index)
        returns_df = returns_df.loc[common_dates]
        lagged_weights = lagged_weights.reindex(common_dates, fill_value=0.0)

        # 从第2个交易日开始（第1日 pct_change 为 NaN）
        dates = common_dates[1:]  # 跳过第一行（NaN 收益率行）

        # 初始化 NAV 和收益率序列
        nav_values: list[float] = [1.0]
        return_values: list[float] = [0.0]
        nav_dates: list[pd.Timestamp] = [common_dates[0]]

        # verbose 辅助容器
        holdings_records: list[dict] = []
        fee_records: list[dict] = []
        rebalance_records: list[dict] = []

        current_nav = 1.0

        for t in dates:
            ret_t: pd.Series = returns_df.loc[t].fillna(0.0)
            is_adj: bool = t in adjust_dates

            # 当日目标权重（lag 后）
            target: Optional[pd.Series] = lagged_weights.loc[t] if is_adj else None

            # FX
            fx: float = float(fx_series.loc[t]) if fx_series is not None else 1.0

            # VRS 检查（仅在非调仓日且 vrs 已配置时）
            vrs_triggered = False
            if self.vrs is not None and not is_adj:
                # 用过去窗口收益率计算组合波动率
                # 使用 lagged_weights × returns 的滚动标准差
                holdings_now = self.tracker.get_holdings()
                common_cols = holdings_now.index.intersection(returns_df.columns)
                port_ret_history = (
                    returns_df[common_cols].fillna(0.0)
                    @ holdings_now[common_cols]
                )
                idx_pos = returns_df.index.get_loc(t)
                vol_22 = float(port_ret_history.iloc[max(0, idx_pos - 22): idx_pos].std()) if idx_pos >= 2 else 0.0
                vol_65 = float(port_ret_history.iloc[max(0, idx_pos - 65): idx_pos].std()) if idx_pos >= 2 else 0.0
                vol_130 = float(port_ret_history.iloc[max(0, idx_pos - 130): idx_pos].std()) if idx_pos >= 2 else 0.0

                triggered, vol_max = self.vrs.check_trigger(
                    date=t,
                    vol_22=vol_22,
                    vol_65=vol_65,
                    vol_130=vol_130,
                    adjust_dates=adjust_dates,
                )
                if triggered:
                    vrs_triggered = True
                    scale = self.vrs.apply(self.tracker, vol_max)  # type: ignore[arg-type]
                    # VRS 触发时额外计提交易费（若有 TradingFee 模型）
                    for fm in self.fee_models:
                        if isinstance(fm, TradingFee):
                            # 缩减量 = holdings × (1 - scale)
                            holdings_before = self.tracker.get_holdings()
                            vrs_fee = fm.rate * (holdings_before * (1 - scale)).abs().sum()
                            # 将 vrs_fee 并入下一步费用（通过调整 nav 近似处理）
                            # 此处直接从 nav 扣除
                            current_nav *= (1 - vrs_fee)

            # 记录调仓前的持仓（用于费用计算）
            prev_holdings = self.tracker.get_holdings()

            # 更新持仓，获取 P&L
            pnl = self.tracker.update(
                date=t,
                target_weights=target,
                returns=ret_t,
                is_rebalance=is_adj,
                fx=fx,
            )

            # 更新后的持仓（用于费用计算）
            curr_holdings = self.tracker.get_holdings()

            # 计算当日总费用
            total_fee = 0.0
            fee_detail: dict[str, float] = {}
            for fm in self.fee_models:
                fee = fm.daily_fee(
                    date=t,
                    nav=current_nav,
                    holdings=curr_holdings,
                    prev_holdings=prev_holdings,
                    is_rebalance=is_adj,
                )
                total_fee += fee
                fee_detail[type(fm).__name__] = fee

            # 当日收益率 = pnl - 费用
            daily_return = pnl - total_fee

            # 更新 NAV
            current_nav = current_nav * (1.0 + daily_return)

            nav_values.append(current_nav)
            return_values.append(daily_return)
            nav_dates.append(t)

            # verbose 记录
            if verbose:
                holdings_records.append(curr_holdings.to_dict())
                fee_records.append({"date": t, **fee_detail})
                if is_adj:
                    rebalance_records.append({
                        "date": t,
                        "nav": current_nav,
                    })

        # 构建结果 Series
        nav_series = pd.Series(nav_values, index=pd.DatetimeIndex(nav_dates), name="nav")
        ret_series = pd.Series(return_values, index=pd.DatetimeIndex(nav_dates), name="returns")

        # verbose 辅助 DataFrame
        holdings_log: Optional[pd.DataFrame] = None
        fee_log: Optional[pd.DataFrame] = None
        rebalance_log: Optional[pd.DataFrame] = None

        if verbose:
            if holdings_records:
                holdings_log = pd.DataFrame(holdings_records, index=pd.DatetimeIndex(nav_dates[1:]))
            if fee_records:
                fee_log = pd.DataFrame(fee_records).set_index("date")
            if rebalance_records:
                rebalance_log = pd.DataFrame(rebalance_records).set_index("date")

        return BacktestResult(
            nav=nav_series,
            returns=ret_series,
            holdings_log=holdings_log,
            fee_log=fee_log,
            rebalance_log=rebalance_log,
        )
