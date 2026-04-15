"""连续合约价格序列：策略层直接消费的标准价格对象。"""

from __future__ import annotations

from enum import Enum

import numpy as np
import pandas as pd

from .bar import BarSeries
from .calendar import TradingCalendar
from .contract import Contract
from .roll import ContractSchedule, OIMaxRoll, RollEvent, RollRule, StabilizedRule

_ANNUALIZE = 252  # 年化因子（交易日）


class AdjustMethod(str, Enum):
    """连续合约价格拼接时的跳跃消除方式。"""

    NONE = "none"   # 不调整，直接拼接原始价格
    RATIO = "ratio" # 比例调整：以换仓日前一合约收盘价为基准乘以比例系数
    ADD = "add"     # 加法调整：以换仓日前后价差做历史平移
    NAV = "nav"     # Buy-and-Roll NAV：各合约独立计算收益率后累乘，换仓基差不计入损益


class ContinuousSeries:
    """连续合约价格序列，由 BarSeries 集合 + ContractSchedule + AdjustMethod 构建。"""

    _NAV_OUTPUT_MODES = {"price", "normalized"}

    def __init__(
        self,
        symbol: str,
        series: pd.Series,
        schedule: ContractSchedule,
    ) -> None:
        self.symbol = symbol
        self._series = series.sort_index()
        self.schedule = schedule

    # ------------------------------------------------------------------
    # 构建入口
    # ------------------------------------------------------------------

    @classmethod
    def build(
        cls,
        symbol: str,
        bar_data: dict[str, BarSeries],
        contracts: list[Contract],
        roll_rule: RollRule,
        adjust: AdjustMethod = AdjustMethod.NAV,
        calendar: TradingCalendar | None = None,
        transition_days: int = 1,
        nav_output: str = "price",
    ) -> "ContinuousSeries":
        """构建连续合约序列：生成换仓时间表 → 拼接 → 价格调整。

        transition_days：换仓过渡天数（仅 NAV 模式有效）。
            1（默认）：确认当日 shift(1) 延迟，次日立即全仓切换。
            N > 1：确认次日起线性分 N-1 天过渡，第 N 天全仓到位。
            例：transition_days=3，换仓确认在第 i 日：
                第 i 日：100% 旧合约（shift(1) 保证）
                第 i+1 日：2/3 旧 + 1/3 新
                第 i+2 日：1/3 旧 + 2/3 新
                第 i+3 日：100% 新
        """
        if transition_days < 1:
            raise ValueError("transition_days 必须 >= 1。")
        if nav_output not in cls._NAV_OUTPUT_MODES:
            raise ValueError(f"nav_output must be one of {sorted(cls._NAV_OUTPUT_MODES)}.")

        # 重置有状态规则（如 StabilizedRule）的内部追踪
        roll_rule.reset()

        # ── 1. 确定全量交易日序列 ─────────────────────────────────────
        if calendar is not None:
            all_dates = calendar._dates
        else:
            # 从各合约 bar_data 的 index 合并
            all_ts: set[pd.Timestamp] = set()
            for bs in bar_data.values():
                all_ts.update(bs.data.index)
            all_dates = pd.DatetimeIndex(sorted(all_ts))

        if len(all_dates) == 0:
            raise ValueError("No trading dates found to build ContinuousSeries.")

        # ── 2. 选合约，生成 ContractSchedule 与逐日合约序列 ──────────────
        full_contract_series = cls._select_contract_series(
            all_dates=all_dates,
            contracts=contracts,
            bar_data=bar_data,
            roll_rule=roll_rule,
        )
        if full_contract_series.empty:
            raise ValueError(f"No contract schedule assembled for symbol '{symbol}'.")

        schedule = cls._build_schedule(symbol, full_contract_series)

        # ── 3. 按逐日合约序列拼接各合约 settle 价格 ────────────────────
        raw_series, contract_series = cls._assemble_raw_series(bar_data, full_contract_series)
        if raw_series.empty:
            raise ValueError(f"No settle data assembled for symbol '{symbol}'.")

        # ── 4. 按 AdjustMethod 消除换仓价格跳跃 ──────────────────────
        if adjust == AdjustMethod.NAV:
            initial_price = 1.0 if nav_output == "normalized" else float(raw_series.iloc[0])
            adjusted = cls._build_nav(
                bar_data,
                contract_series,
                initial_price=initial_price,
                transition_days=transition_days,
            )
        else:
            adjusted = cls._apply_adjustment(raw_series, contract_series, adjust)

        return cls(symbol, adjusted, schedule)

    @staticmethod
    def _build_schedule(symbol: str, contract_series: pd.Series) -> ContractSchedule:
        """根据逐日合约序列恢复换仓时间表。"""
        series = contract_series.dropna().astype(str)
        if series.empty:
            return ContractSchedule([], symbol)
        change_mask = series != series.shift(1)
        changed = series[change_mask]
        events = [
            RollEvent(
                date=ts,
                from_contract="" if i == 0 else str(changed.iloc[i - 1]),
                to_contract=str(changed.iloc[i]),
            )
            for i, ts in enumerate(changed.index)
        ]
        return ContractSchedule(events, symbol)

    @classmethod
    def _select_contract_series(
        cls,
        all_dates: pd.DatetimeIndex,
        contracts: list[Contract],
        bar_data: dict[str, "BarSeries"],
        roll_rule: RollRule,
    ) -> pd.Series:
        """生成逐日应持有的合约序列。"""
        fast = cls._select_contract_series_fast(all_dates, contracts, bar_data, roll_rule)
        if fast is not None:
            return fast

        chosen: dict[pd.Timestamp, str] = {}
        prev_code: str | None = None
        for date in all_dates:
            active_candidates = [c for c in contracts if c.is_active(date.date())]
            if not active_candidates:
                continue
            try:
                contract = roll_rule.select_contract(date, active_candidates, bar_data)
            except ValueError:
                continue
            prev_code = contract.code
            chosen[date] = prev_code
        return pd.Series(chosen, dtype=object).sort_index()

    @staticmethod
    def _apply_stability_filter(top_contract: pd.Series, stability_days: int) -> pd.Series:
        """对候选主力序列应用稳定性过滤。"""
        series = top_contract.copy()
        non_na = series.dropna()
        if non_na.empty or stability_days <= 1:
            return series

        values = series.tolist()
        stable_values: list[str | float] = [np.nan] * len(values)

        first_idx = next(i for i, v in enumerate(values) if pd.notna(v))
        current = values[first_idx]
        candidate = current
        streak = 0
        stable_values[first_idx] = current

        for i in range(first_idx + 1, len(values)):
            top_today = values[i]
            if pd.isna(top_today):
                stable_values[i] = current
                continue
            if top_today == current:
                candidate = current
                streak = 0
            else:
                if top_today == candidate:
                    streak += 1
                else:
                    candidate = top_today
                    streak = 1
                if streak >= stability_days:
                    current = candidate
                    streak = 0
            stable_values[i] = current

        return pd.Series(stable_values, index=series.index, dtype=object)

    @classmethod
    def _select_contract_series_fast(
        cls,
        all_dates: pd.DatetimeIndex,
        contracts: list[Contract],
        bar_data: dict[str, "BarSeries"],
        roll_rule: RollRule,
    ) -> pd.Series | None:
        """OIMaxRoll 的向量化 fast path。"""
        stability_days = 1
        base_rule: RollRule = roll_rule
        if isinstance(roll_rule, StabilizedRule):
            base_rule = roll_rule.base
            stability_days = roll_rule.stability_days

        if not isinstance(base_rule, OIMaxRoll):
            return None

        oi_pivot = pd.DataFrame(
            {code: bs.data["open_interest"] for code, bs in bar_data.items()}
        ).reindex(all_dates)
        if oi_pivot.empty:
            return pd.Series(dtype=object)

        active_mask = pd.DataFrame(False, index=all_dates, columns=oi_pivot.columns)
        for contract in contracts:
            if contract.code not in active_mask.columns:
                continue
            mask = (
                (all_dates >= pd.Timestamp(contract.list_date))
                & (all_dates <= pd.Timestamp(contract.last_trade_date))
            )
            active_mask.loc[mask, contract.code] = True

        eligible_oi = oi_pivot.where(active_mask)
        has_valid = eligible_oi.notna().any(axis=1)
        top_contract = eligible_oi.idxmax(axis=1).where(has_valid)
        top_contract = cls._apply_stability_filter(top_contract, stability_days=stability_days)
        return top_contract.dropna().astype(str)

    @staticmethod
    def _assemble_raw_series(
        bar_data: dict[str, "BarSeries"],
        contract_series: pd.Series,
    ) -> tuple[pd.Series, pd.Series]:
        """按逐日合约序列提取原始 settle 价格序列。"""
        settle_pivot = pd.DataFrame(
            {code: bs.data["settle"] for code, bs in bar_data.items()}
        ).sort_index()
        dates = contract_series.index
        row_idx = settle_pivot.index.get_indexer(dates)
        col_idx = settle_pivot.columns.get_indexer(contract_series.astype(str).values)

        settle_vals = settle_pivot.to_numpy()
        n_rows, n_cols = settle_vals.shape
        extracted = np.where(
            (row_idx >= 0) & (col_idx >= 0),
            settle_vals[
                np.clip(row_idx, 0, n_rows - 1),
                np.clip(col_idx, 0, n_cols - 1),
            ],
            np.nan,
        )
        raw_series = pd.Series(extracted, index=dates, name="settle").dropna().sort_index()
        contract_used = contract_series.loc[raw_series.index].astype(str)
        return raw_series, contract_used

    @staticmethod
    def _build_nav(
        bar_data: dict[str, "BarSeries"],
        contract_series: pd.Series,
        initial_price: float = 1.0,
        transition_days: int = 1,
    ) -> pd.Series:
        """Buy-and-Roll NAV：各合约独立计算收益率，换仓不引入基差损益。

        算法：
        1. 宽表 price_pivot 各列独立 pct_change()，跨合约价差永不进入收益。
        2. shift(1) 延迟：换仓确认当日收益仍来自旧合约。
        3. transition_days > 1 时，确认次日起线性分天过渡：
           过渡第 k 天（k=1..transition_days-1）：
               ret = (1 - k/transition_days) * 旧合约 + (k/transition_days) * 新合约
        4. 累乘 (1 + ret) 得到锚定到 initial_price 的连续价格链。
        """
        # ── 1. 宽表 & 逐合约收益率 ───────────────────────────────────
        price_dict: dict[str, pd.Series] = {
            code: bs.data["settle"] for code, bs in bar_data.items()
        }
        price_pivot = pd.DataFrame(price_dict).sort_index()
        ret_pivot = price_pivot.pct_change()

        # ── 2. shift(1)：换仓确认日收益仍来自旧合约 ──────────────────
        contract_for_ret = contract_series.shift(1).ffill()
        contract_for_ret.iloc[0] = contract_series.iloc[0]

        # ── 3. 向量化提取基线每日收益率 ─────────────────────────────
        dates = contract_series.index
        codes = contract_for_ret.values

        row_idx = ret_pivot.index.get_indexer(dates)
        col_idx = ret_pivot.columns.get_indexer(codes)

        ret_vals = ret_pivot.to_numpy()
        n_rows, n_cols = ret_vals.shape
        daily_ret_arr = np.where(
            (row_idx >= 0) & (col_idx >= 0),
            ret_vals[
                np.clip(row_idx, 0, n_rows - 1),
                np.clip(col_idx, 0, n_cols - 1),
            ],
            0.0,
        )
        daily_ret = pd.Series(daily_ret_arr, index=dates)
        daily_ret.iloc[0] = 0.0
        daily_ret = daily_ret.fillna(0.0)

        # ── 4. 线性过渡期覆盖（transition_days > 1）─────────────────
        # 换仓确认在第 i 日（shift(1) 已保证当日用旧合约）。
        # 过渡期：第 i+1 至 i+transition_days-1 日线性混合旧/新合约收益。
        # 第 i+transition_days 日起基线本身就是 100% 新合约，无需覆盖。
        if transition_days > 1:
            # 找出所有换仓日（contract_series 发生变化的日期，跳过首日）
            roll_mask = contract_series != contract_series.shift(1)
            roll_ilocs = np.where(roll_mask.values)[0]
            roll_ilocs = roll_ilocs[roll_ilocs > 0]  # 跳过首行

            for roll_iloc in roll_ilocs:
                old_code = contract_series.iloc[roll_iloc - 1]
                new_code = contract_series.iloc[roll_iloc]
                if old_code == new_code:
                    continue

                # 过渡步骤 k = 1 .. transition_days-1
                for k in range(1, transition_days):
                    t_iloc = roll_iloc + k
                    if t_iloc >= len(dates):
                        break
                    t_date = dates[t_iloc]

                    w_new = k / transition_days
                    w_old = 1.0 - w_new

                    old_ret = 0.0
                    if old_code in ret_pivot.columns and t_date in ret_pivot.index:
                        v = ret_pivot.loc[t_date, old_code]
                        if pd.notna(v):
                            old_ret = float(v)

                    new_ret = 0.0
                    if new_code in ret_pivot.columns and t_date in ret_pivot.index:
                        v = ret_pivot.loc[t_date, new_code]
                        if pd.notna(v):
                            new_ret = float(v)

                    daily_ret.iloc[t_iloc] = w_old * old_ret + w_new * new_ret

        nav = initial_price * (1.0 + daily_ret).cumprod()
        return nav

    @staticmethod
    def _apply_adjustment(
        raw: pd.Series,
        contract_map: pd.Series,
        method: AdjustMethod,
    ) -> pd.Series:
        """对拼接后的原始序列按指定方式进行价格连续性调整。"""

        if method == AdjustMethod.NONE:
            return raw.copy()

        if method == AdjustMethod.RATIO:
            # 从最后一段向前逐段乘以比例系数，保持最新合约价格不变
            adjusted = raw.copy()
            contract_changes = contract_map[contract_map != contract_map.shift(1)]
            change_dates = contract_changes.index.tolist()

            for roll_date in reversed(change_dates[1:]):  # 跳过第一次（初始合约）
                idx = raw.index.searchsorted(roll_date)
                if idx == 0:
                    continue
                prev_date = raw.index[idx - 1]
                price_after = raw.iloc[idx]
                price_before = raw.iloc[idx - 1]
                if price_before == 0:
                    continue
                ratio = price_after / price_before
                # 将 roll_date 之前的历史价格乘以 ratio
                adjusted.iloc[:idx] = adjusted.iloc[:idx] * ratio

            return adjusted

        if method == AdjustMethod.ADD:
            # 从最后一段向前逐段加差值，保持最新合约价格不变
            adjusted = raw.copy()
            contract_changes = contract_map[contract_map != contract_map.shift(1)]
            change_dates = contract_changes.index.tolist()

            for roll_date in reversed(change_dates[1:]):
                idx = raw.index.searchsorted(roll_date)
                if idx == 0:
                    continue
                price_after = raw.iloc[idx]
                price_before = raw.iloc[idx - 1]
                gap = price_after - price_before
                adjusted.iloc[:idx] = adjusted.iloc[:idx] + gap

            return adjusted

        raise ValueError(f"Unknown AdjustMethod: {method}")

    # ------------------------------------------------------------------
    # 固有变换方法（委托给内部序列计算）
    # ------------------------------------------------------------------

    def log_returns(self) -> pd.Series:
        """计算连续序列的对数日收益率。"""
        return np.log(self._series / self._series.shift(1))

    def pct_returns(self) -> pd.Series:
        """计算连续序列的百分比日收益率。"""
        return self._series.pct_change()

    def ewm_vol(self, halflife: int = 60) -> pd.Series:
        """计算对数收益的 EWM 年化波动率。"""
        lr = self.log_returns()
        return lr.ewm(halflife=halflife, min_periods=1).std() * np.sqrt(_ANNUALIZE)

    def rolling_vol(self, window: int = 20) -> pd.Series:
        """计算对数收益的滚动年化波动率。"""
        lr = self.log_returns()
        return lr.rolling(window=window).std() * np.sqrt(_ANNUALIZE)

    def drawdown(self) -> pd.Series:
        """计算连续序列的水下回撤（值域 [−1, 0]）。"""
        rolling_max = self._series.cummax()
        return (self._series - rolling_max) / rolling_max

    # ------------------------------------------------------------------
    # 容器协议
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        """返回序列长度（交易日数）。"""
        return len(self._series)

    def __getitem__(self, key: slice | str | pd.Timestamp) -> "ContinuousSeries":
        """支持日期切片，返回新的 ContinuousSeries 对象。"""
        sliced = self._series.loc[key]
        return ContinuousSeries(self.symbol, sliced, self.schedule)

    def __repr__(self) -> str:
        return (
            f"ContinuousSeries(symbol={self.symbol!r}, "
            f"rows={len(self)}, "
            f"range=[{self._series.index[0].date()} ~ {self._series.index[-1].date()}])"
        )

    # ------------------------------------------------------------------
    # 属性暴露
    # ------------------------------------------------------------------

    @property
    def prices(self) -> pd.Series:
        """返回调整后的连续价格序列。"""
        return self._series
