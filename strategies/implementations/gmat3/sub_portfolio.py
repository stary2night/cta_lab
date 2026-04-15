"""GMAT3 子组合价值序列。"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .data_access import GMAT3DataAccess
from .main_contract import MainContractEngine
from .roll_return import RollReturnCalculator
from .universe import (
    BLACK_COMPONENTS,
    BLACK_REBALANCE_HISTORY_DAYS,
    BLACK_WEIGHT_MAX,
    BLACK_WEIGHT_MIN,
    BLACK_WEIGHT_WINDOW,
    SUB_PORTFOLIOS,
)


class SubPortfolioEngine:
    """计算 GMAT3 子组合价值序列。

    当前阶段已支持：
    - 切换日期前：使用替代标的收益
    - 切换日期后：使用期货展期收益
    - 境外子组合保持本币计价，FX 在指数层处理
    - `BLACK` 黑色系复合子组合：动态持仓金额权重 + 10 天线性过渡
    """

    def __init__(self, access: GMAT3DataAccess):
        self.access = access
        self._main_engine = MainContractEngine(access)
        self._roll_calc = RollReturnCalculator(access)

    def compute(
        self,
        variety: str,
        main_dfs: dict[str, pd.DataFrame] | None = None,
    ) -> pd.Series:
        if variety == "BLACK":
            return self._compute_black(main_dfs)

        cfg = SUB_PORTFOLIOS[variety]
        sub_base = pd.Timestamp(cfg["sub_base_date"])
        switch_date = pd.Timestamp(cfg["switch_date"]) if cfg.get("switch_date") else None
        substitute = cfg.get("substitute")

        if substitute and switch_date is not None:
            sub_price = self.access.get_substitute_price(substitute)
            sub_price = sub_price[(sub_price.index >= sub_base) & (sub_price.index < switch_date)]
            sub_ret = sub_price.pct_change()
            if not sub_ret.empty:
                sub_ret.iloc[0] = 0.0
        else:
            sub_ret = pd.Series(dtype=float)

        if main_dfs is not None and variety in main_dfs:
            main_df = main_dfs[variety]
        else:
            main_df = self._main_engine.compute(variety)

        fut_ret = self._roll_calc.compute(variety, main_df)
        if switch_date is not None and isinstance(fut_ret.index, pd.DatetimeIndex):
            fut_ret = fut_ret[fut_ret.index >= switch_date]

        parts = [s for s in (sub_ret, fut_ret) if len(s) > 0]
        if not parts:
            return pd.Series(dtype=float, name=variety)

        all_ret = pd.concat(parts).sort_index()
        all_ret = all_ret[~all_ret.index.duplicated(keep="last")]

        value = (1.0 + all_ret.fillna(0.0)).cumprod()
        value.name = variety
        return value

    def _compute_black(
        self,
        main_dfs: dict[str, pd.DataFrame] | None = None,
    ) -> pd.Series:
        component_main_dfs: dict[str, pd.DataFrame] = {}
        for comp in BLACK_COMPONENTS:
            if main_dfs is not None and comp in main_dfs:
                component_main_dfs[comp] = main_dfs[comp]
            else:
                component_main_dfs[comp] = self._main_engine.compute(comp)

        roll_rets: dict[str, pd.Series] = {}
        for comp, comp_main_df in component_main_dfs.items():
            roll_rets[comp] = self._roll_calc.compute(comp, comp_main_df)

        if not roll_rets:
            return pd.Series(dtype=float, name="BLACK")

        avail_comps = sorted(roll_rets)
        combined = pd.DataFrame(roll_rets).sort_index()

        ha_dict: dict[str, pd.Series] = {}
        for comp in avail_comps:
            daily = self.access.get_daily(comp)
            daily = daily[daily["settle_price"].notna() & daily["open_interest"].notna()].copy()
            daily["ha"] = daily["open_interest"] * daily["settle_price"]
            ha_dict[comp] = daily.groupby("trade_date")["ha"].sum()

        if not ha_dict:
            result = combined.mean(axis=1)
            result.name = "BLACK"
            return result

        ha_df = pd.DataFrame(ha_dict).sort_index()
        all_dates = combined.index
        ha_df = ha_df.reindex(all_dates).ffill()

        april_calc_dates: list[pd.Timestamp] = []
        april_count: dict[int, int] = {}
        for date in all_dates:
            if date.month == 4:
                year = date.year
                april_count[year] = april_count.get(year, 0) + 1
                if april_count[year] == 3:
                    april_calc_dates.append(date)

        n_smooth = 10
        eq_w = {c: 1.0 / len(avail_comps) for c in avail_comps}
        cur_drifted = eq_w.copy()
        old_weights = eq_w.copy()
        target_weights: dict[str, float] | None = None
        transition_start_i: int | None = None
        april_calc_set = set(april_calc_dates)
        weight_rows: list[dict[str, float]] = []

        for i, date in enumerate(all_dates):
            if date in april_calc_set:
                hist = ha_df.loc[:date].iloc[-BLACK_WEIGHT_WINDOW:]
                if len(hist) >= BLACK_REBALANCE_HISTORY_DAYS:
                    avg_ha = hist.mean()
                    eligible = {
                        c: float(v)
                        for c, v in avg_ha.items()
                        if not np.isnan(v) and v > 0
                    }
                    if eligible:
                        computed = _normalize_black_weights(eligible, avail_comps)
                        old_weights = cur_drifted.copy()
                        target_weights = computed
                        transition_start_i = i

            if target_weights is not None and transition_start_i is not None:
                elapsed = i - transition_start_i
                if elapsed < n_smooth:
                    alpha = (elapsed + 1) / n_smooth
                    use_w = {
                        c: (1 - alpha) * old_weights.get(c, 0.0)
                        + alpha * target_weights.get(c, 0.0)
                        for c in avail_comps
                    }
                    cur_drifted = use_w.copy()
                    if elapsed == n_smooth - 1:
                        cur_drifted = target_weights.copy()
                        target_weights = None
                else:
                    cur_drifted = target_weights.copy()
                    target_weights = None
                    use_w = cur_drifted.copy()
            else:
                use_w = cur_drifted.copy()

            weight_rows.append({c: use_w.get(c, 0.0) for c in avail_comps})

            if target_weights is None and i + 1 < len(all_dates):
                rets_today = {
                    c: float(combined.at[date, c])
                    if (c in combined.columns and not pd.isna(combined.at[date, c]))
                    else 0.0
                    for c in avail_comps
                }
                scale = sum(use_w.get(c, 0.0) * (1.0 + rets_today[c]) for c in avail_comps)
                if scale > 0:
                    cur_drifted = {
                        c: use_w.get(c, 0.0) * (1.0 + rets_today[c]) / scale
                        for c in avail_comps
                    }

        weights_df = pd.DataFrame(weight_rows, index=all_dates)
        valid_mask = combined.notna()
        w_aligned = weights_df.reindex(combined.columns, axis=1).fillna(0.0)
        w_aligned = w_aligned * valid_mask
        w_sum = w_aligned.sum(axis=1).replace(0, np.nan)
        w_norm = w_aligned.div(w_sum, axis=0)

        weighted_ret = (combined.fillna(0.0) * w_norm).sum(axis=1)
        all_nan_mask = valid_mask.sum(axis=1) == 0
        weighted_ret[all_nan_mask] = np.nan

        value = (1.0 + weighted_ret.fillna(0.0)).cumprod()
        value.name = "BLACK"
        return value


def _normalize_black_weights(
    eligible: dict[str, float],
    avail_comps: list[str],
) -> dict[str, float]:
    total = sum(eligible.values())
    if total <= 0:
        n = max(len(avail_comps), 1)
        return {c: 1.0 / n for c in avail_comps}

    raw_w = {c: v / total for c, v in eligible.items()}
    raw_w = {c: w for c, w in raw_w.items() if w >= BLACK_WEIGHT_MIN}
    if not raw_w:
        raw_w = {c: 1.0 / len(eligible) for c in eligible}

    total2 = sum(raw_w.values())
    norm_w = {c: w / total2 for c, w in raw_w.items()}

    for _ in range(20):
        over = {c: w for c, w in norm_w.items() if w > BLACK_WEIGHT_MAX}
        if not over:
            break
        excess = sum(w - BLACK_WEIGHT_MAX for w in over.values())
        under = {c: w for c, w in norm_w.items() if w <= BLACK_WEIGHT_MAX}
        if not under:
            n_over = len(over)
            norm_w = {c: 1.0 / n_over for c in over}
            break
        under_total = sum(under.values())
        new_w: dict[str, float] = {}
        for c, w in norm_w.items():
            if c in over:
                new_w[c] = BLACK_WEIGHT_MAX
            else:
                new_w[c] = w + excess * (w / under_total)
        norm_w = new_w

    total_final = sum(norm_w.values()) or 1.0
    return {c: norm_w.get(c, 0.0) / total_final for c in avail_comps}
