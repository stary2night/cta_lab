"""GMAT3 权重计算。"""

from __future__ import annotations

from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd

from portfolio.constraints.vol_scaler import WAF
from portfolio.sizing.risk_budget import RiskBudgetSizer
from .schedule import build_staggered_schedule
from .signals import SignalCalculator
from .universe import MIN_HISTORY_DAYS, MOMENTUM_SELECT_N, SUB_PORTFOLIOS, VOL_WINDOWS, WAF_TARGET_VOL


RB_MULT_BOTH = 1.5
RB_MULT_MOM = 1.0
RB_MULT_REV = 0.5
RB_BASE_RATE = 0.10
_TVS_VOL_WINDOWS = [21, 64, 129, 259]


def build_gmat3_weights(
    signal_df: pd.DataFrame,
    vol_df: pd.DataFrame,
    *,
    base_risk: float,
    signal_mode: str,
    waf_threshold: float,
    waf_target: float,
) -> pd.DataFrame:
    """RiskBudget 定仓 + WAF 缩放。"""
    raw_w = RiskBudgetSizer(base_risk=base_risk, signal_mode=signal_mode).compute(
        signal_df, vol_df
    )

    pv22 = vol_df.rolling(22).mean().mean(axis=1)
    pv65 = vol_df.rolling(65).mean().mean(axis=1)
    pv130 = vol_df.rolling(130).mean().mean(axis=1)

    waf = WAF(waf_threshold, waf_target)
    return waf.apply(raw_w, pv22, pv65, pv130)


class WeightCalculator:
    """GMAT3 权重计算器。"""

    def __init__(self) -> None:
        self.signal_calc = SignalCalculator()

    def sub_index_schedule(
        self,
        calendar_or_index_days,
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
        n_sub_portfolios: int = 4,
    ):
        if isinstance(calendar_or_index_days, list):
            return self._sub_index_schedule_from_index_days(calendar_or_index_days)
        return build_staggered_schedule(
            calendar=calendar_or_index_days,
            start=start,
            end=end,
            n_sub_portfolios=n_sub_portfolios,
        )

    def compute(
        self,
        value_df: pd.DataFrame,
        index_trading_days: List[pd.Timestamp],
    ) -> tuple[pd.DataFrame, Dict[int, Dict[str, List[pd.Timestamp]]]]:
        varieties = list(value_df.columns)
        schedule = self._sub_index_schedule_from_index_days(index_trading_days)

        rev_series_cache: Dict[str, pd.Series] = {}
        risk_series_cache: Dict[str, pd.Series] = {}
        for variety in varieties:
            rev_series_cache[variety] = SignalCalculator.compute_reversal_series(value_df[variety])
            risk_series_cache[variety] = SignalCalculator.compute_risk_series(value_df[variety])

        returns_df = value_df.pct_change()

        sub_weights_computed: Dict[int, Dict[str, float]] = {n: {} for n in range(1, 5)}
        sub_weights_applied: Dict[int, Dict[str, float]] = {n: {} for n in range(1, 5)}
        calc_date_sets = {n: set(schedule[n]["calc_dates"]) for n in range(1, 5)}
        adjust_date_sets = {n: set(schedule[n]["adjust_dates"]) for n in range(1, 5)}

        result_rows = []
        for i, t in enumerate(index_trading_days):
            for sub_n in range(1, 5):
                if t in calc_date_sets[sub_n]:
                    sub_weights_computed[sub_n] = self._calc_mid_weights(
                        value_df=value_df,
                        t=t,
                        varieties=varieties,
                        rev_series_cache=rev_series_cache,
                        index_trading_days=index_trading_days,
                        t_idx=i,
                        returns_df=returns_df,
                        risk_series_cache=risk_series_cache,
                    )
                if t in adjust_date_sets[sub_n]:
                    sub_weights_applied[sub_n] = sub_weights_computed[sub_n]

            all_sub = {}
            for variety in varieties:
                w_sum = sum(sub_weights_applied[n].get(variety, 0.0) for n in range(1, 5))
                all_sub[variety] = w_sum / 4.0
            result_rows.append({"trade_date": t, **all_sub})

        if not result_rows:
            return pd.DataFrame(), schedule

        weight_df = pd.DataFrame(result_rows).set_index("trade_date")
        return weight_df, schedule

    def _sub_index_schedule_from_index_days(
        self, index_trading_days: List[pd.Timestamp]
    ) -> Dict[int, Dict[str, List[pd.Timestamp]]]:
        from collections import defaultdict
        import bisect

        by_month: Dict[tuple[int, int], List[pd.Timestamp]] = defaultdict(list)
        for d in index_trading_days:
            by_month[(d.year, d.month)].append(d)

        calc_1, adjust_1 = [], []
        for ym in sorted(by_month):
            days = sorted(by_month[ym])
            if len(days) >= 2:
                calc_1.append(days[1])
            if len(days) >= 4:
                adjust_1.append(days[3])

        days_sorted = sorted(index_trading_days)

        def next_trading_day(anchor: pd.Timestamp):
            idx = bisect.bisect_right(days_sorted, anchor)
            return days_sorted[idx] if idx < len(days_sorted) else None

        result = {1: {"calc_dates": calc_1, "adjust_dates": adjust_1}}
        prev_calc = calc_1
        prev_adjust = adjust_1
        for sub_n in range(2, 5):
            calc_n = [d2 for d in prev_calc if (d2 := next_trading_day(d)) is not None]
            adjust_n = [d2 for d in prev_adjust if (d2 := next_trading_day(d)) is not None]
            result[sub_n] = {"calc_dates": calc_n, "adjust_dates": adjust_n}
            prev_calc = calc_n
            prev_adjust = adjust_n
        return result

    def _calc_mid_weights(
        self,
        value_df: pd.DataFrame,
        t: pd.Timestamp,
        varieties: List[str],
        rev_series_cache: Dict[str, pd.Series],
        index_trading_days: List[pd.Timestamp],
        t_idx: int,
        returns_df: Optional[pd.DataFrame] = None,
        risk_series_cache: Optional[Dict[str, pd.Series]] = None,
    ) -> Dict[str, float]:
        t_dates = value_df.index[value_df.index <= t]
        eligible = []
        for variety in varieties:
            cfg = SUB_PORTFOLIOS.get(variety)
            if cfg is None:
                continue
            base = pd.Timestamp(cfg["sub_base_date"])
            history_count = len(t_dates[t_dates >= base])
            if history_count >= MIN_HISTORY_DAYS:
                eligible.append(variety)

        if not eligible:
            return {}

        rets_sub = returns_df[eligible] if returns_df is not None else None
        mom_scores = self.signal_calc.compute_momentum_scores(
            value_df[eligible], t, precomputed_returns=rets_sub
        )
        ret_22_tb: dict[str, float] = {}
        for variety in mom_scores.index:
            hist = value_df[variety].loc[:t].dropna()
            ret_22_tb[variety] = float(hist.iloc[-1] / hist.iloc[-23] - 1) if len(hist) >= 23 else 0.0
        top_mom_sorted = sorted(
            mom_scores.index.tolist(),
            key=lambda variety: (mom_scores[variety], ret_22_tb.get(variety, 0.0)),
            reverse=True,
        )
        top_mom: Set[str] = set(top_mom_sorted[:MOMENTUM_SELECT_N])

        rev_selected: Set[str] = set()
        for variety in eligible:
            rev_s = rev_series_cache.get(variety)
            if rev_s is not None and self.signal_calc.is_reversal_selected_from_series(
                rev_s, t, value_df[variety]
            ):
                rev_selected.add(variety)

        tvs_vals: Dict[str, float] = {}
        for variety in eligible:
            val = np.nan
            if risk_series_cache and variety in risk_series_cache:
                tvs_s = risk_series_cache[variety]
                if t in tvs_s.index:
                    val = tvs_s.loc[t]
            tvs_vals[variety] = 0.0 if pd.isna(val) else float(val)

        n_mom = len(top_mom)
        n_rev = len(rev_selected)
        eff_count = n_mom + n_rev / 2.0
        rb_unit = RB_BASE_RATE / eff_count if eff_count > 0 else 0.0

        init_weights: Dict[str, float] = {}
        for variety in eligible:
            in_mom = variety in top_mom
            in_rev = variety in rev_selected
            if in_mom and in_rev:
                rb = RB_MULT_BOTH * rb_unit
            elif in_mom:
                rb = RB_MULT_MOM * rb_unit
            elif in_rev:
                rb = RB_MULT_REV * rb_unit
            else:
                rb = 0.0

            if rb == 0.0:
                init_weights[variety] = 0.0
                continue

            tvs = tvs_vals[variety]
            if tvs <= 0:
                vol = (
                    self._vol_from_returns(returns_df, variety, t, window=21)
                    if returns_df is not None
                    else self._vol(value_df[variety], t, window=21)
                )
                vol_divisor = max(0.01, vol) if (vol and not np.isnan(vol) and vol > 0) else 0.01
            else:
                vols = [
                    self._vol_from_returns(returns_df, variety, t, window=w)
                    if returns_df is not None
                    else self._vol(value_df[variety], t, window=w)
                    for w in _TVS_VOL_WINDOWS
                ]
                valid = [x for x in vols if x and not np.isnan(x) and x > 0]
                vol_divisor = max([0.01] + valid)

            d_c = SUB_PORTFOLIOS[variety].get("direction", 1)
            init_weights[variety] = d_c * (rb / vol_divisor)

        adj_coef = self._vol_adjust_coef(value_df[eligible], init_weights, t, returns_df)

        mid_weights: Dict[str, float] = {}
        for variety in eligible:
            ub = SUB_PORTFOLIOS[variety].get("weight_ub", 1.0)
            raw_w = adj_coef * init_weights.get(variety, 0.0)
            mid_weights[variety] = float(np.sign(raw_w)) * min(abs(raw_w), ub)
        return mid_weights

    def _vol(self, value_series: pd.Series, t: pd.Timestamp, window: int = 22) -> float:
        v = value_series.loc[:t].dropna()
        if len(v) < window + 1:
            return np.nan
        ret = v.pct_change().iloc[-window:]
        return float(ret.std() * np.sqrt(260))

    @staticmethod
    def _vol_from_returns(
        returns_df: pd.DataFrame, variety: str, t: pd.Timestamp, window: int = 22
    ) -> float:
        if returns_df is None or variety not in returns_df.columns:
            return np.nan
        r = returns_df[variety].loc[:t].dropna()
        if len(r) < window:
            return np.nan
        return float(r.iloc[-window:].std() * np.sqrt(260))

    def _vol_adjust_coef(
        self,
        value_df: pd.DataFrame,
        init_weights: Dict[str, float],
        t: pd.Timestamp,
        returns_df: Optional[pd.DataFrame] = None,
    ) -> float:
        rets_list = []
        for variety, weight in init_weights.items():
            if weight == 0:
                continue
            col_df = returns_df if (returns_df is not None and variety in returns_df.columns) else None
            if col_df is not None:
                r = col_df[variety].loc[:t].dropna()
            elif variety in value_df.columns:
                r = value_df[variety].loc[:t].pct_change().dropna()
            else:
                continue
            rets_list.append(r * weight)

        if not rets_list:
            return 1.0

        port_ret = sum(rets_list)
        port_vol = max(
            (port_ret.iloc[-win:].std() * np.sqrt(252) if len(port_ret) >= win else 0.0)
            for win in VOL_WINDOWS
        )
        if port_vol <= 0 or port_vol <= WAF_TARGET_VOL:
            return 1.0
        return WAF_TARGET_VOL / port_vol
