"""GMAT3 信号计算。"""

from __future__ import annotations

import numpy as np
import pandas as pd


REVERSAL_MA_MAX = 260
REVERSAL_LOW_THRESH = 0.1
REVERSAL_HIGH_THRESH = 0.35
REVERSAL_DELTA = 0.005


class SignalCalculator:
    """GMAT3 动量 / 反转 / 风险信号计算器。"""

    def __init__(self, vol_window: int = 22):
        self.vol_window = vol_window

    def compute_momentum_scores(
        self,
        value_df: pd.DataFrame,
        t: pd.Timestamp,
        precomputed_returns: pd.DataFrame | None = None,
    ) -> pd.Series:
        val_t = value_df.loc[:t]
        ret_t = precomputed_returns.loc[:t] if precomputed_returns is not None else val_t.pct_change()

        mom_factors: dict[str, list[float]] = {}
        for variety in val_t.columns:
            v = val_t[variety].dropna()
            if len(v) < 261:
                continue

            v_t = v.iloc[-1]
            v_22 = v.iloc[-23] if len(v) > 22 else np.nan
            v_260 = v.iloc[-261] if len(v) > 260 else np.nan

            if variety in ret_t.columns:
                r_series = ret_t[variety].loc[v.index].dropna()
            else:
                r_series = v.pct_change().dropna()

            ret_22 = r_series.iloc[-22:]
            ret_260 = r_series.iloc[-260:]

            vol_22 = ret_22.std() * np.sqrt(260) if len(ret_22) >= 5 else np.nan
            mom1 = (v_t / v_22 - 1) / vol_22 if (vol_22 and not np.isnan(vol_22) and not np.isnan(v_22)) else np.nan

            mom2 = v_t / v_260 - 1 if not np.isnan(v_260) else np.nan

            vol_260 = ret_260.std() * np.sqrt(260) if len(ret_260) >= 20 else np.nan
            mom3 = (v_t / v_260 - 1) / vol_260 if (vol_260 and not np.isnan(vol_260) and not np.isnan(v_260)) else np.nan

            v_window = v.iloc[-261:]
            v_min, v_max = v_window.min(), v_window.max()
            mom4 = (v_t - v_min) / (v_max - v_min) if v_max > v_min else 0.5

            mom_factors[variety] = [mom1, mom2, mom3, mom4]

        if not mom_factors:
            return pd.Series(dtype=float)

        factor_df = pd.DataFrame(mom_factors, index=["mom1", "mom2", "mom3", "mom4"]).T
        ranked = factor_df.rank(method="average", na_option="keep")
        scores = ranked.mean(axis=1)
        return scores.sort_values(ascending=False)

    @staticmethod
    def compute_reversal_series(value_series: pd.Series) -> pd.Series:
        v = value_series.dropna()
        n = len(v)
        if n == 0:
            return pd.Series(np.nan, index=value_series.index)

        vals = v.values
        ma_matrix = np.full((n, REVERSAL_MA_MAX), np.nan)
        cumsum = np.cumsum(vals)
        for k in range(1, REVERSAL_MA_MAX + 1):
            ma_matrix[k - 1 :, k - 1] = (
                (cumsum[k - 1 :] - np.concatenate([[0], cumsum[: n - k]])) / k
            )

        ma_long = ma_matrix[:, REVERSAL_MA_MAX - 1 : REVERSAL_MA_MAX]
        ma_short = ma_matrix[:, : REVERSAL_MA_MAX - 1]
        g_matrix = (ma_short >= ma_long).astype(float)
        rev_vals = g_matrix.sum(axis=1) / (REVERSAL_MA_MAX - 1)
        rev_vals[:REVERSAL_MA_MAX] = np.nan

        result = pd.Series(np.nan, index=value_series.index)
        result.loc[v.index] = rev_vals
        return result

    def is_reversal_selected_from_series(
        self,
        rev_series: pd.Series,
        t: pd.Timestamp,
        value_series: pd.Series,
        direction: int = 1,
    ) -> bool:
        if t not in rev_series.index:
            return False
        rev_t = rev_series.loc[t]
        if np.isnan(rev_t):
            return False

        history = rev_series.loc[:t].dropna()
        v_hist = value_series.loc[:t].dropna()
        if len(v_hist) < 5:
            return False
        before_t = history.iloc[:-1]

        if direction == 1:
            if rev_t > REVERSAL_HIGH_THRESH:
                return False
            above_high = before_t[before_t > REVERSAL_HIGH_THRESH]
            judge_win = history.loc[above_high.index[-1] :] if len(above_high) > 0 else history
            if judge_win.empty:
                return False
            if not (judge_win <= REVERSAL_LOW_THRESH).any():
                return False
            if rev_t < float(judge_win.min()) + REVERSAL_DELTA:
                return False
            if float(v_hist.iloc[-1]) < float(v_hist.iloc[-5:].mean()):
                return False
            return True

        high_short = 1 - REVERSAL_HIGH_THRESH
        low_short = 1 - REVERSAL_LOW_THRESH
        if rev_t < high_short:
            return False
        below_high = before_t[before_t < high_short]
        judge_win = history.loc[below_high.index[-1] :] if len(below_high) > 0 else history
        if judge_win.empty:
            return False
        if not (judge_win >= low_short).any():
            return False
        if rev_t > float(judge_win.max()) - REVERSAL_DELTA:
            return False
        if float(v_hist.iloc[-1]) > float(v_hist.iloc[-5:].mean()):
            return False
        return True

    @staticmethod
    def compute_risk_series(value_series: pd.Series, d: float = 1.0) -> pd.Series:
        v = value_series.dropna()
        if len(v) == 0:
            return pd.Series(np.nan, index=value_series.index)
        r = v.pct_change()
        sigma = r.ewm(span=44, adjust=False).std()
        sp = r / sigma.replace(0, np.nan)
        tvs = sp.rolling(260, min_periods=260).corr(sigma) * d
        result = pd.Series(np.nan, index=value_series.index)
        result.loc[tvs.index] = tvs.values
        return result
