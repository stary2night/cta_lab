"""GMAT3 指数合成。"""

from __future__ import annotations

from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd

from .universe import SUB_PORTFOLIOS


INDEX_BASE_DATE = pd.Timestamp("2009-12-31")
INDEX_BASE_VALUE = 1000.0
TRADING_FEE_RATE = 0.0005
TRACKING_FEE_RATE = 0.005
VOL_RESET_THRESHOLD = 0.045
VOL_RESET_TARGET = 0.04
VOL_WINDOWS = [22, 65, 130]


class GMAT3IndexBuilder:
    """GMAT3 指数合成器。"""

    def compute(
        self,
        value_df: pd.DataFrame,
        weight_df: pd.DataFrame,
        *,
        index_trading_days,
        adjust_date_sets,
        fx_series: pd.Series | None = None,
    ) -> pd.Series:
        varieties = list(value_df.columns)
        base_date = INDEX_BASE_DATE
        trading_days = list(index_trading_days)
        n_days = len(trading_days)

        all_adjust_dates: Set[pd.Timestamp] = set()
        for dates in adjust_date_sets.values():
            all_adjust_dates |= set(dates)

        n_var = len(varieties)
        usd_mask = np.array(
            [SUB_PORTFOLIOS.get(v, {}).get("currency") == "USD" for v in varieties], dtype=bool
        )

        calc_idx = pd.DatetimeIndex(trading_days)
        if fx_series is not None and usd_mask.any():
            usdcny = (1.0 / fx_series).reindex(calc_idx, method="ffill").ffill().bfill()
            fx_array = usdcny.values.astype(float)
        else:
            fx_array = np.ones(n_days)

        val_matrix = np.full((n_days, n_var), np.nan)
        for j, variety in enumerate(varieties):
            if variety in value_df.columns:
                val_matrix[:, j] = value_df[variety].reindex(calc_idx).values

        ret_matrix = np.full((n_days, n_var), 0.0)
        with np.errstate(divide="ignore", invalid="ignore"):
            returns = np.where(val_matrix[:-1] > 0, val_matrix[1:] / val_matrix[:-1] - 1.0, 0.0)
        ret_matrix[1:] = np.where(np.isfinite(returns), returns, 0.0)

        h_cny = np.zeros(n_var)
        h_usd = np.zeros(n_var)
        accum_usd_pnl = np.zeros(n_var)

        index_vals = np.full(n_days, np.nan)
        index_val = INDEX_BASE_VALUE
        last_rebalance_val = INDEX_BASE_VALUE

        vrs_trigger_idx: Optional[int] = None
        vr_idx: Optional[int] = None
        max_vol_win = max(VOL_WINDOWS)
        port_ret_buffer = np.full(max_vol_win, np.nan)

        for i, t in enumerate(trading_days):
            if t <= base_date:
                index_vals[i] = index_val
                continue

            is_adjust = t in all_adjust_dates
            is_vr_day = vr_idx is not None and i == vr_idx
            prev_index_val = index_vals[i - 1] if i > 0 else INDEX_BASE_VALUE

            fx_t = fx_array[i]
            fx_prev = fx_array[i - 1] if i > 0 else fx_t
            day_ret = ret_matrix[i]

            cny_pnl = float(np.dot(h_cny, day_ret))
            today_usd_pnl_vec = h_usd * day_ret
            today_usd_pnl = float(np.sum(today_usd_pnl_vec))
            fx_remeasure = (fx_t - fx_prev) * float(np.sum(accum_usd_pnl))
            pnl = cny_pnl + fx_t * today_usd_pnl + fx_remeasure
            accum_usd_pnl += today_usd_pnl_vec

            if is_vr_day and vrs_trigger_idx is not None:
                vrs_val = self._vrs_from_buffer(port_ret_buffer)
                scale = min(1.0, VOL_RESET_TARGET / vrs_val) if vrs_val > 0 else 1.0
                new_h_cny = h_cny * scale
                new_h_usd = h_usd * scale
                old_cny_all = np.where(usd_mask, h_usd * fx_prev, h_cny)
                new_cny_all = old_cny_all * scale
                trading_cost = float(np.sum(np.abs(new_cny_all - old_cny_all)) * TRADING_FEE_RATE)
                h_cny = new_h_cny
                h_usd = new_h_usd
                accum_usd_pnl = np.zeros(n_var)
                vrs_trigger_idx = None
                vr_idx = None
            elif is_adjust:
                if t in weight_df.index:
                    row = weight_df.loc[t]
                    target_w = np.array([row[v] if v in row.index else 0.0 for v in varieties])
                else:
                    target_w = np.zeros(n_var)
                new_h_cny_all = target_w * prev_index_val
                old_h_cny_all = np.where(usd_mask, h_usd * fx_prev, h_cny)
                trading_cost = float(np.sum(np.abs(new_h_cny_all - old_h_cny_all)) * TRADING_FEE_RATE)
                h_cny = np.where(usd_mask, 0.0, new_h_cny_all)
                h_usd = np.where(usd_mask, new_h_cny_all / fx_prev, 0.0)
                accum_usd_pnl = np.zeros(n_var)
            else:
                h_cny = h_cny * (1.0 + day_ret)
                h_usd = h_usd * (1.0 + day_ret)
                trading_cost = 0.0

            t_prev = trading_days[i - 1] if i > 0 else t
            calendar_days = (t - t_prev).days
            tracking_fee = last_rebalance_val * TRACKING_FEE_RATE * calendar_days / 365.0

            index_val = prev_index_val + pnl - trading_cost - tracking_fee
            index_vals[i] = index_val

            if is_vr_day or is_adjust:
                last_rebalance_val = index_val

            port_r = pnl / prev_index_val if prev_index_val > 0 else 0.0
            port_ret_buffer = np.roll(port_ret_buffer, -1)
            port_ret_buffer[-1] = port_r

            if vr_idx is None and i >= VOL_WINDOWS[0]:
                vrs = self._vrs_from_buffer(port_ret_buffer)
                if vrs > VOL_RESET_THRESHOLD and i + 2 < n_days:
                    window_dates = trading_days[i + 1 : i + 3]
                    has_adjust = any(d in all_adjust_dates for d in window_dates)
                    if not has_adjust:
                        vrs_trigger_idx = i
                        vr_idx = i + 2

        result = pd.Series(index_vals, index=pd.DatetimeIndex(trading_days), name="GMAT3")
        return result.sort_index()

    @staticmethod
    def _vrs_from_buffer(port_ret_buffer: np.ndarray) -> float:
        valid = port_ret_buffer[~np.isnan(port_ret_buffer)]
        if len(valid) < VOL_WINDOWS[0]:
            return 0.0
        return max(
            float(valid[-win:].std() * np.sqrt(260)) if len(valid) >= win else 0.0
            for win in VOL_WINDOWS
        )
