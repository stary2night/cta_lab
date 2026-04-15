"""GMAT3 展期收益率。"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .data_access import GMAT3DataAccess


class RollReturnCalculator:
    """根据主力合约序列计算展期收益率。"""

    def __init__(self, access: GMAT3DataAccess):
        self.access = access

    def compute(self, variety: str, main_df: pd.DataFrame) -> pd.Series:
        daily = self.access.get_daily(variety)
        settle_map: dict = {}
        for cid, grp in daily.groupby("contract_id"):
            settle_map[cid] = grp.set_index("trade_date")["settle_price"].sort_index()

        returns = []
        prev_row = None
        for row in main_df.itertuples(index=False):
            t = row.trade_date
            if prev_row is None:
                returns.append((t, np.nan))
                prev_row = row
                continue
            t_prev = prev_row.trade_date
            if row.old_weight == 0.0:
                r = self._single_return(settle_map, row.new_contract, t, t_prev)
            else:
                r_old = self._single_return(settle_map, row.old_contract, t, t_prev)
                r_new = self._single_return(settle_map, row.new_contract, t, t_prev)
                r = np.nan if (np.isnan(r_old) or np.isnan(r_new)) else row.old_weight * r_old + row.new_weight * r_new
            returns.append((t, r))
            prev_row = row
        if not returns:
            return pd.Series(dtype=float, index=pd.DatetimeIndex([]), name=variety)
        dates, vals = zip(*returns)
        return pd.Series(vals, index=pd.DatetimeIndex(dates), name=variety)

    @staticmethod
    def _single_return(settle_map: dict, cid, t: pd.Timestamp, t_prev: pd.Timestamp) -> float:
        if cid not in settle_map or (isinstance(cid, float) and np.isnan(cid)):
            return np.nan
        s = settle_map[cid]
        p_t = s.get(t, np.nan)
        p_prev = s.get(t_prev, np.nan)
        if np.isnan(p_t) or np.isnan(p_prev) or p_prev == 0:
            return np.nan
        return p_t / p_prev - 1.0
