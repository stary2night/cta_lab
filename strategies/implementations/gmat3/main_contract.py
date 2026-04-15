"""GMAT3 主力合约序列。"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from .data_access import GMAT3DataAccess
from .universe import (
    BRENT_MONTHLY_DELIVERY,
    BLACK_COMPONENTS,
    ROLL_PARAMS,
    SUB_PORTFOLIOS,
)

_QUARTERLY_MONTHS = {3, 6, 9, 12}


def _is_quarterly(delivery_ym) -> bool:
    return delivery_ym is not None and delivery_ym[1] in _QUARTERLY_MONTHS


class MainContractEngine:
    """计算 GMAT3 口径主力合约序列。"""

    def __init__(self, access: GMAT3DataAccess):
        self.access = access

    def compute(self, variety: str, end: str | pd.Timestamp | None = None) -> pd.DataFrame:
        cfg = SUB_PORTFOLIOS.get(variety) or BLACK_COMPONENTS.get(variety)
        if cfg is None:
            raise ValueError(f"Unknown variety: {variety}")

        ctype = cfg["contract_type"]
        if ctype == "domestic_equity":
            return self._domestic_equity(variety, cfg, end=end)
        if ctype == "domestic_bond":
            return self._domestic_bond(variety, cfg, end=end)
        if ctype in ("domestic_commodity", "black_series"):
            return self._domestic_commodity(variety, cfg, end=end)
        if ctype in ("overseas_equity", "overseas_bond"):
            return self._overseas_window(variety, cfg, end=end)
        if ctype == "overseas_commodity":
            return self._overseas_lco(variety, cfg, end=end)
        raise ValueError(f"Unsupported contract_type: {ctype}")

    @staticmethod
    def _make_row(date, old_cid, new_cid, old_w, new_w) -> dict:
        return {
            "trade_date": date,
            "old_contract": old_cid,
            "new_contract": new_cid,
            "old_weight": old_w,
            "new_weight": new_w,
        }

    @staticmethod
    def _no_roll_row(date, main_cid) -> dict:
        return {
            "trade_date": date,
            "old_contract": np.nan,
            "new_contract": main_cid,
            "old_weight": 0.0,
            "new_weight": 1.0,
        }

    @staticmethod
    def _finalize(rows: list[dict]) -> pd.DataFrame:
        columns = [
            "trade_date",
            "old_contract",
            "new_contract",
            "old_weight",
            "new_weight",
        ]
        df = pd.DataFrame(rows, columns=columns)
        if df.empty:
            return df
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df.reset_index(drop=True)

    @staticmethod
    def _nearest_active_contract(contracts: pd.DataFrame, date: pd.Timestamp):
        active = contracts[contracts["last_trade_date"] >= date]
        if active.empty:
            return None
        return active.iloc[0]

    @staticmethod
    def _nearest_active_contract_lhd(contracts: pd.DataFrame, date: pd.Timestamp):
        active = contracts[contracts["last_holding_date"].notna() & (contracts["last_holding_date"] >= date)]
        if active.empty:
            active = contracts[contracts["last_trade_date"] >= date]
        if active.empty:
            return None
        return active.iloc[0]

    @staticmethod
    def _next_quarterly(contracts: pd.DataFrame, after_ltd: pd.Timestamp):
        later = contracts[contracts["last_trade_date"] > after_ltd]
        if later.empty:
            return None
        return later.iloc[0]["contract_id"]

    @staticmethod
    def _max_oi_quarterly_contract(
        quarterly: pd.DataFrame,
        oi_dict: dict,
        date: pd.Timestamp,
        n_candidates: int = 2,
    ):
        active = quarterly[quarterly["last_trade_date"] >= date].head(n_candidates)
        if active.empty:
            return None
        best_oi = -1.0
        best_row = None
        for _, row in active.iterrows():
            oi = oi_dict.get(row["contract_id"], 0.0) or 0.0
            if oi > best_oi:
                best_oi = oi
                best_row = row
        return best_row

    @staticmethod
    def _max_oi_contract(
        oi_dict: dict,
        info: pd.DataFrame,
        min_ltd: Optional[pd.Timestamp] = None,
        strict_gt: bool = False,
    ) -> Optional[str]:
        if not oi_dict:
            return None
        ltd_series = info.set_index("contract_id")["last_trade_date"]
        candidates = []
        for cid, oi in oi_dict.items():
            if cid not in ltd_series.index:
                continue
            ltd = ltd_series[cid]
            if min_ltd is not None:
                if strict_gt and ltd <= min_ltd:
                    continue
                if (not strict_gt) and ltd < min_ltd:
                    continue
            if oi and oi > 0:
                candidates.append((oi, ltd, cid))
        if not candidates:
            return None
        candidates.sort(key=lambda x: (-x[0], x[1]))
        return candidates[0][2]

    def _domestic_equity(
        self, variety: str, cfg: dict, end: str | pd.Timestamp | None = None
    ) -> pd.DataFrame:
        roll_days = int(ROLL_PARAMS[variety]["roll_days"])
        exchange = cfg["exchange"]
        base_date = pd.Timestamp(cfg["futures_base_date"])
        info = self.access.get_contract_info(variety)
        quarterly = info[info["delivery_ym"].apply(_is_quarterly)].sort_values("last_trade_date").reset_index(drop=True)
        end_ts = pd.Timestamp(end) if end is not None else quarterly["last_trade_date"].max()
        trading_days = self.access.trading_days_between(
            exchange, base_date, min(end_ts, quarterly["last_trade_date"].max())
        )
        if not trading_days:
            return self._finalize([])
        current = self._nearest_active_contract(quarterly, base_date)
        if current is None:
            return self._finalize([])
        rows = []
        roll_state: dict | None = None
        for t in trading_days:
            if roll_state is not None:
                n = roll_state["n"]
                N = roll_state["total"]
                rows.append(self._make_row(t, current["contract_id"], roll_state["new_cid"], (N - n) / N, n / N))
                roll_state["n"] += 1
                if roll_state["n"] >= N:
                    current = quarterly[quarterly["contract_id"] == roll_state["new_cid"]].iloc[0]
                    roll_state = None
            else:
                lhd = current["last_holding_date"]
                if pd.notna(lhd):
                    cnt = len(self.access.trading_days_between(exchange, t, lhd, inclusive="left"))
                    if cnt == roll_days:
                        new_cid = self._next_quarterly(quarterly, current["last_trade_date"])
                        if new_cid is not None:
                            roll_state = {"new_cid": new_cid, "n": 0, "total": roll_days}
                if roll_state is not None:
                    rows.append(self._make_row(t, current["contract_id"], roll_state["new_cid"], 1.0, 0.0))
                    roll_state["n"] = 1
                    if roll_days == 1:
                        current = quarterly[quarterly["contract_id"] == roll_state["new_cid"]].iloc[0]
                        roll_state = None
                else:
                    rows.append(self._no_roll_row(t, current["contract_id"]))
        return self._finalize(rows)

    def _domestic_bond(
        self, variety: str, cfg: dict, end: str | pd.Timestamp | None = None
    ) -> pd.DataFrame:
        roll_days = int(ROLL_PARAMS[variety]["roll_days"])
        exchange = cfg["exchange"]
        base_date = pd.Timestamp(cfg["futures_base_date"])
        info = self.access.get_contract_info(variety)
        quarterly = info[info["delivery_ym"].apply(_is_quarterly)].sort_values("last_trade_date").reset_index(drop=True)
        daily = self.access.get_daily(variety)
        oi_map: dict = {}
        for row in daily[["trade_date", "contract_id", "open_interest"]].itertuples(index=False):
            oi_map.setdefault(row.trade_date, {})[row.contract_id] = row.open_interest
        end_ts = pd.Timestamp(end) if end is not None else quarterly["last_trade_date"].max()
        trading_days = self.access.trading_days_between(
            exchange, base_date, min(end_ts, quarterly["last_trade_date"].max())
        )
        if not trading_days:
            return self._finalize([])
        prev_date = self.access.nth_trading_day(exchange, base_date, -1)
        current = self._max_oi_quarterly_contract(quarterly, oi_map.get(prev_date, {}), base_date, n_candidates=2)
        if current is None:
            current = self._nearest_active_contract(quarterly, base_date)
        if current is None:
            return self._finalize([])
        rows = []
        roll_state = None
        oi_ratio_thresh = 0.5
        for i, t in enumerate(trading_days):
            prev_oi = oi_map.get(trading_days[i - 1] if i > 0 else t, {})
            if roll_state is not None:
                n, N = roll_state["n"], roll_state["total"]
                rows.append(self._make_row(t, current["contract_id"], roll_state["new_cid"], (N - n) / N, n / N))
                roll_state["n"] += 1
                if roll_state["n"] >= N:
                    current = quarterly[quarterly["contract_id"] == roll_state["new_cid"]].iloc[0]
                    roll_state = None
            else:
                trigger = False
                lhd = current["last_holding_date"]
                if pd.notna(lhd):
                    cnt = len(self.access.trading_days_between(exchange, t, lhd, inclusive="left"))
                    if cnt <= roll_days:
                        trigger = True
                if not trigger:
                    near_oi = prev_oi.get(current["contract_id"], 0.0) or 0.0
                    far_cid = self._next_quarterly(quarterly, current["last_trade_date"])
                    if far_cid is not None and near_oi > 0:
                        far_oi = prev_oi.get(far_cid, 0.0) or 0.0
                        if far_oi / near_oi > oi_ratio_thresh:
                            trigger = True
                if trigger:
                    new_cid = self._next_quarterly(quarterly, current["last_trade_date"])
                    if new_cid is not None:
                        roll_state = {"new_cid": new_cid, "n": 0, "total": roll_days}
                if roll_state is not None:
                    rows.append(self._make_row(t, current["contract_id"], roll_state["new_cid"], 1.0, 0.0))
                    roll_state["n"] = 1
                    if roll_days == 1:
                        current = quarterly[quarterly["contract_id"] == roll_state["new_cid"]].iloc[0]
                        roll_state = None
                else:
                    rows.append(self._no_roll_row(t, current["contract_id"]))
        return self._finalize(rows)

    def _domestic_commodity(
        self, variety: str, cfg: dict, end: str | pd.Timestamp | None = None
    ) -> pd.DataFrame:
        roll_days = int(ROLL_PARAMS[variety]["roll_days"])
        exchange = cfg["exchange"]
        base_date = pd.Timestamp(cfg["futures_base_date"])
        info = self.access.get_contract_info(variety).sort_values("last_trade_date").reset_index(drop=True)
        daily = self.access.get_daily(variety)
        oi_map: dict = {}
        for row in daily[["trade_date", "contract_id", "open_interest"]].itertuples(index=False):
            oi_map.setdefault(row.trade_date, {})[row.contract_id] = row.open_interest
        lhd_map = info.set_index("contract_id")["last_holding_date"].to_dict()
        ltd_map = info.set_index("contract_id")["last_trade_date"].to_dict()
        end_ts = pd.Timestamp(end) if end is not None else info["last_trade_date"].max()
        trading_days = self.access.trading_days_between(
            exchange, base_date, min(end_ts, info["last_trade_date"].max())
        )
        if not trading_days:
            return self._finalize([])
        start_idx = 0
        current_cid = None
        for start_idx, t0 in enumerate(trading_days):
            prev_date_0 = self.access.nth_trading_day(exchange, t0, -1)
            prev_oi_0 = oi_map.get(prev_date_0, {})
            cid = self._max_oi_contract(prev_oi_0, info, min_ltd=t0)
            if cid is None:
                cid = self._max_oi_contract(oi_map.get(t0, {}), info, min_ltd=t0)
            if cid is not None:
                current_cid = cid
                break
        if current_cid is None:
            return self._finalize([])
        rows = []
        roll_state = None
        for i, t in enumerate(trading_days):
            if i < start_idx:
                continue
            prev_oi_dict = oi_map.get(trading_days[i - 1] if i > 0 else t, {})
            current_lhd = lhd_map.get(current_cid)
            current_ltd = ltd_map.get(current_cid)
            if roll_state is not None:
                n, N = roll_state["n"], roll_state["total"]
                rows.append(self._make_row(t, current_cid, roll_state["new_cid"], (N - n) / N, n / N))
                roll_state["n"] += 1
                if roll_state["n"] >= N:
                    current_cid = roll_state["new_cid"]
                    roll_state = None
            else:
                days_left = len(self.access.trading_days_between(exchange, t, current_lhd, inclusive="left")) if pd.notna(current_lhd) else 999
                if days_left > roll_days:
                    candidate_cid = self._max_oi_contract(prev_oi_dict, info, min_ltd=current_ltd)
                else:
                    candidate_cid = self._max_oi_contract(prev_oi_dict, info, min_ltd=current_ltd, strict_gt=True)
                if candidate_cid is not None and candidate_cid != current_cid:
                    roll_state = {"new_cid": candidate_cid, "n": 0, "total": roll_days}
                if roll_state is not None:
                    rows.append(self._make_row(t, current_cid, roll_state["new_cid"], 1.0, 0.0))
                    roll_state["n"] = 1
                    if roll_days == 1:
                        current_cid = roll_state["new_cid"]
                        roll_state = None
                else:
                    rows.append(self._no_roll_row(t, current_cid))
        return self._finalize(rows)

    def _overseas_window(
        self, variety: str, cfg: dict, end: str | pd.Timestamp | None = None
    ) -> pd.DataFrame:
        roll_days = int(ROLL_PARAMS[variety]["roll_days"])
        roll_window = int(ROLL_PARAMS[variety]["roll_window"])
        exchange = cfg["exchange"]
        base_date = pd.Timestamp(cfg["futures_base_date"])
        info = self.access.get_contract_info(variety)
        quarterly = info[info["delivery_ym"].apply(_is_quarterly)].sort_values("last_trade_date").reset_index(drop=True)
        end_ts = pd.Timestamp(end) if end is not None else quarterly["last_trade_date"].max()
        trading_days = self.access.trading_days_between(
            exchange, base_date, min(end_ts, quarterly["last_trade_date"].max())
        )
        if not trading_days:
            return self._finalize([])
        current = self._nearest_active_contract_lhd(quarterly, base_date)
        if current is None:
            return self._finalize([])
        rows = []
        roll_state = None
        for t in trading_days:
            if roll_state is not None:
                n, N = roll_state["n"], roll_state["total"]
                rows.append(self._make_row(t, current["contract_id"], roll_state["new_cid"], (N - n) / N, n / N))
                roll_state["n"] += 1
                if roll_state["n"] >= N:
                    current = quarterly[quarterly["contract_id"] == roll_state["new_cid"]].iloc[0]
                    roll_state = None
            else:
                lhd = current["last_holding_date"]
                if pd.notna(lhd):
                    cnt = len(self.access.trading_days_between(exchange, t, lhd, inclusive="both"))
                    if cnt == roll_window:
                        new_cid = self._next_quarterly(quarterly, current["last_trade_date"])
                        if new_cid is not None:
                            roll_state = {"new_cid": new_cid, "n": 0, "total": roll_days}
                if roll_state is not None:
                    rows.append(self._make_row(t, current["contract_id"], roll_state["new_cid"], 1.0, 0.0))
                    roll_state["n"] = 1
                    if roll_days == 1:
                        current = quarterly[quarterly["contract_id"] == roll_state["new_cid"]].iloc[0]
                        roll_state = None
                else:
                    rows.append(self._no_roll_row(t, current["contract_id"]))
        return self._finalize(rows)

    def _overseas_lco(
        self, variety: str, cfg: dict, end: str | pd.Timestamp | None = None
    ) -> pd.DataFrame:
        roll_days = int(ROLL_PARAMS[variety]["roll_days"])
        exchange = cfg["exchange"]
        base_date = pd.Timestamp(cfg["futures_base_date"])
        info = self.access.get_contract_info(variety).sort_values("last_trade_date").reset_index(drop=True)
        end_ts = pd.Timestamp(end) if end is not None else info["last_trade_date"].max()
        trading_days = self.access.trading_days_between(
            exchange, base_date, min(end_ts, info["last_trade_date"].max())
        )
        if not trading_days:
            return self._finalize([])
        current = self._nearest_active_contract_lhd(info, base_date)
        if current is None:
            return self._finalize([])
        ym_to_cid = {}
        for _, row in info.iterrows():
            if row["delivery_ym"]:
                ym_to_cid[row["delivery_ym"]] = row["contract_id"]
        triggered_months: set = set()
        rows = []
        roll_state = None
        for t in trading_days:
            t_ym = (t.year, t.month)
            month_end = pd.Timestamp(t.year, t.month, 1) + pd.offsets.MonthEnd(0)
            month_days_so_far = self.access.trading_days_between(
                exchange,
                pd.Timestamp(t.year, t.month, 1),
                month_end,
            )
            is_5th = len(month_days_so_far) >= 5 and month_days_so_far[4] == t and t_ym not in triggered_months
            if roll_state is not None:
                n, N = roll_state["n"], roll_state["total"]
                rows.append(self._make_row(t, current["contract_id"], roll_state["new_cid"], (N - n) / N, n / N))
                roll_state["n"] += 1
                if roll_state["n"] >= N:
                    new_row = info[info["contract_id"] == roll_state["new_cid"]]
                    if not new_row.empty:
                        current = new_row.iloc[0]
                    roll_state = None
            else:
                if is_5th:
                    delivery_month = BRENT_MONTHLY_DELIVERY[t.month]
                    delivery_year = t.year if delivery_month > t.month else t.year + 1
                    new_cid = ym_to_cid.get((delivery_year, delivery_month))
                    if new_cid is not None and new_cid != current["contract_id"]:
                        triggered_months.add(t_ym)
                        roll_state = {"new_cid": new_cid, "n": 0, "total": roll_days}
                if roll_state is not None:
                    rows.append(self._make_row(t, current["contract_id"], roll_state["new_cid"], 1.0, 0.0))
                    roll_state["n"] = 1
                    if roll_days == 1:
                        new_row = info[info["contract_id"] == roll_state["new_cid"]]
                        if not new_row.empty:
                            current = new_row.iloc[0]
                        roll_state = None
                else:
                    rows.append(self._no_roll_row(t, current["contract_id"]))
        return self._finalize(rows)
