"""GMAT3 原始数据接入层。"""

from __future__ import annotations

import bisect
import calendar as _calendar_mod
from pathlib import Path

import pandas as pd

from data.sources.parquet_source import ParquetSource
from data.sources.csv_source import CSVSource

from .config import DEFAULT_MARKET_DATA_ROOT, DEFAULT_GMAT3_STRATEGY_DATA_ROOT
from .universe import (
    BRENT_MONTHLY_DELIVERY,
    BLACK_COMPONENTS,
    DOMESTIC_VARIETY_GROUP,
    OVERSEAS_DAILY_FILES,
    ROLL_PARAMS,
    SUB_PORTFOLIOS,
)


class GMAT3DataAccess:
    """基于 `cta_lab` DataSource 的 GMAT3 原始数据访问对象。

    数据来源分两处：
      - market_data_root（market_data/）：交易日历、国内外日线、替代标的、汇率
      - strategy_data_root（strategy_data/gmat3/）：国内外合约信息
    """

    def __init__(
        self,
        market_data_root: str | Path = DEFAULT_MARKET_DATA_ROOT,
        strategy_data_root: str | Path = DEFAULT_GMAT3_STRATEGY_DATA_ROOT,
    ) -> None:
        self.market_data_root = Path(market_data_root)
        self.strategy_data_root = Path(strategy_data_root)
        self.mkt_source = ParquetSource(self.market_data_root)
        self.strat_source = ParquetSource(self.strategy_data_root)

        self._calendar_by_exchange: dict[str, list[pd.Timestamp]] = {}
        self._dom_info: pd.DataFrame | None = None
        self._ovs_info: pd.DataFrame | None = None
        self._dom_info_by_variety: dict[str, pd.DataFrame] = {}
        self._ovs_info_by_variety: dict[str, pd.DataFrame] = {}
        self._dom_daily_raw: dict[str, pd.DataFrame] = {}   # variety -> df
        self._ovs_daily_raw: dict[str, pd.DataFrame] = {}   # variety -> df
        self._substitute_prices: pd.DataFrame | None = None
        self._fx: pd.Series | None = None

        self._load_calendar()
        self._load_contract_info()

    # ------------------------------------------------------------------
    # calendar
    # ------------------------------------------------------------------

    def _load_calendar(self) -> None:
        df = self.mkt_source.read_dataframe("calendar/china_trading_calendar")
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.normalize()
        for exchange, grp in df.groupby("exchange"):
            self._calendar_by_exchange[str(exchange)] = sorted(grp["trade_date"].unique().tolist())

    def trading_days(self, exchange: str) -> list[pd.Timestamp]:
        return self._calendar_by_exchange[exchange]

    def trading_days_between(
        self,
        exchange: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        inclusive: str = "both",
    ) -> list[pd.Timestamp]:
        days = self._calendar_by_exchange[exchange]
        lo = bisect.bisect_left(days, start)
        hi = bisect.bisect_right(days, end)
        result = list(days[lo:hi])
        if inclusive in ("neither", "right") and result and result[0] == start:
            result = result[1:]
        if inclusive in ("neither", "left") and result and result[-1] == end:
            result = result[:-1]
        return result

    def nth_trading_day(
        self,
        exchange: str,
        anchor: pd.Timestamp,
        n: int,
    ) -> pd.Timestamp | None:
        days = self._calendar_by_exchange[exchange]
        if n > 0:
            idx = bisect.bisect_right(days, anchor) + n - 1
        else:
            idx = bisect.bisect_left(days, anchor) + n
        return days[idx] if 0 <= idx < len(days) else None

    def nth_month_trading_day(
        self, exchange: str, year: int, month: int, n: int
    ) -> pd.Timestamp | None:
        last_day = _calendar_mod.monthrange(year, month)[1]
        month_days = self.trading_days_between(
            exchange,
            pd.Timestamp(year, month, 1),
            pd.Timestamp(year, month, last_day),
        )
        if not month_days:
            return None
        try:
            return month_days[n - 1] if n > 0 else month_days[n]
        except IndexError:
            return None

    # ------------------------------------------------------------------
    # contract info
    # ------------------------------------------------------------------

    def _load_contract_info(self) -> None:
        # 国内合约信息（strategy_data/gmat3/contract_info_domestic.parquet）
        dom = self.strat_source.read_dataframe("contract_info_domestic").copy()
        dom = dom.rename(columns={"wind_code": "contract_id"})
        dom["market"] = "domestic"
        dom["contract_id"] = dom["contract_id"].astype(str)
        dom["list_date"] = pd.to_datetime(dom["list_date"]).dt.normalize()
        dom["last_trade_date"] = pd.to_datetime(dom["last_trade_date"]).dt.normalize()
        dom["delivery_ym"] = dom["delivery_month"].apply(self._parse_dom_delivery_ym)
        dom["last_holding_date"] = dom.apply(self._calc_last_holding, axis=1)
        self._dom_info = dom

        # 海外合约信息（strategy_data/gmat3/contract_info_overseas.parquet）
        ovs = self.strat_source.read_dataframe("contract_info_overseas").copy()
        ovs = ovs.rename(columns={"FutCode": "contract_id", "LastTrdDate": "last_trade_date"})
        ovs["market"] = "overseas"
        ovs["list_date"] = pd.to_datetime(ovs["StartDate"]).dt.normalize()
        ovs["last_trade_date"] = pd.to_datetime(ovs["last_trade_date"]).dt.normalize()
        ovs["delivery_ym"] = ovs.apply(self._parse_ovs_delivery_ym, axis=1)
        ovs["last_holding_date"] = ovs.apply(self._calc_last_holding, axis=1)
        self._ovs_info = ovs

    @staticmethod
    def _parse_dom_delivery_ym(delivery_month) -> tuple[int, int] | None:
        try:
            s = str(int(delivery_month))
            return int(s[:4]), int(s[4:6])
        except Exception:
            return None

    @staticmethod
    def _parse_ovs_delivery_ym(row: pd.Series) -> tuple[int, int] | None:
        try:
            cd = str(row.get("ContrDate", ""))
            if len(cd) == 4:
                mm = int(cd[:2])
                yy = int(cd[2:])
                year = 2000 + yy if yy < 50 else 1900 + yy
                return year, mm
        except Exception:
            pass
        ldt = row.get("last_trade_date")
        if pd.notna(ldt):
            return ldt.year, ldt.month
        return None

    def _calc_last_holding(self, row: pd.Series) -> pd.Timestamp | None:
        variety = row.get("variety")
        if variety not in ROLL_PARAMS:
            return row.get("last_trade_date")

        rule = ROLL_PARAMS[variety]["last_holding_rule"]
        if rule in ("last_trade_date", "monthly_calendar"):
            return row.get("last_trade_date")

        delivery_ym = row.get("delivery_ym")
        if delivery_ym is None:
            return row.get("last_trade_date")
        delivery_year, delivery_month = delivery_ym

        if rule == "prev_1_month_last":
            target_year = delivery_year - 1 if delivery_month == 1 else delivery_year
            target_month = 12 if delivery_month == 1 else delivery_month - 1
            use_14th = False
        elif rule == "prev_2_month_last":
            target_year = delivery_year
            target_month = delivery_month - 2
            if target_month <= 0:
                target_month += 12
                target_year -= 1
            use_14th = False
        elif rule == "prev_1_month_14th":
            target_year = delivery_year - 1 if delivery_month == 1 else delivery_year
            target_month = 12 if delivery_month == 1 else delivery_month - 1
            use_14th = True
        else:
            return row.get("last_trade_date")

        cfg = SUB_PORTFOLIOS.get(variety) or BLACK_COMPONENTS.get(variety)
        if cfg is None:
            return row.get("last_trade_date")
        exchange = cfg["exchange"]

        month_days = self.trading_days_between(
            exchange,
            pd.Timestamp(target_year, target_month, 1),
            pd.Timestamp(target_year, target_month, _calendar_mod.monthrange(target_year, target_month)[1]),
        )
        if not month_days:
            return row.get("last_trade_date")

        if use_14th:
            return month_days[13] if len(month_days) > 13 else month_days[-1]
        return month_days[-1]

    def get_contract_info(self, variety: str) -> pd.DataFrame:
        if variety in DOMESTIC_VARIETY_GROUP or variety in BLACK_COMPONENTS:
            if variety not in self._dom_info_by_variety:
                assert self._dom_info is not None
                self._dom_info_by_variety[variety] = (
                    self._dom_info[self._dom_info["variety"] == variety]
                    .copy()
                    .reset_index(drop=True)
                )
            return self._dom_info_by_variety[variety]

        if variety in OVERSEAS_DAILY_FILES:
            if variety not in self._ovs_info_by_variety:
                assert self._ovs_info is not None
                self._ovs_info_by_variety[variety] = (
                    self._ovs_info[self._ovs_info["variety"] == variety]
                    .copy()
                    .reset_index(drop=True)
                )
            return self._ovs_info_by_variety[variety]

        raise ValueError(f"Unknown variety: {variety}")

    # ------------------------------------------------------------------
    # daily klines
    # ------------------------------------------------------------------

    def _load_dom_daily(self, variety: str) -> pd.DataFrame:
        """按品种从 market_data/kline/china_daily_full/ 加载国内日线。"""
        if variety not in self._dom_daily_raw:
            df = self.mkt_source.read_dataframe(f"kline/china_daily_full/{variety}").copy()
            # market_data 用 contract_code / interest，统一映射为 contract_id / open_interest
            df = df.rename(columns={"contract_code": "contract_id", "interest": "open_interest"})
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.normalize()
            df["contract_id"] = df["contract_id"].astype(str)
            df["variety"] = variety
            self._dom_daily_raw[variety] = df
        return self._dom_daily_raw[variety]

    def _load_ovs_daily(self, variety: str) -> pd.DataFrame:
        """从 market_data/kline/overseas_daily/ 加载海外日线。"""
        if variety not in self._ovs_daily_raw:
            df = self.mkt_source.read_dataframe(f"kline/overseas_daily/daily_{variety}").copy()
            # market_data 列名：FutContrID/TradeDate/SettlePrice/OpenInterest（首字母大写）
            # 统一映射为内部标准列名
            df = df.rename(columns={
                "FutContrID": "contract_id",
                "TradeDate": "trade_date",
                "SettlePrice": "settle_price",
                "OpenInterest": "open_interest",
                "OpenPrice": "open_price",
                "HighPrice": "high_price",
                "LowPrice": "low_price",
                "Volume": "volume",
            })
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.normalize()
            df["variety"] = variety
            self._ovs_daily_raw[variety] = df
        return self._ovs_daily_raw[variety]

    def get_daily(self, variety: str) -> pd.DataFrame:
        if variety in DOMESTIC_VARIETY_GROUP or variety in BLACK_COMPONENTS:
            return self._load_dom_daily(variety).copy()

        if variety in OVERSEAS_DAILY_FILES:
            return self._load_ovs_daily(variety).copy()

        raise ValueError(f"Unknown variety: {variety}")

    def get_oi_on_date(self, variety: str, date: pd.Timestamp) -> pd.Series:
        daily = self.get_daily(variety)
        sub = daily.loc[daily["trade_date"] == date, ["contract_id", "open_interest"]].dropna()
        return sub.set_index("contract_id")["open_interest"]

    # ------------------------------------------------------------------
    # substitute / fx
    # ------------------------------------------------------------------

    def _load_substitute_prices(self) -> None:
        if self._substitute_prices is None:
            df = self.mkt_source.read_dataframe("kline/china_daily/substitute_indices").copy()
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.normalize()
            df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
            self._substitute_prices = df

    def get_substitute_price(self, wind_code: str) -> pd.Series:
        self._load_substitute_prices()
        assert self._substitute_prices is not None
        sub = self._substitute_prices[self._substitute_prices["wind_code"] == wind_code]
        return sub.set_index("trade_date")["close_price"].sort_index()

    def _load_fx(self) -> None:
        if self._fx is None:
            # market_data/fx/usdcny.csv，列：day, mid
            fx_path = self.market_data_root / "fx" / "usdcny.csv"
            df = pd.read_csv(fx_path, parse_dates=["day"])
            df["day"] = pd.to_datetime(df["day"]).dt.normalize()
            s = pd.to_numeric(df["mid"], errors="coerce")
            s.index = df["day"]
            self._fx = s.sort_index().ffill()

    def get_fx_rate(self) -> pd.Series:
        self._load_fx()
        assert self._fx is not None
        return self._fx

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def get_sub_portfolio_meta(self, variety: str) -> dict:
        if variety in SUB_PORTFOLIOS:
            return SUB_PORTFOLIOS[variety]
        if variety in BLACK_COMPONENTS:
            return BLACK_COMPONENTS[variety]
        raise ValueError(f"Unknown variety: {variety}")
