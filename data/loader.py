"""DataLoader — 统一数据服务入口，将原始 DataFrame 转换为领域对象。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .model import (
    AdjustMethod,
    BarSeries,
    Contract,
    ContractSchedule,
    ContinuousSeries,
    Instrument,
    InstrumentRegistry,
    MultiExchangeCalendar,
    OIMaxRoll,
    RollEvent,
    RollRule,
    StabilizedRule,
    TradingCalendar,
)
from .sources.base import DataSource


@dataclass
class KlineSchema:
    """K 线数据列名映射。描述实际存储中的列名，DataLoader 据此规范化为领域对象。"""

    # 日期：可以是 index（DatetimeIndex）或独立列
    date_col: str = "date"               # 若数据已以 DatetimeIndex 存储，此字段忽略
    # 合约代码：多合约混合表时用于过滤；单合约文件时可为 None
    contract_col: str | None = None
    # OHLC + 结算价
    open_col: str = "open"
    high_col: str = "high"
    low_col: str = "low"
    close_col: str = "close"
    settle_col: str = "settle"
    # 成交量 & 持仓量
    volume_col: str = "volume"
    oi_col: str = "open_interest"
    # 文件键前缀：read_dataframe(key) 中 key = key_prefix + symbol
    # 例：海外文件名为 daily_ES.parquet，key_prefix="daily_"
    key_prefix: str = ""
    # 合约代码是否为复合格式 "{symbol}_{id}"（如 "ES_195176"）
    # True 时从代码中解析 symbol 和原始 id 用于文件定位和行过滤
    compound_code: bool = False
    # 若 compound_code=True，contract_col 对应的原始数据类型（"int" 或 "str"）
    contract_col_dtype: str = "str"

    # ── 预置 Schema ──────────────────────────────────────────────────────

    @classmethod
    def default(cls) -> "KlineSchema":
        """cta_lab 标准列名（已预处理数据）。"""
        return cls()

    @classmethod
    def tushare(cls) -> "KlineSchema":
        """Tushare / cta 项目原始 dayKline 格式。
        多合约混合表，contract_code 列标识合约，trade_date 为日期列。
        """
        return cls(
            date_col="trade_date",
            contract_col="contract_code",
            open_col="open_price",
            high_col="high_price",
            low_col="low_price",
            close_col="close_price",
            settle_col="settle_price",
            volume_col="volume",
            oi_col="interest",
        )

    @classmethod
    def overseas(cls) -> "KlineSchema":
        """海外期货数据格式（FutContrID 整数标识，per-BaseTicker 文件）。
        文件命名：daily_{BaseTicker}.parquet，合约代码格式："{BaseTicker}_{FutContrID}"。
        无 ClosePrice，close_col 映射到 SettlePrice。
        """
        return cls(
            key_prefix="daily_",
            date_col="TradeDate",
            contract_col="FutContrID",
            contract_col_dtype="int",
            compound_code=True,
            open_col="OpenPrice",
            high_col="HighPrice",
            low_col="LowPrice",
            close_col="SettlePrice",
            settle_col="SettlePrice",
            volume_col="Volume",
            oi_col="OpenInterest",
        )


@dataclass
class ContractSchema:
    """合约基础信息列名映射。"""

    code_col: str = "code"
    exchange_col: str | None = "exchange"
    list_date_col: str = "list_date"
    expire_date_col: str = "expire_date"
    last_trade_date_col: str = "last_trade_date"
    # 按品种过滤时使用的列名；None 则用 code_col 做前缀匹配（国内默认行为）
    symbol_col: str | None = None
    # 若 symbol_col 不为 None，是否需要将 code 格式化为 "{symbol}_{raw_id}"
    # 海外场景：code_col=RawContractID(int)，需与 KlineSchema.compound_code 配合
    compound_code: bool = False

    @classmethod
    def default(cls) -> "ContractSchema":
        """cta_lab 标准列名。"""
        return cls()

    @classmethod
    def tushare(cls) -> "ContractSchema":
        """Tushare future_basic_info 格式。
        ts_code 列为带交易所后缀的合约代码（如 RB2410.SHF），与 kline parquet
        的 contract_code 列格式一致；exchange 同名；
        list_date / delist_date（到期日）/ last_ddate（最后交易日）。
        """
        return cls(
            code_col="ts_code",
            exchange_col="exchange",
            list_date_col="list_date",
            expire_date_col="delist_date",
            last_trade_date_col="last_ddate",
        )

    @classmethod
    def overseas(cls) -> "ContractSchema":
        """海外期货合约信息格式。
        按 BaseTicker 列过滤品种，合约代码格式化为 "{BaseTicker}_{RawContractID}"
        以与 KlineSchema.overseas() 的 compound_code 格式对齐。
        """
        return cls(
            code_col="RawContractID",
            exchange_col=None,
            list_date_col="StartDate",
            expire_date_col="SettleDate",
            last_trade_date_col="LastTradeDate",
            symbol_col="BaseTicker",
            compound_code=True,
        )


@dataclass
class InstrumentSchema:
    """品种静态信息列名映射。"""

    symbol_col: str = "symbol"
    name_col: str = "name"
    exchange_col: str = "exchange"
    currency_col: str = "currency"
    lot_size_col: str = "lot_size"
    tick_size_col: str = "tick_size"
    margin_rate_col: str = "margin_rate"
    default_currency: str = "CNY"
    default_tick_size: float = 1.0
    default_margin_rate: float = 0.1

    @classmethod
    def default(cls) -> "InstrumentSchema":
        """cta_lab 标准列名。"""
        return cls()

    @classmethod
    def china_from_contracts(cls) -> "InstrumentSchema":
        """从国内 contract_info 主表提取 instrument 元数据。"""
        return cls(
            symbol_col="fut_code",
            name_col="name",
            exchange_col="exchange",
            currency_col="currency",
            lot_size_col="per_unit",
            tick_size_col="tick_size",
            margin_rate_col="margin_rate",
            default_currency="CNY",
            default_tick_size=1.0,
            default_margin_rate=0.1,
        )

    @classmethod
    def overseas_from_contracts(cls) -> "InstrumentSchema":
        """从海外 contract_info 主表提取 instrument 元数据。"""
        return cls(
            symbol_col="BaseTicker",
            name_col="ParentName",
            exchange_col="exchange",
            currency_col="CurrencyCode",
            lot_size_col="lot_size",
            tick_size_col="tick_size",
            margin_rate_col="margin_rate",
            default_currency="USD",
            default_tick_size=1.0,
            default_margin_rate=0.1,
        )


class DataLoader:
    """统一数据服务入口：将原始 DataFrame 转换为领域对象。

    外部模块获取数据应通过 DataLoader，而非直接操作 DataSource。
    """

    def __init__(
        self,
        kline_source: DataSource,
        contract_source: DataSource | None = None,
        calendar_source: DataSource | None = None,
        instrument_source: DataSource | None = None,
        kline_schema: KlineSchema | None = None,
        contract_schema: ContractSchema | None = None,
        instrument_schema: InstrumentSchema | None = None,
        cache: bool = True,
    ) -> None:
        """初始化 DataLoader。

        contract_source / calendar_source 可独立注入；若为 None，则按约定路径回退到
        kline_source：
            - contracts/{symbol}
            - instruments/{symbol}
            - calendars/{exchange}
        kline_schema / contract_schema 为 None 时使用标准列名（KlineSchema.default()）。
        """
        self._kline_source = kline_source
        self._contract_source = contract_source
        self._calendar_source = calendar_source
        self._instrument_source = instrument_source
        self._kline_schema = kline_schema if kline_schema is not None else KlineSchema.default()
        self._contract_schema = contract_schema if contract_schema is not None else ContractSchema.default()
        self._instrument_schema = instrument_schema if instrument_schema is not None else InstrumentSchema.default()
        self._cache_enabled = cache
        self._cache: dict[tuple[Any, ...], Any] = {}

    # ------------------------------------------------------------------
    # 内部缓存辅助
    # ------------------------------------------------------------------

    def _get_cached(self, cache_key: tuple[Any, ...]) -> Any | None:
        """从缓存中取值，未命中返回 None。"""
        return self._cache.get(cache_key)

    def _set_cached(self, cache_key: tuple[Any, ...], value: Any) -> None:
        """将值存入缓存（仅当 cache=True 时生效）。"""
        if self._cache_enabled:
            self._cache[cache_key] = value

    # ------------------------------------------------------------------
    # Schema 规范化（内部辅助）
    # ------------------------------------------------------------------

    def _read_contract_dataframe(self, symbol: str) -> pd.DataFrame:
        """读取合约元数据。

        优先使用显式配置的 contract_source；否则回退到 kline_source 下约定路径
        contracts/{symbol}。
        """
        if self._contract_source is not None:
            return self._contract_source.read_dataframe(symbol)
        return self._kline_source.read_dataframe(f"contracts/{symbol}")

    def _read_instrument_dataframe(self, symbol: str) -> pd.DataFrame:
        """读取品种静态元数据。

        当前约定：
            - 显式配置 instrument_source 时，优先使用
            - 否则回退到 kline_source 下约定路径 instruments/{symbol}
            - 若 instrument 文件不存在且 contract_source 已配置，则回退到 contract_source
        """
        if self._instrument_source is not None:
            return self._instrument_source.read_dataframe(symbol)
        instrument_key = f"instruments/{symbol}"
        if self._kline_source.exists(instrument_key):
            return self._kline_source.read_dataframe(instrument_key)
        if self._contract_source is not None:
            return self._contract_source.read_dataframe(symbol)
        raise FileNotFoundError(f"No instrument data found for symbol '{symbol}'.")

    def _read_calendar_dataframe(self, exchange: str) -> pd.DataFrame:
        """读取交易日历数据。

        优先使用显式配置的 calendar_source；否则回退到 kline_source 下约定路径
        calendars/{exchange}。
        """
        if self._calendar_source is not None:
            return self._calendar_source.read_dataframe(exchange)
        return self._kline_source.read_dataframe(f"calendars/{exchange}")

    def _build_schedule_from_contract_series(
        self,
        symbol: str,
        contract_series: pd.Series,
    ) -> ContractSchedule:
        """根据逐日合约序列恢复换仓时间表。"""
        if contract_series.empty:
            return ContractSchedule([], symbol)

        series = contract_series.dropna().astype(str)
        if series.empty:
            return ContractSchedule([], symbol)
        if not isinstance(series.index, pd.DatetimeIndex):
            series.index = pd.DatetimeIndex(pd.to_datetime(series.index))
        series = series.sort_index()
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

    def _load_prebuilt_schedule(
        self,
        symbol: str,
        adjust: str,
        price_df: pd.DataFrame,
    ) -> ContractSchedule:
        """从预构建 continuous 数据中恢复换仓时间表。

        支持两种来源：
            1. continuous 文件自带 contract / active_contract / contract_code 列
            2. companion 文件 continuous/{symbol}_{adjust}_schedule(.parquet)
        """
        for col in ("contract", "active_contract", "contract_code"):
            if col in price_df.columns:
                return self._build_schedule_from_contract_series(symbol, price_df[col])

        schedule_key = f"continuous/{symbol}_{adjust}_schedule"
        if self._kline_source.exists(schedule_key):
            df = self._kline_source.read_dataframe(schedule_key)
            if "to_contract" in df.columns:
                if isinstance(df.index, pd.DatetimeIndex):
                    dates = pd.DatetimeIndex(df.index)
                elif "date" in df.columns:
                    dates = pd.DatetimeIndex(pd.to_datetime(df["date"]))
                elif "trade_date" in df.columns:
                    dates = pd.DatetimeIndex(pd.to_datetime(df["trade_date"]))
                else:
                    dates = pd.DatetimeIndex(pd.to_datetime(df.index))

                events: list[RollEvent] = []
                for i, (_, row) in enumerate(df.reset_index(drop=True).iterrows()):
                    from_contract = str(row.get("from_contract", "")) if "from_contract" in df.columns else ""
                    if i > 0 and not from_contract:
                        from_contract = str(df.iloc[i - 1]["to_contract"])
                    events.append(
                        RollEvent(
                            date=dates[i],
                            from_contract=from_contract,
                            to_contract=str(row["to_contract"]),
                        )
                    )
                return ContractSchedule(events, symbol)

            for col in ("contract", "active_contract", "contract_code"):
                if col in df.columns:
                    return self._build_schedule_from_contract_series(symbol, df[col])

        return ContractSchedule([], symbol)

    def _normalize_kline(self, df: pd.DataFrame, contract_code: str) -> pd.DataFrame:
        """将原始 DataFrame 按 KlineSchema 规范化为 BarSeries 标准列名。

        标准列名：open / high / low / close / settle / volume / open_interest
        标准 index：DatetimeIndex
        """
        s = self._kline_schema

        # 1. 按合约代码过滤（多合约混合表）
        if s.contract_col is not None and s.contract_col in df.columns:
            # compound_code 格式 "{symbol}_{raw_id}"（如 "ES_195176"）：提取 raw_id 做过滤
            if s.compound_code and "_" in contract_code:
                raw_id_str = contract_code.split("_", 1)[1]
                if s.contract_col_dtype == "int":
                    try:
                        filter_val = int(raw_id_str)
                    except ValueError:
                        # 无法转换为 int，保守地使用原始字符串作为过滤值
                        filter_val = raw_id_str
                else:
                    filter_val = raw_id_str
            else:
                filter_val = contract_code
            df = df[df[s.contract_col] == filter_val].copy()
            if df.empty:
                raise KeyError(
                    f"Contract '{contract_code}' not found in column '{s.contract_col}'."
                )
        else:
            df = df.copy()

        # 2. 设置日期 index
        if not isinstance(df.index, pd.DatetimeIndex):
            if s.date_col in df.columns:
                df = df.set_index(pd.DatetimeIndex(pd.to_datetime(df[s.date_col])))
                df = df.drop(columns=[s.date_col], errors="ignore")
            else:
                df.index = pd.to_datetime(df.index)

        df.index.name = "date"

        # 3. 列名映射
        rename_map = {
            s.open_col:   "open",
            s.high_col:   "high",
            s.low_col:    "low",
            s.close_col:  "close",
            s.settle_col: "settle",
            s.volume_col: "volume",
            s.oi_col:     "open_interest",
        }
        # 只重命名存在的列，忽略缺失列（BarSeries 构造时会检查必要列）
        rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(columns=rename_map)

        # 对于某些数据源（如海外），close_col 与 settle_col 可能指向同一列（例如 SettlePrice）。
        # 在这种情况下，重命名后可能只有 'settle' 而没有 'close'，但 BarSeries 要求同时存在。
        # 如果出现这种情况，使用 settle 的值填充缺失的 close 列以兼容下游模型。
        if "settle" in df.columns and "close" not in df.columns:
            df["close"] = df["settle"]

        # 4. 丢弃非标准列（合约代码等)
        standard_cols = {"open", "high", "low", "close", "settle", "volume", "open_interest"}
        df = df[[c for c in df.columns if c in standard_cols]]

        return df.sort_index()

    def _load_bar_data_for_continuous(
        self,
        symbol: str,
        contracts: list[Contract],
    ) -> dict[str, BarSeries]:
        """为连续合约构建加载全量 BarSeries。

        对多合约混合表（如 tushare / overseas）走“单次读品种表 -> 拆分合约”的路径，
        避免按合约重复读取同一 parquet 文件。
        """
        cache_key = ("_load_bar_data_for_continuous", symbol, tuple(c.code for c in contracts))
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        s = self._kline_schema
        if s.contract_col is None:
            bar_data: dict[str, BarSeries] = {}
            for contract in contracts:
                try:
                    bar_data[contract.code] = self.load_bar_series(contract.code)
                except (FileNotFoundError, KeyError):
                    continue
            self._set_cached(cache_key, bar_data)
            return bar_data

        file_key = s.key_prefix + symbol
        df_raw = self._kline_source.read_dataframe(file_key).copy()
        if df_raw.empty:
            self._set_cached(cache_key, {})
            return {}

        if not isinstance(df_raw.index, pd.DatetimeIndex):
            if s.date_col in df_raw.columns:
                df_raw.index = pd.DatetimeIndex(pd.to_datetime(df_raw[s.date_col]))
                df_raw = df_raw.drop(columns=[s.date_col], errors="ignore")
            else:
                df_raw.index = pd.DatetimeIndex(pd.to_datetime(df_raw.index))
        df_raw.index.name = "date"

        raw_contract = df_raw[s.contract_col]
        if s.compound_code:
            contract_labels = symbol + "_" + raw_contract.astype(str)
        else:
            contract_labels = raw_contract.astype(str)

        needed_codes = {c.code for c in contracts}
        df_raw["__contract_code__"] = contract_labels
        df_raw = df_raw[df_raw["__contract_code__"].isin(needed_codes)].copy()
        if df_raw.empty:
            self._set_cached(cache_key, {})
            return {}

        rename_map = {
            s.open_col: "open",
            s.high_col: "high",
            s.low_col: "low",
            s.close_col: "close",
            s.settle_col: "settle",
            s.volume_col: "volume",
            s.oi_col: "open_interest",
        }
        rename_map = {k: v for k, v in rename_map.items() if k in df_raw.columns}
        df_raw = df_raw.rename(columns=rename_map)
        if "settle" in df_raw.columns and "close" not in df_raw.columns:
            df_raw["close"] = df_raw["settle"]

        standard_cols = {"open", "high", "low", "close", "settle", "volume", "open_interest"}
        keep_cols = ["__contract_code__"] + [c for c in df_raw.columns if c in standard_cols]
        df_raw = df_raw[keep_cols].sort_index()

        bar_data: dict[str, BarSeries] = {}
        for code, sub in df_raw.groupby("__contract_code__", sort=False):
            sub = sub.drop(columns=["__contract_code__"]).sort_index()
            try:
                bar_data[str(code)] = BarSeries(str(code), sub)
            except (KeyError, ValueError):
                continue

        self._set_cached(cache_key, bar_data)
        return bar_data

    def _normalize_contracts(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """将原始合约信息 DataFrame 按 ContractSchema 规范化。

        标准列名：code / exchange / list_date / expire_date / last_trade_date
        """
        s = self._contract_schema
        df = df.copy()

        # 按品种过滤：symbol_col 指定列做精确匹配（海外），否则 code 列做前缀匹配（国内）
        if s.symbol_col is not None and s.symbol_col in df.columns:
            df = df[df[s.symbol_col] == symbol].copy()
        elif s.code_col in df.columns:
            df = df[df[s.code_col].astype(str).str.startswith(symbol)].copy()

        rename_map = {
            s.list_date_col:       "list_date",
            s.expire_date_col:     "expire_date",
            s.last_trade_date_col: "last_trade_date",
        }
        if s.exchange_col is not None:
            rename_map[s.exchange_col] = "exchange"
        rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(columns=rename_map)

        # compound_code：将 code 格式化为 "{symbol}_{raw_id}"（如 "ES_195176"）
        if s.compound_code and s.code_col in df.columns:
            df["code"] = symbol + "_" + df[s.code_col].astype(str)
        elif s.code_col in df.columns:
            df = df.rename(columns={s.code_col: "code"})

        # 将日期列从 YYYYMMDD 整数/浮点格式解析为 datetime
        # 兼容：整数 20260116、浮点 20270119.0、字符串 "20260116"、已是 datetime
        for col in ("list_date", "expire_date", "last_trade_date"):
            if col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    pass  # 已是 datetime，无需转换
                else:
                    cleaned = df[col].astype(str).str.split(".").str[0]
                    df[col] = pd.to_datetime(cleaned, errors="coerce")

        return df

    def _normalize_instrument(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """将原始 instrument DataFrame 按 InstrumentSchema 规范化。"""
        s = self._instrument_schema
        df = df.copy()

        if s.symbol_col in df.columns:
            df = df[df[s.symbol_col].astype(str) == symbol].copy()

        rename_map = {
            s.symbol_col: "symbol",
            s.name_col: "name",
            s.exchange_col: "exchange",
            s.currency_col: "currency",
            s.lot_size_col: "lot_size",
            s.tick_size_col: "tick_size",
            s.margin_rate_col: "margin_rate",
        }
        rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(columns=rename_map)

        if "symbol" not in df.columns:
            df["symbol"] = symbol
        else:
            df["symbol"] = df["symbol"].fillna(symbol).astype(str)

        if "currency" not in df.columns:
            df["currency"] = s.default_currency
        else:
            df["currency"] = df["currency"].fillna(s.default_currency).astype(str)

        for col, default in (
            ("lot_size", 1.0),
            ("tick_size", s.default_tick_size),
            ("margin_rate", s.default_margin_rate),
        ):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)
            else:
                df[col] = default

        for col in ("name", "exchange"):
            if col not in df.columns:
                df[col] = ""
            else:
                df[col] = df[col].fillna("").astype(str)

        standard_cols = ["symbol", "name", "exchange", "currency", "lot_size", "tick_size", "margin_rate"]
        return df[standard_cols]

    # ------------------------------------------------------------------
    # 行情数据
    # ------------------------------------------------------------------

    def load_bar_series(
        self,
        contract_code: str,
        start: str | None = None,
        end: str | None = None,
    ) -> BarSeries:
        """加载单个合约的 K 线序列。

        key 规则：f"klines/{contract_code}"（单合约文件）
        或从品种文件中按 contract_col 过滤（多合约混合表，此时 key 为 f"klines/{symbol}"）。
        """
        cache_key = ("load_bar_series", contract_code, start, end)
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        s = self._kline_schema
        # 判断是单合约文件还是品种混合文件
        # 若 schema 声明了 contract_col，说明是多合约混合大表：
        #   · 日期存于普通列（非 index），source 层无法按 DatetimeIndex 过滤
        #   · 先整表读入，normalize 后建好 DatetimeIndex，再做日期截取
        if s.contract_col is not None:
            if s.compound_code and "_" in contract_code:
                # compound_code 格式 "{symbol}_{raw_id}"（如 "ES_195176"）
                symbol = contract_code.split("_", 1)[0]
            else:
                bare = contract_code.split(".")[0]       # 去交易所后缀：RB2410.SHF → RB2410
                symbol = "".join(c for c in bare if c.isalpha())  # 取字母部分：RB
            file_key = s.key_prefix + symbol            # 海外："daily_ES"，国内："RB"
            df_raw = self._kline_source.read_dataframe(file_key)
            df = self._normalize_kline(df_raw, contract_code)
            if start is not None:
                df = df[df.index >= pd.Timestamp(start)]
            if end is not None:
                df = df[df.index <= pd.Timestamp(end)]
        else:
            # 单合约文件：index 已是 DatetimeIndex，source 层可直接过滤
            df_raw = self._kline_source.read_dataframe(
                contract_code, start=start, end=end
            )
            df = self._normalize_kline(df_raw, contract_code)

        result = BarSeries(contract_code, df)
        self._set_cached(cache_key, result)
        return result

    def load_bar_matrix(
        self,
        symbols: list[str],
        start: str | None = None,
        end: str | None = None,
        price_col: str = "settle",
    ) -> pd.DataFrame:
        """加载多品种价格矩阵，返回 DataFrame(dates × symbols)。

        key 规则：f"continuous/{symbol}"，读取各品种的连续合约数据。
        """
        cache_key = ("load_bar_matrix", tuple(symbols), start, end, price_col)
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        series_dict: dict[str, pd.Series] = {}
        for symbol in symbols:
            try:
                df = self._kline_source.read_dataframe(
                    f"continuous/{symbol}", start=start, end=end
                )
                if price_col not in df.columns:
                    raise KeyError(
                        f"Column '{price_col}' not found in continuous/{symbol}."
                    )
                series_dict[symbol] = df[price_col]
            except FileNotFoundError:
                continue

        result = pd.DataFrame(series_dict)
        self._set_cached(cache_key, result)
        return result

    def load_continuous_field_series(
        self,
        symbol: str,
        field_name: str,
        start: str | None = None,
        end: str | None = None,
        stability_days: int = 1,
    ) -> pd.Series:
        """加载连续主力路径下某个标准字段的逐日序列。

        当前用于研究层补充 close / settle / open_interest 等字段矩阵。
        字段值沿 ``load_continuous()`` 使用的主力 schedule 回溯到对应合约日线。
        """
        cache_key = (
            "load_continuous_field_series",
            symbol,
            field_name,
            start,
            end,
            stability_days,
        )
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        standard_cols = {"open", "high", "low", "close", "settle", "volume", "open_interest"}
        if field_name not in standard_cols:
            raise ValueError(f"Unsupported field_name: {field_name}")

        cs = self.load_continuous(
            symbol,
            start=start,
            end=end,
            adjust="nav",
            nav_output="price",
            stability_days=stability_days,
        )
        dates = pd.DatetimeIndex(cs.prices.index)
        if dates.empty:
            result = pd.Series(dtype=float, name=symbol)
            self._set_cached(cache_key, result)
            return result

        if not cs.schedule.events:
            result = pd.Series(index=dates, dtype=float, name=symbol)
            self._set_cached(cache_key, result)
            return result

        active_contract = pd.Series(
            [cs.schedule.get_active_contract(ts) for ts in dates],
            index=dates,
            name="contract_code",
        )

        pieces: list[pd.Series] = []
        for contract_code, contract_dates in active_contract.groupby(active_contract):
            segment_index = pd.DatetimeIndex(contract_dates.index)
            bs = self.load_bar_series(
                str(contract_code),
                start=str(segment_index[0].date()),
                end=str(segment_index[-1].date()),
            )
            if field_name not in bs.data.columns:
                raise KeyError(f"Column '{field_name}' not found in contract '{contract_code}'.")
            pieces.append(bs.data[field_name].reindex(segment_index))

        result = pd.concat(pieces).sort_index().reindex(dates)
        result.name = symbol
        self._set_cached(cache_key, result)
        return result

    def load_continuous_contract_series(
        self,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
        stability_days: int = 1,
    ) -> pd.Series:
        """加载连续主力路径下的逐日主力合约代码序列。"""
        cache_key = (
            "load_continuous_contract_series",
            symbol,
            start,
            end,
            stability_days,
        )
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        cs = self.load_continuous(
            symbol,
            start=start,
            end=end,
            adjust="nav",
            nav_output="price",
            stability_days=stability_days,
        )
        dates = pd.DatetimeIndex(cs.prices.index)
        if dates.empty:
            result = pd.Series(dtype=object, name=symbol)
            self._set_cached(cache_key, result)
            return result

        if not cs.schedule.events:
            result = pd.Series(index=dates, dtype=object, name=symbol)
            self._set_cached(cache_key, result)
            return result

        result = pd.Series(
            [cs.schedule.get_active_contract(ts) for ts in dates],
            index=dates,
            name=symbol,
            dtype=object,
        )
        self._set_cached(cache_key, result)
        return result

    def load_continuous_field_returns_series(
        self,
        symbol: str,
        field_name: str,
        start: str | None = None,
        end: str | None = None,
        stability_days: int = 1,
        zero_on_roll: bool = True,
        clip_abs_return: float | None = 0.5,
    ) -> pd.Series:
        """加载连续主力字段的日收益序列，并可在换月日清零收益。"""
        cache_key = (
            "load_continuous_field_returns_series",
            symbol,
            field_name,
            start,
            end,
            stability_days,
            zero_on_roll,
            clip_abs_return,
        )
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        field_series = self.load_continuous_field_series(
            symbol,
            field_name,
            start=start,
            end=end,
            stability_days=stability_days,
        )
        contract_series = self.load_continuous_contract_series(
            symbol,
            start=start,
            end=end,
            stability_days=stability_days,
        ).reindex(field_series.index)

        returns = field_series.pct_change()
        if zero_on_roll and not contract_series.empty:
            same_contract = contract_series.eq(contract_series.shift(1))
            if not same_contract.empty:
                same_contract.iloc[0] = False
            returns = returns.where(same_contract, 0.0)
        if clip_abs_return is not None:
            returns = returns.where(returns.abs() <= clip_abs_return, 0.0)
        if not returns.empty:
            returns.iloc[0] = 0.0
        returns = returns.fillna(0.0)
        returns.name = symbol
        self._set_cached(cache_key, returns)
        return returns

    def load_continuous_field_matrix(
        self,
        symbols: list[str],
        field_name: str,
        start: str | None = None,
        end: str | None = None,
        stability_days: int = 1,
    ) -> pd.DataFrame:
        """批量加载连续主力路径下的标准字段矩阵。"""
        cache_key = (
            "load_continuous_field_matrix",
            tuple(symbols),
            field_name,
            start,
            end,
            stability_days,
        )
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        series_dict: dict[str, pd.Series] = {}
        for symbol in symbols:
            try:
                series = self.load_continuous_field_series(
                    symbol,
                    field_name,
                    start=start,
                    end=end,
                    stability_days=stability_days,
                )
            except (FileNotFoundError, ValueError, KeyError):
                continue
            if not series.empty:
                series_dict[symbol] = series

        result = pd.DataFrame(series_dict).sort_index()
        self._set_cached(cache_key, result)
        return result

    def load_continuous_field_returns_matrix(
        self,
        symbols: list[str],
        field_name: str,
        start: str | None = None,
        end: str | None = None,
        stability_days: int = 1,
        zero_on_roll: bool = True,
        clip_abs_return: float | None = 0.5,
        min_obs: int = 0,
    ) -> pd.DataFrame:
        """批量加载连续主力字段的收益率矩阵。"""
        cache_key = (
            "load_continuous_field_returns_matrix",
            tuple(symbols),
            field_name,
            start,
            end,
            stability_days,
            zero_on_roll,
            clip_abs_return,
            min_obs,
        )
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        series_dict: dict[str, pd.Series] = {}
        for symbol in symbols:
            try:
                series = self.load_continuous_field_returns_series(
                    symbol,
                    field_name,
                    start=start,
                    end=end,
                    stability_days=stability_days,
                    zero_on_roll=zero_on_roll,
                    clip_abs_return=clip_abs_return,
                )
            except (FileNotFoundError, ValueError, KeyError):
                continue
            if not series.empty:
                series_dict[symbol] = series

        result = pd.DataFrame(series_dict).sort_index()
        if min_obs > 0 and not result.empty:
            valid_counts = result.notna().sum()
            keep = valid_counts[valid_counts >= min_obs].index.tolist()
            result = result[keep]
        self._set_cached(cache_key, result)
        return result

    def load_continuous_matrix(
        self,
        symbols: list[str],
        start: str | None = None,
        end: str | None = None,
        adjust: str = "nav",
        nav_output: str = "price",
        stability_days: int = 1,
        transition_days: int = 1,
    ) -> pd.DataFrame:
        """批量加载多个品种的连续价格矩阵，返回 DataFrame(dates × symbols)。"""
        cache_key = (
            "load_continuous_matrix",
            tuple(symbols),
            start,
            end,
            adjust,
            nav_output,
            stability_days,
            transition_days,
        )
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        series_dict: dict[str, pd.Series] = {}
        for symbol in symbols:
            try:
                cs = self.load_continuous(
                    symbol,
                    start=start,
                    end=end,
                    adjust=adjust,
                    nav_output=nav_output,
                    stability_days=stability_days,
                    transition_days=transition_days,
                )
                series_dict[symbol] = cs.prices
            except (FileNotFoundError, ValueError, KeyError):
                continue

        result = pd.DataFrame(series_dict).sort_index()
        self._set_cached(cache_key, result)
        return result

    def load_returns_matrix(
        self,
        symbols: list[str],
        start: str | None = None,
        end: str | None = None,
        adjust: str = "nav",
        min_obs: int = 0,
    ) -> pd.DataFrame:
        """加载多品种日收益率宽表，返回 DataFrame(dates × symbols)。

        对 load_continuous_matrix() 的薄包装：加载连续价格矩阵后取一阶百分比收益率。
        支持路径③（无合约元数据、内联 OI-max），与 load_continuous_matrix() 共享底层缓存。

        Parameters
        ----------
        symbols:
            品种代码列表。
        start / end:
            日期范围过滤（传递给 load_continuous_matrix()）。
        adjust:
            连续价格拼接方式，默认 'nav'（Buy-and-Roll NAV）。
        min_obs:
            最少有效观测天数；不足此值的品种从结果中剔除，默认 0（不过滤）。

        Returns
        -------
        DataFrame，index=DatetimeIndex，columns=品种代码，值=日百分比收益率。
        首行因无前值为 NaN，调用方按需 dropna。
        """
        cache_key = ("load_returns_matrix", tuple(symbols), start, end, adjust, min_obs)
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        price_matrix = self.load_continuous_matrix(
            symbols=symbols,
            start=start,
            end=end,
            adjust=adjust,
        )

        if price_matrix.empty:
            return pd.DataFrame()

        returns = price_matrix.pct_change()

        if min_obs > 0:
            valid_counts = returns.notna().sum()
            keep = valid_counts[valid_counts >= min_obs].index.tolist()
            returns = returns[keep]

        self._set_cached(cache_key, returns)
        return returns

    def available_symbols(self, exclude: set[str] | None = None) -> list[str]:
        """列举 kline_source 中所有可用品种代码。

        对 kline_source.list_keys() 的结果去除 KlineSchema.key_prefix，
        返回排序后的品种代码列表。仅处理顶层文件（不含子目录路径）。

        Parameters
        ----------
        exclude:
            需排除的品种代码集合，默认不排除。

        Examples
        --------
        >>> loader = DataLoader(ParquetSource("overseas_daily_full/"), KlineSchema.overseas())
        >>> loader.available_symbols(exclude={"VX", "BTC"})
        ['6A', '6B', '6C', ...]
        """
        prefix = self._kline_schema.key_prefix
        symbols: list[str] = []
        for key in self._kline_source.list_keys():
            if "/" in key:           # 跳过子目录路径
                continue
            if prefix and not key.startswith(prefix):
                continue
            sym = key[len(prefix):]  # 去除 key_prefix（如 "daily_ES" → "ES"）
            symbols.append(sym)

        if exclude:
            symbols = [sym for sym in symbols if sym not in exclude]

        return sorted(set(symbols))

    # ------------------------------------------------------------------
    # 合约信息
    # ------------------------------------------------------------------

    def load_contracts(
        self,
        symbol: str,
        active_only: bool = False,
        ref_date: str | None = None,
    ) -> list[Contract]:
        """加载品种的所有合约信息，可按 active_only 和 ref_date 过滤。

        key 规则：symbol（contract_source 自身负责定位文件并过滤）
        期望列：code/exchange/list_date/expire_date/last_trade_date
        """
        cache_key = ("load_contracts", symbol, active_only, ref_date)
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        df_raw = self._read_contract_dataframe(symbol)
        df = self._normalize_contracts(df_raw, symbol)
        contracts: list[Contract] = []

        for _, row in df.iterrows():
            try:
                list_ts = pd.Timestamp(row["list_date"])
                expire_ts = pd.Timestamp(row["expire_date"])
                last_ts = pd.Timestamp(row["last_trade_date"])
                # 跳过任一关键日期解析失败（NaT）的行
                if pd.isna(list_ts) or pd.isna(expire_ts) or pd.isna(last_ts):
                    continue
                contract = Contract(
                    symbol=symbol,
                    code=str(row["code"]),
                    exchange=str(row.get("exchange", "")),
                    list_date=list_ts.date(),
                    expire_date=expire_ts.date(),
                    last_trade_date=last_ts.date(),
                )
                contracts.append(contract)
            except (KeyError, ValueError, TypeError):
                continue  # 跳过缺失关键字段的行

        if active_only and ref_date is not None:
            ref = pd.Timestamp(ref_date).date()
            contracts = [c for c in contracts if c.is_active(ref)]

        contracts.sort(key=lambda c: (c.last_trade_date, c.code))

        self._set_cached(cache_key, contracts)
        return contracts

    def load_instrument(self, symbol: str) -> Instrument:
        """加载品种静态信息并自动注册到 InstrumentRegistry。

        key 规则：symbol。默认回退到 instruments/{symbol}。
        """
        cache_key = ("load_instrument", symbol)
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        df_raw = self._read_instrument_dataframe(symbol)
        df = self._normalize_instrument(df_raw, symbol)
        if df.empty:
            raise FileNotFoundError(f"No instrument data found for symbol '{symbol}'.")
        row = df.iloc[0]

        instrument = Instrument(
            symbol=str(row["symbol"]),
            name=str(row["name"]),
            exchange=str(row["exchange"]),
            currency=str(row["currency"]),
            lot_size=float(row["lot_size"]),
            tick_size=float(row["tick_size"]),
            margin_rate=float(row["margin_rate"]),
        )

        InstrumentRegistry().register(instrument)
        self._set_cached(cache_key, instrument)
        return instrument

    # ------------------------------------------------------------------
    # 日历
    # ------------------------------------------------------------------

    def load_calendar(self, exchange: str) -> TradingCalendar:
        """加载交易所日历。

        key 规则：exchange。默认回退到 calendars/{exchange}。
        期望列：date 或 trade_date（表示交易日），或 DatetimeIndex。
        """
        cache_key = ("load_calendar", exchange)
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        df = self._read_calendar_dataframe(exchange)

        # 支持三种格式：DatetimeIndex、date 列、trade_date 列
        if isinstance(df.index, pd.DatetimeIndex):
            trading_dates = df.index
        elif "trade_date" in df.columns:
            trading_dates = pd.DatetimeIndex(pd.to_datetime(df["trade_date"]))
        elif "date" in df.columns:
            trading_dates = pd.DatetimeIndex(pd.to_datetime(df["date"]))
        else:
            trading_dates = pd.DatetimeIndex(pd.to_datetime(df.index))

        result = TradingCalendar(exchange, trading_dates)
        self._set_cached(cache_key, result)
        return result

    def load_multi_calendar(self, exchanges: list[str]) -> MultiExchangeCalendar:
        """加载多交易所合并日历。"""
        cache_key = ("load_multi_calendar", tuple(exchanges))
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        calendars = [self.load_calendar(ex) for ex in exchanges]
        result = MultiExchangeCalendar(calendars)
        self._set_cached(cache_key, result)
        return result

    # ------------------------------------------------------------------
    # 连续合约
    # ------------------------------------------------------------------

    def _build_continuous_from_raw_kline(
        self,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
        nav_output: str = "price",
    ) -> "ContinuousSeries":
        """路径③：从多合约 kline 宽表内联 OI-max 构建 ContinuousSeries（NAV 模式）。

        适用于只有逐品种 kline 文件、无独立合约元数据表的数据源
        （如 china_daily_full/、overseas_daily_full/）。
        通过 KlineSchema 的列名映射同时支持国内和境外数据格式。

        Roll 处理：换仓日收益计为 0（不引入跨合约价差），NAV 价格在换仓日保持平坦。
        换仓点记录在 ContractSchedule 中，供诊断使用。
        """
        s = self._kline_schema
        file_key = s.key_prefix + symbol
        df = self._kline_source.read_dataframe(file_key).copy()

        # 1. 规范化日期 index
        if not isinstance(df.index, pd.DatetimeIndex):
            if s.date_col in df.columns:
                df.index = pd.DatetimeIndex(pd.to_datetime(df[s.date_col]))
                df = df.drop(columns=[s.date_col], errors="ignore")
            else:
                df.index = pd.DatetimeIndex(pd.to_datetime(df.index))
        df.index.name = "date"

        # 2. 规范化关键列（OI、结算价）
        df[s.oi_col] = pd.to_numeric(df[s.oi_col], errors="coerce").fillna(0.0)
        df[s.settle_col] = pd.to_numeric(df[s.settle_col], errors="coerce")

        # 3. OI-max：每日保留持仓量最大的合约（与 MaxInterestSelector 逻辑一致）
        df_reset = df.reset_index()
        df_reset = df_reset.sort_values(["date", s.oi_col], ascending=[True, False])
        df_reset = df_reset.drop_duplicates(subset=["date"], keep="first")
        df_main = df_reset.set_index("date").sort_index()

        if df_main.empty or df_main[s.settle_col].isna().all():
            raise ValueError(f"No valid data for '{symbol}' after OI-max selection.")

        # 4. 换仓检测与 ContractSchedule 构建
        if s.contract_col is not None and s.contract_col in df_main.columns:
            contract_raw = df_main[s.contract_col]
            same_contract = contract_raw == contract_raw.shift(1)
            contract_labels = (
                symbol + "_" + contract_raw.astype(str)
                if s.compound_code
                else contract_raw.astype(str)
            )
            schedule = self._build_schedule_from_contract_series(symbol, contract_labels)
        else:
            same_contract = pd.Series(True, index=df_main.index)
            schedule = ContractSchedule([], symbol)

        # 首日无前一日对比，视作换仓（收益 = 0）
        if not same_contract.empty:
            same_contract.iloc[0] = False

        # 5. 计算日收益率：换仓日 = 0，超过 ±50% 视为数据异常也归 0
        raw_ret = df_main[s.settle_col].pct_change()
        raw_ret[~same_contract] = 0.0
        raw_ret[raw_ret.abs() > 0.5] = 0.0
        raw_ret.iloc[0] = 0.0
        raw_ret = raw_ret.fillna(0.0)

        # 6. 构建 NAV 价格序列（锚定到首日结算价，或归一化到 1.0）
        if nav_output == "normalized":
            initial_price = 1.0
        else:
            first_valid = df_main[s.settle_col].first_valid_index()
            initial_price = (
                float(df_main.loc[first_valid, s.settle_col])
                if first_valid is not None
                else 1.0
            )
        nav = initial_price * (1.0 + raw_ret).cumprod()

        # 7. 按 start/end 截取
        if start is not None:
            nav = nav[nav.index >= pd.Timestamp(start)]
        if end is not None:
            nav = nav[nav.index <= pd.Timestamp(end)]

        return ContinuousSeries(symbol, nav, schedule)

    def load_continuous(
        self,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
        adjust: str = "nav",
        nav_output: str = "price",
        calendar: "TradingCalendar | None" = None,
        stability_days: int = 1,
        transition_days: int = 1,
    ) -> ContinuousSeries:
        """加载或动态构建连续合约序列。

        按优先级依次尝试三条路径：
          路径①  从存储加载预构建 continuous 文件（key: f"continuous/{symbol}_{adjust}"）。
          路径③  KlineSchema 声明了 contract_col（多合约混合表）时，从原始 kline 宽表
                 内联 OI-max 主力选择，无需独立合约元数据。
          路径②  动态构建：加载合约列表 + 各合约 K 线 + OIMaxRoll 滚动（需 contract_source）。

        calendar：用于 ContinuousSeries.build 的交易日序列。
            - 显式传入：直接使用。
            - 不传且 calendar_source 已配置：从合约列表推断交易所后自动加载。
            - 不传且无 calendar_source：由 build 从 bar_data 合并交易日（fallback）。

        stability_days：换仓稳定性过滤天数（默认 1 = 不过滤）。
            新合约需连续 stability_days 天保持最高持仓量才确认切换。
        transition_days：换仓过渡天数（默认 1 = 立即切换，仅 NAV 模式有效）。
            N > 1 时，换仓确认次日起线性分 N-1 天逐步移仓。
        nav_output：仅在 adjust="nav" 时生效。
            - "price"：输出锚定到首日原始价格的连续价格链
            - "normalized"：输出从 1.0 起始的标准化净值链
        """
        cache_key = (
            "load_continuous",
            symbol,
            start,
            end,
            adjust,
            nav_output,
            stability_days,
            transition_days,
        )
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        adjust_method = AdjustMethod(adjust)
        if nav_output not in {"price", "normalized"}:
            raise ValueError("nav_output must be 'price' or 'normalized'.")
        pre_built_key = f"continuous/{symbol}_{adjust}"

        # ── 尝试加载预构建的连续合约 ────────────────────────────────────
        if self._kline_source.exists(pre_built_key):
            df = self._kline_source.read_dataframe(pre_built_key, start=start, end=end)

            # 预构建数据期望有 settle 列，index 为 DatetimeIndex
            if "settle" in df.columns:
                price_series = df["settle"]
            else:
                price_series = df.iloc[:, 0]
            if adjust_method == AdjustMethod.NAV and nav_output == "normalized" and not price_series.empty:
                price_series = price_series / float(price_series.iloc[0])

            schedule = self._load_prebuilt_schedule(symbol, adjust, df)
            result = ContinuousSeries(symbol, price_series, schedule)
            self._set_cached(cache_key, result)
            return result

        # ── 路径③：多合约 kline 宽表内联 OI-max（无独立合约元数据时的 fallback）───
        # 当 KlineSchema 声明了 contract_col（多合约混合表格式）时，直接从原始宽表做
        # OI-max 主力选择，跳过 load_contracts() 的合约元数据依赖。
        # 适用于 china_daily_full/、overseas_daily_full/ 等只有 kline 文件的数据源。
        if self._kline_schema.contract_col is not None:
            result = self._build_continuous_from_raw_kline(
                symbol, start=start, end=end, nav_output=nav_output
            )
            self._set_cached(cache_key, result)
            return result

        # ── 路径②：动态构建（需合约元数据）────────────────────────────────────
        # 1. 加载合约列表（全量，不受 start/end 约束）
        contracts = self.load_contracts(symbol)
        if not contracts:
            raise ValueError(
                f"No contracts found for symbol '{symbol}'. Cannot build ContinuousSeries."
            )

        # 2. 逐一加载各合约的全量 BarSeries（不传 start/end）
        #    必须使用全量数据：OIMaxRoll 换仓选择依赖持仓量的跨合约比较，
        #    裁剪数据会导致 start 附近的换仓选错合约。
        #    start/end 裁剪在 build 完成后统一执行。
        bar_data = self._load_bar_data_for_continuous(symbol, contracts)

        if not bar_data:
            raise ValueError(
                f"No kline data found for any contract of symbol '{symbol}'."
            )

        # 3. 解析日历：显式传入 > calendar_source 自动加载 > 从 bar_data 推断
        #    优先级依次降低；有 start/end 时裁剪日历范围，避免 build 内逐日遍历全量交易日。
        cal = calendar
        if cal is None and self._calendar_source is not None and contracts:
            exchange = getattr(contracts[0], "exchange", "") or ""
            if exchange:
                try:
                    cal = self.load_calendar(exchange)
                except Exception:
                    cal = None

        if cal is None:
            # 无日历文件（如海外）：从 bar_data 合并交易日，构造合成日历
            # 同时规避 build() 内重复做集合合并的开销
            all_ts: set[pd.Timestamp] = set()
            for bs in bar_data.values():
                all_ts.update(bs.data.index)
            sorted_dates = pd.DatetimeIndex(sorted(all_ts))
            if start is not None:
                sorted_dates = sorted_dates[sorted_dates >= pd.Timestamp(start)]
            if end is not None:
                sorted_dates = sorted_dates[sorted_dates <= pd.Timestamp(end)]
            cal = TradingCalendar("inferred", sorted_dates)
        elif start is not None or end is not None:
            clipped_dates = cal._dates
            if start is not None:
                clipped_dates = clipped_dates[clipped_dates >= pd.Timestamp(start)]
            if end is not None:
                clipped_dates = clipped_dates[clipped_dates <= pd.Timestamp(end)]
            cal = TradingCalendar(cal.exchange, clipped_dates)

        # 4. 动态构建：按需组装换仓规则，传入裁剪后的日历
        base_rule: RollRule = OIMaxRoll()
        roll_rule: RollRule = (
            StabilizedRule(base_rule, stability_days=stability_days)
            if stability_days > 1
            else base_rule
        )
        result = ContinuousSeries.build(
            symbol=symbol,
            bar_data=bar_data,
            contracts=contracts,
            roll_rule=roll_rule,
            adjust=adjust_method,
            calendar=cal,
            transition_days=transition_days,
            nav_output=nav_output,
        )

        # 5. 最终按 start/end 切片（处理 calendar=None 时 build 从 bar_data 合并日期的情况）
        if start is not None or end is not None:
            prices = result.prices
            if start is not None:
                prices = prices[prices.index >= pd.Timestamp(start)]
            if end is not None:
                prices = prices[prices.index <= pd.Timestamp(end)]
            result = ContinuousSeries(symbol, prices, result.schedule)

        self._set_cached(cache_key, result)
        return result
