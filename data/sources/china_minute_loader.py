"""ChinaMinuteLoader：读取中国期货分钟级 Kline 数据，
生成日内动量策略所需的首/尾时段日度收益矩阵。

数据目录结构
-----------
    <data_dir>/{SYMBOL}/year_{YEAR}.parquet

每个 parquet 文件列
-------------------
    trade_date   datetime64[ns]  交易日期（夜盘 bar 归属下一交易日）
    trade_time   datetime64[ns]  时间，日期部分固定为 1970-01-01
    contract_code str
    open_price, high_price, low_price, close_price  int32
    volume       int64
    amount       int64
    interest     int32

设计约定
--------
1. **日盘时段**：策略仅使用日盘，排除夜盘 bar（trade_time.hour >= 20 或 <= 3）。
   - 股指期货 (CFE exchange)：09:30–15:00（午休 11:30–13:00）
   - 商品期货（其余交易所）：09:00–15:00（午休 11:30–13:30）

2. **夜盘处理**：本策略不使用夜盘信息。夜盘 bar 已按数据约定归属下一交易日，
   但日盘滤波器（_DAY_SESSION_MIN / _DAY_SESSION_MAX）会将其完全排除。

3. **主力合约选择**：每日按成交量最大的具体合约（非 XX00/XX01 连续合约）。
   具体合约判断：contract_code 去掉交易所后缀后末4位全为数字（如 IF2403）。

4. **时段收益计算**：
   - period_return = last_close / first_open - 1
   - 过滤零成交量 bar；有效 bar 数 < 2 时标记为 NaN。
   - 日总成交量低于 min_daily_volume 的交易日整体标记为 NaN。

5. **首时段**：从日盘开盘时刻起的 first_period_minutes 分钟。
   **尾时段**：日盘收盘前 last_period_minutes 分钟（包含 15:00 集合竞价 bar，
   但因零量过滤实际不影响收益）。
"""

from __future__ import annotations

import datetime
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# ── 时段常量 ──────────────────────────────────────────────────────────────────

# 日盘过滤窗口（排除夜盘 21:00–次日 03:00）
_DAY_SESSION_MIN = datetime.time(8, 30)
_DAY_SESSION_MAX = datetime.time(15, 30)

# 股指期货（CFFEX）品种集合
_CFE_SYMBOLS: frozenset[str] = frozenset({"IF", "IC", "IH", "IM"})

_SESSION_CFE = {
    "day_open": datetime.time(9, 30),   # 开盘
    "day_close": datetime.time(15, 0),  # 收盘
}
_SESSION_DEFAULT = {
    "day_open": datetime.time(9, 0),
    "day_close": datetime.time(15, 0),
}


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def _get_session(symbol: str) -> dict:
    """根据品种代码返回对应交易时段配置。"""
    return _SESSION_CFE if symbol.upper() in _CFE_SYMBOLS else _SESSION_DEFAULT


def _is_specific_contract(code: str) -> bool:
    """判断是否为具体到期合约，而非连续合约（XX00–XX11）。

    各交易所具体合约格式：
    - SHFE/DCE/INE/GFEX/CFE：末4位全数字，如 CU2401、IF2403
    - CZCE：末3位全数字（年末1位+月2位），如 MA401（2024年1月）、CF503（2025年3月）

    连续合约均为末2位（00-11），可被此规则正确排除。
    """
    prefix = code.split(".")[0]
    return bool(re.search(r"\d{3,4}$", prefix))


def _add_minutes(t: datetime.time, minutes: int) -> datetime.time:
    """时间加减分钟（结果钳制在 00:00–23:59）。"""
    total = t.hour * 60 + t.minute + minutes
    total = max(0, min(23 * 60 + 59, total))
    return datetime.time(total // 60, total % 60)


# ── 核心加载器 ────────────────────────────────────────────────────────────────

class ChinaMinuteLoader:
    """中国期货分钟数据加载器：计算日度首/尾时段收益矩阵。

    Parameters
    ----------
    data_dir : str | Path
        china_minute/ 目录根路径（包含 {SYMBOL}/year_{YEAR}.parquet 子目录）。
    first_period_minutes : int
        首时段分钟数（默认 30，对应论文"首30分钟"）。
    last_period_minutes : int
        尾时段分钟数（默认 30，对应论文"尾30分钟"）。
    min_daily_volume : int
        每日主力合约最小总成交量；低于此阈值的交易日标记为 NaN，默认 500。
    """

    def __init__(
        self,
        data_dir: str | Path,
        first_period_minutes: int = 30,
        last_period_minutes: int = 30,
        min_daily_volume: int = 500,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.first_period_minutes = first_period_minutes
        self.last_period_minutes = last_period_minutes
        self.min_daily_volume = min_daily_volume

    # ── 公开接口 ──────────────────────────────────────────────────────────────

    def available_symbols(self) -> list[str]:
        """返回 data_dir 下所有可用品种代码（大写，字母序）。"""
        if not self.data_dir.exists():
            return []
        return sorted(p.name.upper() for p in self.data_dir.iterdir() if p.is_dir())

    def load_symbol(
        self,
        symbol: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        """加载单品种日度首/尾时段收益率。

        Parameters
        ----------
        symbol : str
            品种代码（大写），如 'IF', 'CU', 'RB'。
        start, end : str | None
            日期过滤，格式 'YYYY-MM-DD'（含两端）。

        Returns
        -------
        first_ret : pd.Series
            首时段日度收益率，index=DatetimeIndex(trade_date)，name=symbol。
        last_ret : pd.Series
            尾时段日度收益率，同上。
        daily_volume : pd.Series
            每日主力合约总成交量，同上。
        """
        sym = symbol.upper()
        sym_dir = self.data_dir / sym
        if not sym_dir.exists():
            raise FileNotFoundError(f"Symbol directory not found: {sym_dir}")

        years = self._resolve_years(sym_dir, start, end)
        if not years:
            return (
                pd.Series(dtype=float, name=sym),
                pd.Series(dtype=float, name=sym),
                pd.Series(dtype=float, name=sym),
            )

        dfs = []
        for year in years:
            path = sym_dir / f"year_{year}.parquet"
            if path.exists():
                dfs.append(pd.read_parquet(path))

        if not dfs:
            return (
                pd.Series(dtype=float, name=sym),
                pd.Series(dtype=float, name=sym),
                pd.Series(dtype=float, name=sym),
            )

        df = pd.concat(dfs, ignore_index=True)

        # 日期过滤
        if start is not None:
            df = df[df["trade_date"] >= pd.Timestamp(start)]
        if end is not None:
            df = df[df["trade_date"] <= pd.Timestamp(end)]

        if df.empty:
            return (
                pd.Series(dtype=float, name=sym),
                pd.Series(dtype=float, name=sym),
                pd.Series(dtype=float, name=sym),
            )

        return self._compute_daily_returns(df, sym)

    def load_universe(
        self,
        symbols: Optional[list[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        verbose: bool = True,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """批量加载多品种，返回日度首/尾时段收益率宽表。

        Parameters
        ----------
        symbols : list[str] | None
            品种列表；None 时加载 data_dir 下所有可用品种。
        start, end : str | None
            日期范围过滤。
        verbose : bool
            是否打印加载进度。

        Returns
        -------
        first_ret_df : DataFrame, shape (days, n_symbols)
            首时段收益矩阵，DatetimeIndex，列为品种代码。
        last_ret_df : DataFrame, shape (days, n_symbols)
            尾时段收益矩阵，同上。
        """
        if symbols is None:
            symbols = self.available_symbols()

        first_cols: dict[str, pd.Series] = {}
        last_cols: dict[str, pd.Series] = {}

        for sym in symbols:
            try:
                fr, lr, _ = self.load_symbol(sym, start=start, end=end)
                if not fr.empty:
                    first_cols[sym] = fr
                    last_cols[sym] = lr
                    if verbose:
                        n_valid = fr.notna().sum()
                        print(f"  {sym:4s}: {len(fr)} days  ({n_valid} valid)")
            except FileNotFoundError:
                if verbose:
                    print(f"  {sym:4s}: NOT FOUND, skip")

        if not first_cols:
            return pd.DataFrame(), pd.DataFrame()

        first_df = pd.DataFrame(first_cols).sort_index()
        last_df = pd.DataFrame(last_cols).sort_index()
        return first_df, last_df

    # ── 内部实现 ──────────────────────────────────────────────────────────────

    def _resolve_years(
        self,
        sym_dir: Path,
        start: Optional[str],
        end: Optional[str],
    ) -> list[int]:
        """返回需要加载的年份列表（与日期范围重叠的年份）。"""
        available = sorted(
            int(p.stem.replace("year_", ""))
            for p in sym_dir.glob("year_*.parquet")
        )
        if not available:
            return []
        start_year = int(start[:4]) if start else available[0]
        end_year = int(end[:4]) if end else available[-1]
        return [y for y in available if start_year <= y <= end_year]

    def _compute_daily_returns(
        self,
        df: pd.DataFrame,
        symbol: str,
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        """从原始 bar DataFrame 计算每日首/尾时段收益（向量化实现）。"""

        df = df.copy()

        # ── Step 1: 提取时间部分 ────────────────────────────────────────────
        # trade_time 日期固定为 1970-01-01，只取 time 分量
        df["time_of_day"] = df["trade_time"].dt.time

        # ── Step 2: 过滤日盘（排除夜盘 bar）──────────────────────────────────
        day_mask = (
            (df["time_of_day"] >= _DAY_SESSION_MIN)
            & (df["time_of_day"] <= _DAY_SESSION_MAX)
        )
        df = df[day_mask]

        # ── Step 3: 过滤具体合约（排除 XX00 连续合约）────────────────────────
        df = df[df["contract_code"].apply(_is_specific_contract)]

        if df.empty:
            return (
                pd.Series(dtype=float, name=symbol),
                pd.Series(dtype=float, name=symbol),
                pd.Series(dtype=float, name=symbol),
            )

        # ── Step 4: 按日选主力合约（日总成交量最大的具体合约）──────────────
        daily_contract_vol = (
            df.groupby(["trade_date", "contract_code"])["volume"].sum()
        )
        # idxmax 返回 (trade_date, contract_code) MultiIndex 中使 volume 最大的 idx
        main_idx = daily_contract_vol.groupby(level="trade_date").idxmax()
        # 提取合约代码部分
        main_contracts = main_idx.map(lambda x: x[1])
        main_contracts.name = "main_contract"

        df = df.merge(
            main_contracts.reset_index(name="main_contract"),
            on="trade_date",
        )
        df = df[df["contract_code"] == df["main_contract"]].drop(
            columns="main_contract"
        )

        # ── Step 5: 按日期和时间排序（保证 first/last 语义正确）────────────
        df = df.sort_values(["trade_date", "trade_time"]).reset_index(drop=True)

        # ── Step 6: 确定时段边界 ────────────────────────────────────────────
        session = _get_session(symbol)
        day_open: datetime.time = session["day_open"]
        day_close: datetime.time = session["day_close"]

        first_start: datetime.time = day_open
        first_end: datetime.time = _add_minutes(day_open, self.first_period_minutes)   # exclusive

        last_start: datetime.time = _add_minutes(day_close, -self.last_period_minutes) # inclusive
        last_end_excl: datetime.time = _add_minutes(day_close, 1)                      # exclusive（覆盖 15:00 集合竞价 bar）

        # ── Step 7: 向量化计算首/尾时段收益 ─────────────────────────────────
        # 只保留有成交量的 bar
        active = df[df["volume"] > 0].copy()

        first_period = active[
            (active["time_of_day"] >= first_start)
            & (active["time_of_day"] < first_end)
        ]
        last_period = active[
            (active["time_of_day"] >= last_start)
            & (active["time_of_day"] < last_end_excl)
        ]

        def _period_returns(period_df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
            """返回 (open_series, close_series, count_series) 按 trade_date 分组。"""
            grp = period_df.groupby("trade_date")
            opens = grp["open_price"].first().astype(float)
            closes = grp["close_price"].last().astype(float)
            counts = grp["volume"].count()
            ret = (closes / opens - 1.0).where(opens > 0).where(counts >= 2)
            return ret

        first_ret = _period_returns(first_period)
        last_ret = _period_returns(last_period)

        # ── Step 8: 按日总量过滤低流动性交易日 ──────────────────────────────
        daily_vol = df.groupby("trade_date")["volume"].sum()
        low_vol = daily_vol < self.min_daily_volume

        all_dates = pd.DatetimeIndex(sorted(daily_vol.index))
        first_ret = first_ret.reindex(all_dates).where(~low_vol.reindex(all_dates, fill_value=False))
        last_ret = last_ret.reindex(all_dates).where(~low_vol.reindex(all_dates, fill_value=False))
        daily_vol = daily_vol.reindex(all_dates)

        first_ret.name = symbol
        last_ret.name = symbol
        daily_vol.name = symbol

        return first_ret, last_ret, daily_vol
