"""JPMChinaDataLoader：从 china_daily_full/ 构建持仓量主力合约日收益率矩阵。

内部使用 cta_lab 的 ParquetSource 读取原始数据，按持仓量（interest）最大值
选取主力合约，基于 settle_price / pre_settle_price 计算日收益率，
过滤掉极端值（|ret| > 0.5）后构成宽表 DataFrame。
"""

from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from data.sources.parquet_source import ParquetSource

from .config import EXCLUDE, MIN_OBS


class JPMChinaDataLoader:
    """加载 china_daily_full/ 数据并构建收益率矩阵。

    Parameters
    ----------
    data_dir:
        china_daily_full/ 目录路径。
    exclude:
        需排除的品种代码集合，默认使用 config.EXCLUDE。
    min_obs:
        品种最少有效观测天数，低于此值的品种被跳过，默认 config.MIN_OBS。
    """

    def __init__(
        self,
        data_dir: str | Path,
        exclude: set[str] | None = None,
        min_obs: int = MIN_OBS,
    ) -> None:
        self.source = ParquetSource(data_dir)
        self.exclude: set[str] = exclude if exclude is not None else set(EXCLUDE)
        self.min_obs = min_obs

    # ── 内部方法 ──────────────────────────────────────────────────────────────

    def _build_ticker_returns(self, ticker: str) -> pd.Series | None:
        """读取单品种 parquet，选主力合约，计算日收益率。"""
        if not self.source.exists(ticker):
            return None

        try:
            df = self.source.read_dataframe(ticker)
        except Exception:
            return None

        # 标准化日期列
        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        else:
            return None

        # 过滤无效行
        df = df.dropna(subset=["contract_code"]).drop_duplicates(
            subset=["contract_code", "trade_date"]
        )
        df_valid = df[df["interest"] > 0].dropna(subset=["interest"])
        if df_valid.empty:
            return None

        # 按日期选持仓量最大的合约（主力合约）
        idx_dom = df_valid.groupby("trade_date")["interest"].idxmax().dropna()
        dom = (
            df_valid.loc[idx_dom]
            .copy()
            .sort_values("trade_date")
            .reset_index(drop=True)
        )

        # 计算日收益率
        pre = dom["pre_settle_price"].replace(0, np.nan)
        dom["ret"] = dom["settle_price"] / pre - 1

        # 过滤极端涨跌（|ret| > 50% 视为数据错误）
        dom.loc[dom["ret"].abs() > 0.5, "ret"] = np.nan

        s = dom.set_index("trade_date")["ret"].rename(ticker)
        return s

    # ── 公开接口 ──────────────────────────────────────────────────────────────

    def available_tickers(self) -> list[str]:
        """返回目录中所有不在排除集的品种列表（已排序）。"""
        all_keys = {Path(k).stem for k in self.source.list_keys()}
        return sorted(all_keys - self.exclude)

    def load_returns(
        self,
        tickers: list[str] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """加载所有（或指定）品种收益率，拼成宽表 DataFrame。

        Parameters
        ----------
        tickers:
            指定品种列表；为 None 时加载全部可用品种。
        verbose:
            True 时打印每个品种的加载摘要。

        Returns
        -------
        DataFrame，index = trade_date（DatetimeIndex），
        columns = 品种代码，值为日收益率（float，含 NaN）。
        """
        if tickers is None:
            tickers = self.available_tickers()

        rets: dict[str, pd.Series] = {}
        skipped: list[str] = []

        for ticker in tickers:
            s = self._build_ticker_returns(ticker)
            if s is None:
                skipped.append(ticker)
                continue

            valid_obs = int(s.notna().sum())
            if valid_obs < self.min_obs:
                skipped.append(ticker)
                if verbose:
                    print(f"  [SKIP] {ticker}: only {valid_obs} obs (< {self.min_obs})")
                continue

            rets[ticker] = s
            if verbose:
                first = s.first_valid_index()
                last = s.last_valid_index()
                print(
                    f"  {ticker:6s}: {first.date()} ~ {last.date()}  ({valid_obs} obs)"
                )

        if skipped and verbose:
            print(f"  Skipped {len(skipped)} tickers: {skipped}")

        if not rets:
            warnings.warn("No valid tickers loaded.", stacklevel=2)
            return pd.DataFrame()

        returns = pd.DataFrame(rets).sort_index()
        returns.index = pd.to_datetime(returns.index)
        return returns
