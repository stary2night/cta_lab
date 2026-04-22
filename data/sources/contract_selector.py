"""ContractSelector：主力合约选择策略接口。

将"每日从多合约 DataFrame 中选出主力合约"的逻辑抽象为可复用接口，
使 JPMChinaDataLoader 及其他数据加载器能灵活替换选择策略。

内置实现
--------
MaxInterestSelector（默认）：按持仓量（OI）选主力，与原 JPMChinaDataLoader 行为完全一致。
MaxVolumeSelector：按成交量选主力。

用法示例
--------
>>> from data.sources.contract_selector import MaxInterestSelector
>>> selector = MaxInterestSelector()
>>> dom = selector.select(raw_df)          # → 每日主力合约 DataFrame
>>> ret = selector.build_returns(raw_df)   # → 日收益率 Series 或 None
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd


class ContractSelector(ABC):
    """主力合约选择器基类。

    子类只需实现 select()，build_returns() 提供完整的"选合约→算收益"流水线。

    输入 DataFrame 约定
    -------------------
    - 含列 ``trade_date``（日期）、``contract_code``（合约代码）
    - 含 ``settle_price`` 和 ``pre_settle_price``（结算价，用于计算收益率）
    - 具体 selector 还依赖各自的筛选列（如 ``interest``、``volume``）
    """

    @abstractmethod
    def select(self, df: pd.DataFrame) -> pd.DataFrame | None:
        """从多合约宽格式 DataFrame 中选出每日主力合约。

        Parameters
        ----------
        df:
            原始多合约 DataFrame，含 ``trade_date`` 列。

        Returns
        -------
        每日主力合约 DataFrame，以 ``trade_date`` 为 index，按日期升序排列。
        若无有效数据则返回 None。
        """

    def build_returns(self, df: pd.DataFrame, ticker: str = "") -> pd.Series | None:
        """完整流水线：select() → 计算日收益率。

        Parameters
        ----------
        df:
            原始多合约 DataFrame。
        ticker:
            品种代码（仅用于 Series.name 命名）。

        Returns
        -------
        日收益率 Series（name=ticker，index=trade_date），或 None（数据不足）。
        """
        dom = self.select(df)
        if dom is None or dom.empty:
            return None
        if {"settle_price", "pre_settle_price"} - set(dom.columns):
            return None

        dom = dom.copy()
        dom["ret"] = dom["settle_price"] / dom["pre_settle_price"] - 1.0
        # 过滤明显异常涨跌（>50%，通常是数据错误或首日无 pre_settle）
        dom.loc[dom["ret"].abs() > 0.5, "ret"] = np.nan

        series = dom["ret"]
        series.name = ticker
        return series


class MaxInterestSelector(ContractSelector):
    """按持仓量（Open Interest）选择主力合约（每日 OI 最大者）。

    这是国内期货研究中最常用的主力合约定义，与原 JPMChinaDataLoader 行为完全兼容。

    Parameters
    ----------
    interest_col:
        持仓量列名，默认 ``'interest'``。
    min_interest:
        最低有效持仓量；低于此值的行被视为无效，默认 0（严格大于 0）。
    """

    def __init__(
        self,
        interest_col: str = "interest",
        min_interest: float = 0.0,
    ) -> None:
        self.interest_col = interest_col
        self.min_interest = min_interest

    def select(self, df: pd.DataFrame) -> pd.DataFrame | None:
        if self.interest_col not in df.columns:
            return None

        df_valid = df[df[self.interest_col] > self.min_interest].dropna(
            subset=[self.interest_col]
        )
        if df_valid.empty:
            return None

        idx_dom = (
            df_valid.groupby("trade_date")[self.interest_col]
            .idxmax()
            .dropna()
        )
        dom = df_valid.loc[idx_dom].sort_values("trade_date")
        return dom.set_index("trade_date")


class MaxVolumeSelector(ContractSelector):
    """按成交量选择主力合约（每日成交量最大者）。

    Parameters
    ----------
    volume_col:
        成交量列名，默认 ``'volume'``。
    min_volume:
        最低有效成交量，默认 0。
    """

    def __init__(
        self,
        volume_col: str = "volume",
        min_volume: float = 0.0,
    ) -> None:
        self.volume_col = volume_col
        self.min_volume = min_volume

    def select(self, df: pd.DataFrame) -> pd.DataFrame | None:
        if self.volume_col not in df.columns:
            return None

        df_valid = df[df[self.volume_col] > self.min_volume].dropna(
            subset=[self.volume_col]
        )
        if df_valid.empty:
            return None

        idx_dom = (
            df_valid.groupby("trade_date")[self.volume_col]
            .idxmax()
            .dropna()
        )
        dom = df_valid.loc[idx_dom].sort_values("trade_date")
        return dom.set_index("trade_date")
