"""strategies 层共享运行时上下文。"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from backtest.vectorized import VectorizedBacktest
from data.loader import DataLoader


@dataclass(slots=True)
class StrategyContext:
    """集成策略运行所需的核心依赖。

    这里承载运行时依赖，而不是策略公式本身：
    - `loader`: 数据访问入口
    - `sector_map`: 品种到板块的映射
    - `backtest`: 可选，外部注入的回测器
    """

    loader: DataLoader
    sector_map: dict[str, str]
    backtest: VectorizedBacktest | None = None

    def available_symbols(self, *, exclude: set[str] | None = None) -> list[str]:
        """列出当前数据源中的可用品种。"""
        return self.loader.available_symbols(exclude=exclude)

    def load_returns_matrix(
        self,
        *,
        tickers: list[str] | None = None,
        start: str | None = None,
        end: str | None = None,
        min_obs: int,
        exclude: set[str] | None = None,
    ) -> pd.DataFrame:
        """通过统一 DataLoader 读取收益率宽表。"""
        symbols = tickers if tickers is not None else self.available_symbols(exclude=exclude)
        return self.loader.load_returns_matrix(symbols, start=start, end=end, min_obs=min_obs)

    def resolve_sector_map(
        self,
        symbols: list[str] | pd.Index,
        sector_map: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """将 symbol 列表映射到板块；缺失项归入 Other。"""
        base = sector_map if sector_map is not None else self.sector_map
        return {str(symbol): base.get(str(symbol), "Other") for symbol in symbols}
