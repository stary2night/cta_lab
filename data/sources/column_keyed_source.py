"""ColumnKeyedSource：从单一文件（parquet 或 csv）按列值过滤的数据源。

适用于"大表"型数据，例如所有期货合约信息存于一个文件，按品种代码过滤。

key 映射规则：filter_col 列的值等于 key 的所有行。
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .base import DataSource


class ColumnKeyedSource(DataSource):
    """从单一文件中按列值过滤的数据源，支持 parquet 和 csv 格式。

    文件在首次访问时懒加载，后续读取直接在内存中过滤。
    与 ParquetSource/CSVSource（每个 key 对应独立文件）不同，
    本类适用于所有 key 共享一张大表的场景。
    """

    def __init__(self, file_path: str | Path, filter_col: str) -> None:
        """初始化 ColumnKeyedSource。

        Args:
            file_path: 数据文件路径（.parquet 或 .csv）。
            filter_col: 用于过滤的列名，key 应与该列的值完全匹配。
                        例如合约大表用 'fut_code'（值为 'RB'、'HC'…），
                        日历大表用 'exchange'（值为 'SHF'、'DCE'…）。
        """
        self._path = Path(file_path)
        self._filter_col = filter_col
        self._df: pd.DataFrame | None = None

    def _load(self) -> pd.DataFrame:
        """懒加载整张表。"""
        if self._df is None:
            if not self._path.exists():
                raise FileNotFoundError(f"Data file not found: {self._path}")
            if self._path.suffix == ".parquet":
                self._df = pd.read_parquet(self._path)
            else:
                self._df = pd.read_csv(self._path)
        return self._df

    def read_dataframe(
        self,
        key: str,
        start: str | None = None,
        end: str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """返回 filter_col == key 的所有行。

        start / end 参数对无 DatetimeIndex 的合约/日历表无效，忽略。
        """
        df = self._load()
        result = df[df[self._filter_col] == key].copy()
        if result.empty:
            raise KeyError(
                f"No rows found where {self._filter_col}='{key}' in {self._path.name}"
            )
        return result

    def write_dataframe(self, key: str, df: pd.DataFrame, **kwargs) -> None:
        raise NotImplementedError("ColumnKeyedSource is read-only.")

    def list_keys(self, prefix: str = "") -> list[str]:
        """返回 filter_col 列的所有唯一值（可选按前缀过滤）。"""
        df = self._load()
        keys = df[self._filter_col].dropna().unique().tolist()
        return [str(k) for k in keys if str(k).startswith(prefix)]

    def exists(self, key: str) -> bool:
        df = self._load()
        return (df[self._filter_col] == key).any()
