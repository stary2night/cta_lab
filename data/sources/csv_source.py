"""CSVSource：基于目录的 CSV 文件数据源。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .base import DataSource


class CSVSource(DataSource):
    """按目录结构管理 CSV 文件的数据源。"""

    def __init__(
        self,
        root_dir: str | Path,
        index_col: str = "date",
        parse_dates: bool = True,
    ) -> None:
        """初始化 CSVSource。

        index_col 默认 'date'，read 时自动解析为 DatetimeIndex。
        """
        self.root_dir = Path(root_dir)
        self.index_col = index_col
        self.parse_dates = parse_dates

    def _key_to_path(self, key: str) -> Path:
        """将 key 转换为对应的 .csv 文件路径。"""
        return self.root_dir / f"{key}.csv"

    def read_dataframe(
        self,
        key: str,
        start: str | None = None,
        end: str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """读取指定 key 对应的 CSV 文件，支持 start/end 日期过滤。

        key 是相对于 root_dir 的路径，不含 .csv 后缀。
        文件不存在时抛出 FileNotFoundError。
        """
        path = self._key_to_path(key)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

        read_kwargs = dict(kwargs)
        read_kwargs.setdefault("index_col", self.index_col)
        if self.parse_dates:
            read_kwargs.setdefault("parse_dates", True)

        df = pd.read_csv(path, **read_kwargs)

        if self.parse_dates and not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        if start is not None or end is not None:
            if not isinstance(df.index, pd.DatetimeIndex):
                raise TypeError(
                    f"Cannot filter by date: index of '{key}' is not DatetimeIndex."
                )
            if start is not None:
                df = df[df.index >= pd.Timestamp(start)]
            if end is not None:
                df = df[df.index <= pd.Timestamp(end)]

        return df

    def write_dataframe(self, key: str, df: pd.DataFrame, **kwargs) -> None:
        """写入 DataFrame 为 CSV 文件，自动创建父目录。"""
        path = self._key_to_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, **kwargs)

    def list_keys(self, prefix: str = "") -> list[str]:
        """递归列出所有 .csv 文件，返回不带后缀的相对路径列表。"""
        if not self.root_dir.exists():
            return []

        keys: list[str] = []
        for path in sorted(self.root_dir.rglob("*.csv")):
            rel = path.relative_to(self.root_dir)
            key = str(rel.with_suffix(""))
            if key.startswith(prefix):
                keys.append(key)

        return keys

    def exists(self, key: str) -> bool:
        """判断 key 对应的 CSV 文件是否存在。"""
        return self._key_to_path(key).exists()
