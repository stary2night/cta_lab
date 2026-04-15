"""ParquetSource：基于目录的 Parquet 文件数据源。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .base import DataSource


class ParquetSource(DataSource):
    """按目录结构管理 Parquet 文件的主力数据源。"""

    def __init__(self, root_dir: str | Path) -> None:
        """初始化 ParquetSource，root_dir 为 parquet 文件根目录。"""
        self.root_dir = Path(root_dir)

    def _key_to_path(self, key: str) -> Path:
        """将 key 转换为对应的 .parquet 文件路径。"""
        return self.root_dir / f"{key}.parquet"

    def read_dataframe(
        self,
        key: str,
        start: str | None = None,
        end: str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """读取指定 key 对应的 parquet 文件，支持 start/end 日期过滤。

        key 是相对于 root_dir 的路径，不含 .parquet 后缀。
        文件不存在时抛出 FileNotFoundError。
        """
        path = self._key_to_path(key)
        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")

        df = pd.read_parquet(path, **kwargs)

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
        """写入 DataFrame 为 parquet 文件，自动创建父目录。"""
        path = self._key_to_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, **kwargs)

    def list_keys(self, prefix: str = "") -> list[str]:
        """递归列出所有 .parquet 文件，返回不带后缀的相对路径列表。"""
        if not self.root_dir.exists():
            return []

        keys: list[str] = []
        for path in sorted(self.root_dir.rglob("*.parquet")):
            rel = path.relative_to(self.root_dir)
            key = str(rel.with_suffix(""))
            if key.startswith(prefix):
                keys.append(key)

        return keys

    def exists(self, key: str) -> bool:
        """判断 key 对应的 parquet 文件是否存在。"""
        return self._key_to_path(key).exists()
