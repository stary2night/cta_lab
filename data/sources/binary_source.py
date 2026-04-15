"""BinarySource：支持 HDF5 和 Feather 格式的二进制数据源。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .base import DataSource

_SUPPORTED_FMTS = ("feather", "hdf5")


class BinarySource(DataSource):
    """支持 HDF5 (.h5) 和 Feather (.feather) 格式的数据源。"""

    def __init__(
        self,
        root_dir: str | Path,
        fmt: str = "feather",
    ) -> None:
        """初始化 BinarySource。

        fmt 可选 'feather' 或 'hdf5'，默认 'feather'。
        """
        if fmt not in _SUPPORTED_FMTS:
            raise ValueError(f"Unsupported format '{fmt}'. Choose from {_SUPPORTED_FMTS}.")
        self.root_dir = Path(root_dir)
        self.fmt = fmt

    def _key_to_path(self, key: str) -> Path:
        """将 key 转换为对应格式的文件路径。"""
        ext = ".feather" if self.fmt == "feather" else ".h5"
        return self.root_dir / f"{key}{ext}"

    def _glob_pattern(self) -> str:
        """返回与当前格式匹配的 glob 模式。"""
        return "*.feather" if self.fmt == "feather" else "*.h5"

    def read_dataframe(
        self,
        key: str,
        start: str | None = None,
        end: str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """读取指定 key 对应的二进制文件，支持 start/end 日期过滤。

        key 是相对于 root_dir 的路径，不含文件后缀。
        文件不存在时抛出 FileNotFoundError。
        """
        path = self._key_to_path(key)
        if not path.exists():
            raise FileNotFoundError(f"{self.fmt.upper()} file not found: {path}")

        if self.fmt == "feather":
            df = pd.read_feather(path, **kwargs)
            # feather 不保留 index，约定 'date' 列为索引
            if "date" in df.columns:
                df = df.set_index("date")
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
        else:
            hdf_key = kwargs.pop("hdf_key", "data")
            df = pd.read_hdf(path, key=hdf_key, **kwargs)

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
        """写入 DataFrame 为 feather 或 HDF5 文件，自动创建父目录。"""
        path = self._key_to_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)

        if self.fmt == "feather":
            # feather 要求 RangeIndex 或命名列，将 index 重置后写入
            df_to_write = df.reset_index()
            df_to_write.to_feather(path, **kwargs)
        else:
            hdf_key = kwargs.pop("hdf_key", "data")
            df.to_hdf(path, key=hdf_key, **kwargs)

    def list_keys(self, prefix: str = "") -> list[str]:
        """递归列出所有对应格式文件，返回不带后缀的相对路径列表。"""
        if not self.root_dir.exists():
            return []

        ext = ".feather" if self.fmt == "feather" else ".h5"
        keys: list[str] = []
        for path in sorted(self.root_dir.rglob(self._glob_pattern())):
            rel = path.relative_to(self.root_dir)
            key = str(rel.with_suffix(""))
            if key.startswith(prefix):
                keys.append(key)

        return keys

    def exists(self, key: str) -> bool:
        """判断 key 对应的文件是否存在。"""
        return self._key_to_path(key).exists()
