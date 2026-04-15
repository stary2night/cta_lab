"""DDBSource：DolphinDB 数据源存根（待实现）。"""

from __future__ import annotations

import pandas as pd

from .base import DataSource


class DDBSource(DataSource):
    """DolphinDB 数据源（存根，待实现）。"""

    def __init__(
        self,
        host: str,
        port: int,
        username: str = "",
        password: str = "",
    ) -> None:
        """初始化 DDBSource（当前直接抛出 NotImplementedError）。"""
        raise NotImplementedError("DDBSource 尚未实现，请使用 ParquetSource")

    def read_dataframe(self, key: str, **kwargs) -> pd.DataFrame:
        """读取 DolphinDB 表（未实现）。"""
        raise NotImplementedError("DDBSource 尚未实现，请使用 ParquetSource")

    def write_dataframe(self, key: str, df: pd.DataFrame, **kwargs) -> None:
        """写入 DolphinDB 表（未实现）。"""
        raise NotImplementedError("DDBSource 尚未实现，请使用 ParquetSource")

    def list_keys(self, prefix: str = "") -> list[str]:
        """列出 DolphinDB 表名（未实现）。"""
        raise NotImplementedError("DDBSource 尚未实现，请使用 ParquetSource")

    def exists(self, key: str) -> bool:
        """判断 DolphinDB 表是否存在（未实现）。"""
        raise NotImplementedError("DDBSource 尚未实现，请使用 ParquetSource")
