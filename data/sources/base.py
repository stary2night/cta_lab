"""DataSource 抽象基类：定义数据源读写接口。"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class DataSource(ABC):
    """数据源抽象基类：负责与存储介质对话，返回原始 DataFrame。"""

    @abstractmethod
    def read_dataframe(self, key: str, **kwargs) -> pd.DataFrame:
        """读取一张表/一个文件，返回原始 DataFrame。

        key 的含义由各实现定义（文件路径、表名等）。
        kwargs 支持 start/end 日期过滤等常用参数。
        """

    @abstractmethod
    def write_dataframe(self, key: str, df: pd.DataFrame, **kwargs) -> None:
        """写入 DataFrame 到存储。"""

    @abstractmethod
    def list_keys(self, prefix: str = "") -> list[str]:
        """列出可用的 key（文件名、表名等）。"""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """判断 key 是否存在。"""
