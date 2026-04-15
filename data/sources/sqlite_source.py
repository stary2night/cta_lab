"""SQLiteSource：基于 SQLite 的关系型数据源。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from .base import DataSource


class SQLiteSource(DataSource):
    """基于 SQLite 的数据源，使用 pandas read_sql_query / to_sql 进行读写。"""

    def __init__(self, db_path: str | Path) -> None:
        """初始化 SQLiteSource，db_path 为 SQLite 数据库文件路径。"""
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        """创建并返回 SQLite 连接，数据库文件不存在时自动创建。"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(str(self.db_path))

    def read_dataframe(
        self,
        key: str,
        start: str | None = None,
        end: str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """读取指定表名 key 的数据，支持按 date 列过滤 start/end。

        key 为表名。若表不存在抛出 FileNotFoundError（与其他 Source 保持一致）。
        """
        with self._connect() as conn:
            # 检查表是否存在
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (key,)
            )
            if cursor.fetchone() is None:
                raise FileNotFoundError(
                    f"SQLite table '{key}' not found in database: {self.db_path}"
                )

            conditions: list[str] = []
            params: list[str] = []

            if start is not None:
                conditions.append("date >= ?")
                params.append(str(start))
            if end is not None:
                conditions.append("date <= ?")
                params.append(str(end))

            where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
            sql = f'SELECT * FROM "{key}"{where_clause}'

            df = pd.read_sql_query(sql, conn, params=params, **kwargs)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")

        return df

    def write_dataframe(self, key: str, df: pd.DataFrame, **kwargs) -> None:
        """写入 DataFrame 到指定表名 key。

        kwargs 支持 if_exists 参数（默认 'replace'）。
        """
        if_exists: str = kwargs.pop("if_exists", "replace")
        with self._connect() as conn:
            df.to_sql(key, conn, if_exists=if_exists, index=True, **kwargs)

    def list_keys(self, prefix: str = "") -> list[str]:
        """列出数据库中所有表名，可按 prefix 过滤。"""
        if not self.db_path.exists():
            return []

        with self._connect() as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            tables = [row[0] for row in cursor.fetchall()]

        return [t for t in tables if t.startswith(prefix)]

    def exists(self, key: str) -> bool:
        """判断数据库中是否存在指定表名。"""
        if not self.db_path.exists():
            return False

        with self._connect() as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (key,)
            )
            return cursor.fetchone() is not None
