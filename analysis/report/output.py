"""BacktestOutput — 回测结果输出管理器。

统一处理目录创建、CSV 写入、图表保存和数据文件写入，
消除各脚本入口中重复的 IO 样板代码。

用法::

    out = BacktestOutput(args.out_dir, subdirs=["reports", "charts", "data"])

    # 保存图表（自动关闭 Figure，打印确认）
    out.save_fig(fig, "charts", "nav.png", dpi=150, bbox_inches="tight")

    # 保存 CSV / Parquet / JSON
    out.save_csv(df, "reports", "annual_stats.csv")
    out.save_parquet(df, "data", "returns.parquet")
    out.save_json({"key": "val"}, "data", "info.json")

    # 直接访问 Path 对象（用于自定义保存）
    path = out["signals"] / "positions.parquet"

    # 打印所有输出目录
    out.summary()
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence

import matplotlib.pyplot as plt
import pandas as pd


class BacktestOutput:
    """统一管理回测输出目录与文件写入。"""

    def __init__(
        self,
        out_dir: str | Path,
        subdirs: Sequence[str] = ("reports", "charts", "data"),
    ) -> None:
        self.root = Path(out_dir)
        self.dirs: dict[str, Path] = {s: self.root / s for s in subdirs}
        for d in self.dirs.values():
            d.mkdir(parents=True, exist_ok=True)

    # ── 路径访问 ──────────────────────────────────────────────────────────────

    def __getitem__(self, key: str) -> Path:
        return self.dirs[key]

    def path(self, subdir: str, filename: str) -> Path:
        """返回 subdir/filename 的完整 Path。"""
        return self.dirs[subdir] / filename

    # ── 文件写入 ──────────────────────────────────────────────────────────────

    def save_fig(self, fig, subdir: str, filename: str, **savefig_kwargs) -> None:
        """保存 matplotlib Figure，自动关闭并打印确认消息。"""
        fig.savefig(str(self.dirs[subdir] / filename), **savefig_kwargs)
        plt.close(fig)
        print(f"  Saved {filename}")

    def save_csv(self, df: pd.DataFrame, subdir: str, filename: str) -> None:
        """将 DataFrame 保存为 CSV。"""
        df.to_csv(self.dirs[subdir] / filename)

    def save_parquet(self, df: pd.DataFrame, subdir: str, filename: str) -> None:
        """将 DataFrame 保存为 Parquet。"""
        df.to_parquet(self.dirs[subdir] / filename)

    def save_json(self, data, subdir: str, filename: str) -> None:
        """将对象序列化为 JSON 文件。"""
        with open(self.dirs[subdir] / filename, "w") as f:
            json.dump(data, f, indent=2)

    # ── 辅助 ──────────────────────────────────────────────────────────────────

    def summary(self) -> None:
        """打印所有输出目录路径。"""
        for name, path in self.dirs.items():
            print(f"  {name:10s}: {path}/")
