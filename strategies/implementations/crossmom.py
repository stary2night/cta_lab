"""CrossMOM 兼容入口。

当前正式实现已迁移至 `strategies/implementations/crossmom_backtest/`。
这个文件保留旧导入路径兼容性。
"""

from __future__ import annotations

from strategies.implementations.crossmom_backtest.strategy import CrossMOMStrategy


class CrossMOM(CrossMOMStrategy):
    """旧名称兼容别名，指向新的 CrossMOMStrategy。"""
