"""截面动量策略（跨品种动量轮动）。"""

from __future__ import annotations

from strategies.base.cross_sectional import CrossSectionalStrategy


class CrossMOM(CrossSectionalStrategy):
    """截面动量策略（跨品种动量轮动）。"""

    DEFAULT_CONFIG: dict = {
        "score_lookbacks": [63, 126, 252],
        "top_pct": 0.30,
        "bottom_pct": 0.30,
        "target_vol": 0.40,
    }

    def __init__(self, config: dict | None = None) -> None:
        merged = {**self.DEFAULT_CONFIG, **(config or {})}
        super().__init__(merged)
