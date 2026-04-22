"""兼容入口：TSMOM StrategyReport 风格运行脚本。

真实执行逻辑已归一到 ``scripts/run_tsmom.py``。保留该文件是为了兼容
历史命令；默认输出目录、费用与分析开关沿用原 V2 口径。
"""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_CTA_LAB = _HERE.parent
if str(_CTA_LAB) not in sys.path:
    sys.path.insert(0, str(_CTA_LAB))

from scripts.run_tsmom import _parse_args, run


def main() -> None:
    args = _parse_args(
        default_out_dir=_CTA_LAB.parent / "research_outputs" / "tsmom_v2",
        default_fee_rate=0.0003,
        default_analysis=True,
        description="TSMOM — StrategyReport 兼容入口",
    )
    run(args)


if __name__ == "__main__":
    main()
