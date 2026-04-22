"""检查项目 Markdown 文档是否与代码变更保持同步。

用法：
    cd /home/ubuntu/dengl/my_projects/cta_lab
    python3 scripts/docs_sync_check.py

行为：
1. 检查关键文档是否存在。
2. 检查文档中是否包含必须覆盖的关键字/片段。
3. 比较文档与其负责的代码范围的修改时间，若代码更新晚于文档则提示文档可能过期。

退出码：
    0: 所有检查通过
    1: 存在文档缺失、内容缺失或疑似过期
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class DocSpec:
    doc: str
    code_globs: tuple[str, ...]
    required_snippets: tuple[str, ...] = ()
    description: str = ""


DOC_SPECS: tuple[DocSpec, ...] = (
    DocSpec(
        doc="ARCH_REVIEW_ROADMAP.md",
        code_globs=(
            "strategies/implementations/gmat3/*.py",
            "portfolio/**/*.py",
            "backtest/**/*.py",
            "data/**/*.py",
        ),
        required_snippets=(
            "RollStrategyBase",
            "Look-Through",
            "Cross Asset Allocation",
            "signals / portfolio",
        ),
        description="平台架构复盘与通用化升级路线图",
    ),
    DocSpec(
        doc="data/README.md",
        code_globs=(
            "data/*.py",
            "data/model/*.py",
            "data/sources/*.py",
            "scripts/verify_data_layer.py",
            "scripts/check_market_data.py",
        ),
        required_snippets=(
            "nav_output",
            "load_continuous_matrix",
            'contracts/{symbol}',
            'instruments/{symbol}',
            'calendars/{exchange}',
            "continuous/{symbol}_{adjust}_schedule",
        ),
        description="数据层使用说明与运行示例",
    ),
    DocSpec(
        doc="signals/README.md",
        code_globs=(
            "signals/*.py",
            "signals/momentum/*.py",
            "signals/reversal/*.py",
            "signals/risk/*.py",
            "signals/composite/*.py",
        ),
        required_snippets=(
            "Signal",
            "CrossSectionalSignal",
            "momentum/",
            "composite/",
            "operators/",
        ),
        description="信号层说明与开发约定",
    ),
    DocSpec(
        doc="portfolio/README.md",
        code_globs=(
            "portfolio/*.py",
            "portfolio/sizing/*.py",
            "portfolio/constraints/*.py",
            "portfolio/scheduler/*.py",
        ),
        required_snippets=(
            "TopBottomSelector",
            "ThresholdSelector",
            "signal_mode",
            "blend()",
        ),
        description="组合层说明与 signal -> portfolio 约定",
    ),
    DocSpec(
        doc="backtest/README.md",
        code_globs=(
            "backtest/*.py",
            "backtest/event/*.py",
            "backtest/fees/*.py",
            "backtest/execution/*.py",
        ),
        required_snippets=(
            "VectorizedBacktest",
            "EventDrivenBacktestEngine",
            "CostModel",
            "ProportionalCostModel",
            "FixedBpsSlippage",
            "on_start",
            "on_bar",
        ),
        description="回测层范式、成本模型与事件策略写法",
    ),
    DocSpec(
        doc="DESIGN.md",
        code_globs=(
            "data/*.py",
            "data/model/*.py",
            "data/sources/*.py",
            "signals/**/*.py",
            "portfolio/**/*.py",
            "backtest/**/*.py",
            "analysis/**/*.py",
            "strategies/**/*.py",
        ),
        required_snippets=(
            "data/",
            "signals/",
            "portfolio/",
            "backtest/",
            "analysis/",
            "strategies/",
        ),
        description="项目整体架构设计",
    ),
    DocSpec(
        doc="DEV_PROGRESS.md",
        code_globs=(
            "data/**/*.py",
            "signals/**/*.py",
            "portfolio/**/*.py",
            "backtest/**/*.py",
            "analysis/**/*.py",
            "strategies/**/*.py",
            "scripts/*.py",
        ),
        required_snippets=(
            "开发进展记录",
            "完整架构设计见",
            "当前状态",
        ),
        description="开发进展与当前能力概览",
    ),
    DocSpec(
        doc="DOC_SYNC.md",
        code_globs=(
            "scripts/docs_sync_check.py",
            "data/README.md",
            "signals/README.md",
            "portfolio/README.md",
            "backtest/README.md",
            "DESIGN.md",
            "DEV_PROGRESS.md",
        ),
        required_snippets=(
            "docs sync",
            "python3 scripts/docs_sync_check.py",
            "Markdown",
        ),
        description="文档同步工作流说明",
    ),
)


def iter_matches(pattern: str) -> list[Path]:
    return sorted(
        path for path in ROOT.glob(pattern)
        if path.is_file() and "__pycache__" not in path.parts
    )


def newest_mtime(paths: list[Path]) -> float | None:
    if not paths:
        return None
    return max(path.stat().st_mtime for path in paths)


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def main() -> int:
    failed = False

    print("Docs Sync Check\n")

    for spec in DOC_SPECS:
        doc_path = ROOT / spec.doc
        print(f"- {spec.doc}: {spec.description}")

        if not doc_path.exists():
            print("  FAIL missing document")
            failed = True
            continue

        text = doc_path.read_text(encoding="utf-8")
        missing_snippets = [s for s in spec.required_snippets if s not in text]
        if missing_snippets:
            failed = True
            print("  FAIL missing required snippets:")
            for snippet in missing_snippets:
                print(f"    - {snippet}")
        else:
            print("  OK required snippets present")

        code_files: list[Path] = []
        for pattern in spec.code_globs:
            code_files.extend(iter_matches(pattern))
        code_files = sorted(set(code_files))

        if not code_files:
            print("  WARN no matched code files")
            continue

        doc_mtime = doc_path.stat().st_mtime
        latest_code_mtime = newest_mtime(code_files)
        assert latest_code_mtime is not None

        if latest_code_mtime > doc_mtime:
            failed = True
            latest_file = max(code_files, key=lambda path: path.stat().st_mtime)
            print(
                "  FAIL code is newer than doc:"
                f" {rel(latest_file)}"
            )
        else:
            print("  OK doc timestamp is newer than covered code")

    print("\nResult:", "FAIL" if failed else "PASS")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
