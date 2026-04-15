"""信号分析子包。"""

from .labels import forward_return, forward_log_return, build_forward_returns
from .evaluator import SignalEvaluationReport, information_coefficient, information_ratio, evaluate_signal
from .persistence import momentum_persistence
from .long_short import long_short_asymmetry
from .correlation import correlation_analysis

__all__ = [
    "forward_return",
    "forward_log_return",
    "build_forward_returns",
    "SignalEvaluationReport",
    "information_coefficient",
    "information_ratio",
    "evaluate_signal",
    "momentum_persistence",
    "long_short_asymmetry",
    "correlation_analysis",
]
