"""TSMOM（时序动量）策略回测包。

基于 Moskowitz, Ooi & Pedersen (JFE, 2012) 论文实现，
使用 cta_lab 的 JPM 框架组件（DataLoader / VectorizedBacktest）进行国内期货回测。
"""

from .strategy import TSMOMRunResult, TSMOMStrategy

__all__ = ["TSMOMRunResult", "TSMOMStrategy"]
