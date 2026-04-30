"""signals.network — 网络动量信号模块。

基于 Pu et al. (2023) "Network Momentum across Asset Classes"：
  - 构建8维动量特征矩阵（vol-scaled returns + MACD）
  - 通过图学习推断资产间动量溢出网络
  - 将邻居特征经网络传播，作为 Ridge 回归的协变量预测未来收益
"""

from .features import MomentumFeatureBuilder
from .graph_learner import NetworkGraphLearner
from .network_momentum_signal import NetworkMomentumSignal

__all__ = [
    "MomentumFeatureBuilder",
    "NetworkGraphLearner",
    "NetworkMomentumSignal",
]
