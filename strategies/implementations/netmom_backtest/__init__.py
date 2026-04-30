"""netmom_backtest — 网络动量策略回测包。

基于 Pu et al. (2023) "Network Momentum across Asset Classes"。

快速开始：
    from strategies.implementations.netmom_backtest import NetMOMStrategy, NetMOMConfig

    strategy = NetMOMStrategy(
        config=NetMOMConfig(mode="combo", graph_method="feature_sim"),
        data_dir="/path/to/china_daily_full/",
        verbose=True,
    )
    result = strategy.run_pipeline()
    print(result.summary())
"""

from .config import NetMOMConfig, coerce_config
from .result import NetMOMRunResult
from .strategy import NetMOMStrategy

__all__ = [
    "NetMOMConfig",
    "NetMOMRunResult",
    "NetMOMStrategy",
    "coerce_config",
]
