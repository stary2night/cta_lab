# cta_lab 架构复盘与通用化升级 Roadmap

> 最后更新：2026-04-28

这份 roadmap 只保留当前仍然有效的复盘结论与主线，不再重复早期阶段性叙述。

## 复盘结论

GMAT3 在 `cta_lab` 中已经不是概念验证，而是实际落地案例。这件事验证了两点：

1. 当前平台已经可以承载复杂复合策略
2. 平台对复杂复合策略的正式通用抽象还没有完全收口

换句话说，问题不在“能不能跑”，而在“哪些能力已经成为平台能力，哪些仍停留在单策略内部”。

## 当前已经被验证的基础

- `data/ -> signals/ -> portfolio/ -> backtest/ -> analysis/ -> strategies/` 主链已成立
- `signals / portfolio` 分层已经比早期更清晰
- GMAT3 已形成目录化实现，而不是单文件特例
- `strategies/components/roll/` 已经出现 `RollStrategyBase` 这类可复用组件对象
- `multifactor_cta_backtest/`、`netmom_backtest/` 与多 sleeve 组合研究说明，平台已经可以稳定承接“信号 -> sleeve -> 组合”这类中层结构，而不只是一条单策略净值曲线

## 仍然需要继续收口的四条主线

### 主线 1：Roll Strategy 组件化

这条主线当前已经从“纯设计讨论”推进到“有代码、有 profile、有结果对象”的阶段，但还没有完全成为平台标准层。

当前已落地：

- `RollStrategyBase`
- `RollStrategyProfile`
- `RollStrategyResult`
- `SingleAssetRollStrategy`
- `BundleRollStrategy`
- `ValueComposer`
- `LookThroughResolver`

后续重点不是再快速加很多特例，而是：

- 稳定单资产规则族
- 收口 bundle 输入与输出协议
- 继续把复合策略真正需要的共性逻辑抽出来

### 主线 2：signals / portfolio 通用化

GMAT3、JPM 与海外趋势脚本都说明，`signals / portfolio` 已经不再是简单的“信号函数 + 定仓函数”。

当前已经明确的共性能力有：

- signal 与 raw score 分离
- selector 承担 score -> 仓位意图桥接
- `signal_mode` 让 direction / raw 两种语义并存
- `StaggeredScheduler`、`blend()`、`StrategyBlender` 支持更复杂的组合组织
- 复合策略已不再局限于“趋势 sleeve + 截面 sleeve”；近期 `trend / cross / netmom` 以及 `BasisMomentum` overlay 研究表明，平台已经能够承接多 sleeve 低相关组合与轻量权重覆盖层

后续重点是继续把这些约定稳定成更少歧义的公共接口，而不是在策略内部各写一套。

### 主线 3：Look-Through

复杂策略如果只有最终净值，没有穿透结果，平台就只能做展示，难以继续支撑研究解释、风控和执行映射。

Look-Through 仍然是最需要坚持的硬约束。

当前已有基础：

- roll 组件结果中已经带 `lookthrough_book`
- GMAT3 子系统已经存在多层中间对象

后续重点：

- 统一穿透结果字段与粒度
- 区分策略权重、子策略权重、底层资产权重、合约级持仓
- 让上层策略运行结果天然保留可追溯链路

### 主线 4：轻量事件驱动回测

向量化回测已经证明适合当前大部分趋势/动量研究，但它天然绕开领域状态对象。对于未来需要记录组合状态、策略状态、订单成交与稀疏事件推进的策略，平台需要一条轻量事件驱动路径。

当前已落地第一阶段骨架：

- `backtest/event/events.py`：`EventType` 与通用 `Event`
- `backtest/event/market.py`：`MarketSnapshot`
- `backtest/event/order.py`：`Order`、`Fill`、`Transaction` 与订单状态枚举
- `backtest/event/state.py`：`PositionState`、`PortfolioState`、`StrategyState`
- `backtest/event/context.py`：`SimulationContext`
- `strategies/base/event_driven.py`：`EventDrivenStrategy` hook 协议

当前已落地第二阶段最小闭环：

- `backtest/event/data_portal.py`：`MarketDataPortal`
- `backtest/event/engine.py`：`EventDrivenBacktestEngine`
- `backtest/event/broker.py`：`SimulatedBroker`
- `backtest/event/recorder.py`：`EventRecorder`
- `tests/test_backtest_module/test_event_engine.py`：target-weight 策略与 commission 的最小回归测试

当前已落地第三阶段正确性桥接：

- `backtest/event/adapters.py`：`TargetWeightStrategyAdapter`
- 可将现有 `weights_df` 包装成 callback 策略，交给事件驱动引擎运行
- 对照测试已验证 `lag=0`、`fee=0`、每日权重口径下，事件驱动结果可与 `VectorizedBacktest` 对齐

当前已落地第四阶段研究口径增强：

- `TargetWeightStrategyAdapter.execution_lag`：支持按权重矩阵索引延迟执行目标权重
- `TargetWeightStrategyAdapter.rebalance_dates`：支持稀疏调仓日，非调仓日只做持仓 mark-to-market
- `EventRecorder`：turnover 改为基于真实成交 notional，而不是基于价格漂移后的权重差
- 新增测试验证稀疏调仓下的权重漂移、零交易换手、延迟执行日期，以及 close-to-close 调仓对齐 `VectorizedBacktest(lag=1)`

当前已落地第五阶段策略层接入：

- `strategies/base/vectorized.py`：`VectorizedStrategy`
- `strategies/base/event_driven.py`：`EventDrivenStrategy.run_event_backtest(...)`
- `strategies/examples/event_driven.py`：`SimpleRelativeMomentumEventStrategy`
- `notebooks/事件驱动回测最小示例.ipynb`：从策略层导入事件驱动样板，而不是在 notebook 内临时定义策略

当前已开始把事件驱动范式接入正式策略包：

- `strategies/implementations/jpm_trend_trade/event_strategy.py`：`JPMEventDrivenStrategy`
- JPM 事件策略将 t-stat、sigma 与 CorrCap rolling cache 等市场特征前移到 `on_start()` 预计算，`on_bar()` 只读取当天特征并处理状态/发单
- 合成数据测试已覆盖 baseline 与 CorrCap 两条事件驱动回测路径

这条主线的边界也很明确：借鉴 backtrader、vn.py、rqalpha 的事件和状态设计，但只保留研究回测需要的轻量抽象，不做 gateway、实盘连接、异步撮合或 UI 平台。
第二阶段仍然坚持这个边界：当前 broker 采用 snapshot 价格为参考立即成交，已补齐统一 `cost_model`、固定 bps 滑点和成交 notional 费用记录；后续再逐步补拒单、部分成交和更复杂冲击曲线等研究必要细节。
基于这套成本模型，典型向量化策略入口也已回填 `--cost-bps` 与换手/成本报告输出；`VectorizedBacktest` 在启用 `vol_target` 时按 vol-target 后的有效执行权重计算换手和成本，并且热身期不向前回填未来 scale，使中低频 CTA 研究优先覆盖交易成本和换手暴露，而不是过早推进拒单或成交量约束。

## 当前不作为近期主线的方向

以下方向可能有价值，但当前不再作为近期 roadmap 主路径：

- 独立的大而全 `IndexEngine`
- 过早抽象的统一多币种 engine
- 为单一 legacy 路径做过度兼容
- 生产交易系统式的 gateway、实盘事件总线与完整订单管理平台

原因很简单：这些方向还没有比 `Roll Strategy`、`Cross Asset Allocation`、`Look-Through`、轻量事件驱动回测更清晰的公共边界。

## 当前建议的目标层次

围绕复杂复合策略，当前最清晰的目标层次仍然是四层：

1. Roll Strategy Layer
   输出资产级价值序列、roll 信息与 Look-Through
2. Cross Asset Allocation Layer
   负责 Cross Asset Allocation、状态判断与筛选
3. Portfolio Construction Layer
   负责预算、约束、调度、应用时点、融合
4. Composite Strategy Assembly Layer
   把多层结果组装成完整策略与完整输出

## 当前路线判断

这份 roadmap 现在不再强调“要不要做平台通用化”，因为答案已经很明确：要，而且已经开始做。

当前真正重要的是：

- 只回收已经稳定的共性
- 让 `RollStrategyBase`、`signals / portfolio`、Look-Through、事件驱动状态对象继续从“可用”走向“可复用”
- 继续用真实策略验证抽象，而不是闭门设计

一句话概括当前 roadmap：

以 GMAT3 为验证样本，把复杂策略真正需要的中间层收回平台，但只做已经看清的部分。
