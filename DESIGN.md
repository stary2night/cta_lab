# cta_lab 系统设计文档

> 最后更新：2026-04-23

`cta_lab` 当前已经形成一套可复用的 CTA 研究框架，目标是用统一分层承接：

- 国内期货趋势研究
- 海外期货趋势研究
- 多资产配置型复合策略
- notebook 级信号研究与策略验证

这份文档只记录当前已经落地的设计，不再保留过多阶段性口号。项目阶段状态见 [DEV_PROGRESS.md](DEV_PROGRESS.md)。

## 当前总体分层

```text
cta_lab/
├── data/          # 数据模型、数据源适配、统一 DataLoader
├── signals/       # 时序/截面信号、组合器、operators
├── portfolio/     # selector、sizer、constraint、scheduler、blender
├── backtest/      # 事件驱动与向量化回测、执行与费用
├── analysis/      # 指标、信号评估、Decomposer 报告体系
├── strategies/    # 策略基类、正式实现、components、研究原型
├── scripts/       # 运行脚本、数据脚本、回归与文档检查
├── notebooks/     # 研究与复现实验入口
└── tests/         # 单元测试与回归测试
```

框架主链仍然是：

`data/ -> signals/ -> portfolio/ -> backtest/ -> analysis/ -> strategies/`

其中 `strategies/` 是组装层，负责把下层能力拼成可运行策略；`scripts/`、`notebooks/`、`tests/` 是研发入口，而不是额外业务层。

## data/

`data/` 负责把原始文件或表转换成领域对象，不承载策略逻辑。

当前由三部分组成：

- `data/model/`：`BarSeries`、`Contract`、`Instrument`、`TradingCalendar`、`ContractSchedule`、`ContinuousSeries`
- `data/sources/`：`ParquetSource`、`CSVSource`、`SQLiteSource`、`ColumnKeyedSource`、`BinarySource`
- `data/loader.py`：统一 `DataLoader`

当前 `DataLoader` 的定位很明确：

- 统一管理 `kline_source`、`contract_source`、`calendar_source`、`instrument_source`
- 通过 `KlineSchema`、`ContractSchema`、`InstrumentSchema` 适配不同原始格式
- 对外提供 `load_bar_series()`、`load_continuous()`、`load_continuous_matrix()`、`load_returns_matrix()`、`available_symbols()`、`load_instrument()`、`load_calendar()`

当前已经明确的设计点：

- 当未单独注入数据源时，支持 `contracts/{symbol}`、`instruments/{symbol}`、`calendars/{exchange}` 约定式回退
- `KlineSchema.tushare()` 与 `KlineSchema.overseas()` 已作为两条真实数据路径的标准入口
- 连续合约构建仍放在 data 层，但研究型 roll 逻辑不强行塞进 `ContinuousSeries`
- notebook 场景下优先支持批量接口，如 `load_continuous_matrix()`

## signals/

`signals/` 只负责把价格、收益率或矩阵变成信号，不负责未来收益评估、定仓和回测。

当前顶层抽象有两类：

- `Signal`：单资产时序信号
- `CrossSectionalSignal`：多资产截面信号

当前目录职责：

- `momentum/`：TSMOM、Sharpe、Absolute、Percentile、NLTSMOM、JPM t-stat、Dual Momentum、MultiFactorTrend、MultiFactorCrossSectionalMomentum
- `reversal/`：MASS260Reversal
- `risk/`：TVS
- `composite/`：`LinearCombiner`、`RankCombiner`
- `operators/`：lag、smooth、clip、zscore、rolling_zscore、winsorize、cross_sectional_rank、normalize_by_abs_sum

当前信号层约定：

- 输出始终是浮点序列或矩阵
- 前导 `NaN` 表示预热不足，不隐式当成零信号
- `signals/` 输出的是预测信息或中间 score，不直接承担定仓语义

## portfolio/

`portfolio/` 负责把已经可定仓的信号或仓位意图转换成目标权重。

当前结构：

- `selectors.py`：`TopBottomSelector`、`ThresholdSelector`
- `sizing/`：`EqualRiskSizer`、`RiskBudgetSizer`、`CorrCapSizer`
- `constraints/`：`WeightCap`、`vol_scaler.py` 中的 WAF
- `scheduler/`：`MonthlyScheduler`、`StaggeredScheduler`
- `blender.py`：`blend()` 与 `StrategyBlender`
- `fx_handler.py`：FX 重估辅助

当前关键设计点：

- `signal_df` 默认表示仓位意图，而不是任意原始 score
- `signal_mode="direction" | "raw"` 用来区分方向信号和强度信号
- selector 与 sizer 分离，避免把“排序分数”和“可执行仓位”混为一谈
- `blend()` 只在各子组合自身有效区间内 `ffill`
- `StrategyBlender` 提供“多策略组合 -> 向量化回测”的高层入口

## backtest/

`backtest/` 负责执行层与净值层，不负责生成信号。

当前包含两条主路径：

- `BacktestEngine`：事件驱动，适合稀疏调仓、FX 双轨、VRS、费用计提
- `VectorizedBacktest`：纯矩阵研究路径，适合快速比较策略；支持 `cost_model`，旧 `fee_rate` 仍作为兼容入口保留；启用 `vol_target` 时按缩放后的有效执行权重计算换手和成本，且 vol-target 热身期不回填未来 scale
- `costs.py`：统一轻量成本模型，包括 `ZeroCostModel`、`ProportionalCostModel`、`DailyAccrualCostModel`、`CompositeCostModel`，供向量化与事件驱动路径复用
- `slippage.py`：事件驱动执行滑点模型，包括 `NoSlippage` 与 `FixedBpsSlippage`

辅助模块：

- `position.py`：`SimpleTracker`、`FXTracker`
- `fees/`：`ZeroFee`、`TradingFee`、`TrackingFee`
- `execution/`：`apply_lag()`、`VRS`
- `event/`：轻量事件驱动研究引擎，包括 `MarketDataPortal`、`EventDrivenBacktestEngine`、`SimulatedBroker`、`EventRecorder`、`TargetWeightStrategyAdapter`，以及 `Event`、`MarketSnapshot`、`Order`、`Fill`、`PortfolioState`、`StrategyState`、`SimulationContext`
- `walk_forward.py`：滚动 OOS 验证
- `result.py`：统一 `BacktestResult`

当前需要区分两类“事件驱动”：

- 既有 `BacktestEngine` 是权重矩阵驱动的逐日状态推进器，已经用于稀疏调仓、费用与 FX/VRS 场景
- 新增 `backtest/event/` 是面向 callback/broker/portfolio-state 范式的轻量事件驱动研究路径；第二阶段已经形成 DataFrame market portal、立即成交 broker、事件循环与 recorder 的最小闭环，第三阶段通过 `TargetWeightStrategyAdapter` 打通现有权重矩阵与事件驱动策略协议，第四阶段补齐稀疏调仓、真实交易换手与 adapter 级执行延迟，并保持轻量运行时依赖，不改变现有向量化策略路径

当前 `backtest/event/` 的边界是研究回测，而不是生产交易系统：订单以 snapshot 价格为参考立即成交，已支持统一 `cost_model`、固定 bps 滑点和真实成交 notional 换手记录；第四阶段已明确 close-to-close 事件调仓与 `VectorizedBacktest(lag=1)` 的对齐关系，尚不处理异步撮合、部分成交、复杂冲击曲线、交易所 gateway 或实盘风控。

当前 `BacktestResult` 已明确包含：

- `nav`
- `returns`
- `positions_df`
- `turnover_series`

以及可选 verbose 日志字段。

## analysis/

`analysis/` 负责策略结果解释，不再只是一个指标工具包。

当前分成三类能力：

- 通用绩效统计：`metrics.py`
- 信号研究：`analysis/signal/`，包括 future returns、IC、IR、long-short 分析、persistence
- 报告编排：`AnalysisContext` + `Decomposer` + `StrategyReport`

当前 `StrategyReport` 已从单体报告函数演进为 Decomposer compositor：

- `PerformanceDecomposer`
- `AttributionDecomposer`
- `SectorDecomposer`
- `CrisisDecomposer`
- `SignalDecomposer`
- `LongShortDecomposer`

这使得分析层可以按上下文自动跳过不满足输入条件的维度，而不是在单个巨型函数里堆逻辑。

## strategies/

`strategies/` 是平台组装层，目前由四块组成：

- `base/`：`StrategyBase`、`VectorizedStrategy`、`EventDrivenStrategy`、`CrossSectionalStrategy`
- `implementations/`：正式策略实现
- `components/`：开始沉淀跨策略复用组件
- `roll_research/`：研究型 roll 规则实验

### 策略基类

`StrategyBase` 当前定位为旧有矩阵策略的兼容基类，支持两条矩阵型路径：

- `run()`：旧 `BacktestEngine` 兼容路径，先生成权重矩阵，再按 `adjust_dates` 逐日推进持仓状态；这不是 callback/order/broker 风格的 `backtest.event` 事件驱动范式
- `run_vectorized()`：向量化

真正的事件驱动策略应继承 `EventDrivenStrategy`，并通过 `run_event_backtest(...)` 接入 `EventDrivenBacktestEngine`。

第五阶段后，策略层明确支持三种范式：

- `VectorizedStrategy`：矩阵/向量化研究协议，围绕 `generate_signals -> build_weights -> VectorizedBacktest`
- `EventDrivenStrategy`：callback/stateful 研究协议，围绕 `on_bar/on_event -> Order -> EventDrivenBacktestEngine`，并提供 `run_event_backtest(...)`
- `TargetWeightStrategyAdapter`：桥接协议，把既有 `weights_df` 包装成事件驱动策略，用于对照测试和迁移过渡

`base/event_driven.py` 与 `base/vectorized.py` 并行存在，避免把 callback 型策略强行塞进当前以矩阵信号/权重为核心的 `StrategyBase`。`StrategyBase.run()` 中保留的 `BacktestEngine` 调用只是 legacy 兼容入口，不再作为新事件驱动范式的主接口。`strategies/examples/` 已提供 `SimpleRelativeMomentumEventStrategy` 作为最小事件驱动策略样板，notebook 示例也已切换为从策略层导入该样板。

当前继承关系已收口为：`StrategyBase` 继承 `VectorizedStrategy`，因此旧有 `StrategyBase` 子类自动属于向量化策略范式；`crossmom_backtest.CrossMOMStrategy` 与 `dual_momentum_backtest.DualMomentumStrategy` 也已补充继承 `StrategyBase`，并保留各自基于收益率矩阵的 `run_vectorized()` 口径。

### 当前正式实现

`strategies/implementations/` 当前已包含：

- `crossmom.py`（兼容入口）
- `crossmom_backtest/`
- `tsmom_backtest/`
- `dual_momentum_backtest/`
- `jpm_trend_trade/`
- `multifactor_cta_backtest/`
- `overseas_backtest/`
- `gmat3/`

其中：

- `jpm_trend_trade/` 是较完整的趋势策略目录化实现
- `jpm_trend_trade/event_strategy.py` 已提供 `JPMEventDrivenStrategy`，作为正式策略包中的事件驱动样板；它在 `on_start()` 中预计算 JPM t-stat 信号、sigma 与 CorrCap rolling cache 等市场特征，在 `on_bar()` 中只读取当天特征并生成订单，避免每日重复滚动计算，同时通过 `EventDrivenBacktestEngine` 生成订单与持仓状态；`JPMConfig.transaction_cost_bps` 提供策略默认交易成本，`scripts/run_jpm_event.py` 已支持 `--commission-bps` 与 `--slippage-bps`
- `jpm_trend_trade/` 的向量化 baseline 使用组合层 `vol_target`；CorrCap 路径由 `CorrCapSizer` 在定仓阶段完成目标波动缩放，回测阶段只保留执行延迟和成本扣减，避免二次 vol-target 放大早期有效杠杆
- `multifactor_cta_backtest/` 是吸收全球商品 CTA 组合设计思想的中国期货多因子样板：使用 `MultiFactorTrendSignal` 七因子时序趋势、`MultiFactorCrossSectionalMomentumSignal` 四因子板块内截面动量；策略已从早期 signal-blend 改为 sleeve-blend，趋势 sleeve 独立做 inverse-vol sizing、单品种权重上限和 gross exposure 上限，截面动量 sleeve 默认做四因子行业内多空等权组合，并保留 `cross_weighting="sector_inverse_vol"` 的行业中性 sleeve 风险预算实验分支，组合层再按 `trend_weight/cross_weight` 混合持仓并统一回测、波控和扣费；截面动量在预热期不足时不参与行业排名，避免前导零值形成假多空信号；`scripts/run_multifactor_cta.py` 负责中国期货 CLI、`--start/--end` 区间控制、成本报告和图表输出，`scripts/run_multifactor_cta_global.py` 负责将国内和境外期货合并为全局品种池后的 1/2 趋势 + 1/2 截面动量组合实验
- 典型向量化策略入口已统一接入交易成本参数和报告输出：`run_crossmom.py`、`run_dual_momentum.py`、`run_jpm.py`、`run_multifactor_cta.py`、`run_overseas.py`、`run_tsmom.py` 支持 `--cost-bps`，并输出 `turnover_cost*.csv` 与带换手/成本字段的 `full_sample_summary.csv`；其中 `run_jpm.py` 已直接使用回测结果中的有效换手，保持净值扣费与报告成本口径一致
- `overseas_backtest/` 已收口为 `OverseasTrendSuite`，用于在海外期货 universe 上并行运行 JPM t-stat、TSMOM 与 Dual Momentum 三条趋势/动量研究路径；`scripts/run_overseas.py` 只负责 CLI 与报告输出
- `gmat3/` 是最复杂的复合策略子系统，已形成独立的 `data_access / main_contract / roll_return / sub_portfolio / signals / weights / index_builder / strategy` 链路
- `strategies/context.py` 已新增共享 `StrategyContext`，用于承接策略运行时依赖；`crossmom_backtest/` 作为样板率先切换到这条路径，并已补齐轻量 `run_pipeline`，`tsmom_backtest/`、`dual_momentum_backtest/`、`jpm_trend_trade/` 也已接入共享 context 读取数据与注入默认回测器；收益率矩阵加载支持 `start/end` 透传，便于脚本层做短区间 smoke test
- `crossmom_backtest/` 与 `dual_momentum_backtest/` 的策略类已补齐 `StrategyBase` 继承关系；`dual_momentum_backtest/__init__.py` 使用懒加载导出策略类，避免与 `crossmom_backtest.config` 的配置复用形成循环导入
- `tsmom_backtest/strategy.py` 已成为唯一主线 TSMOM 实现，并通过包级 `__init__.py` 导出；它统一承接 `StrategyContext`、向量化回测、可选交易费用与 `StrategyReport`，原独立 V2 实现不再保留，避免两套 TSMOM 口径并行

### components/ 与 Roll Strategy Layer

`strategies/components/roll/` 已经是当前架构中的正式组件层，不再只是草稿目录。

当前核心对象包括：

- `RollStrategyBase`
- `RollStrategyProfile`
- `RollStrategyResult`
- `SingleAssetRollStrategy`
- `BundleRollStrategy`
- `ValueComposer`
- `LookThroughResolver`

这层的定位是：

- 承接单资产与 bundle 级 roll 资产构造
- 输出 `value_series` 与 Look-Through 结果
- 为复杂复合策略提供可复用的资产级中间层

它目前仍然有明确边界：

- 优先支持 generic contract
- 不追求对所有 legacy alias contract 语义完全兼容
- 更重视 profile + 规则组件，而不是快速堆大量资产专属子类

## 当前设计结论

`cta_lab` 现阶段最重要的设计结论有三条：

1. `data/` 继续承接标准化连续合约与批量研究接口，复杂 roll 逻辑逐步回收到 `strategies/components/roll/`
2. `signals/` 与 `portfolio/` 的边界已经清晰，score、selector、sizer、constraint、scheduler 分层成立
3. 复杂复合策略要通过可穿透中间对象来表达，而不是只输出最终净值

因此，项目当前不是“等待重构的草图”，而是一套已经可运行、正在持续收口的研究框架。
