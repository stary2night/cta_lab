# cta_lab 开发进展记录

> 最后更新：2026-04-24

`cta_lab` 当前已经从“合并 `cta` 与 `ddb` 的搭框架阶段”进入“平台收口与策略扩展并行阶段”。完整架构设计见 [DESIGN.md](DESIGN.md)。

## 当前状态

当前可以把项目理解成：

- 核心研究框架已成型
- 多条真实策略链路已经落地
- 复杂复合策略已经有可运行范例
- 文档、脚本、测试开始围绕现状做同步收口

### 核心层状态

| 层 | 当前情况 | 备注 |
| -- | -- | -- |
| 数据层 | 已稳定可用 | `DataLoader`、schema、连续合约、批量矩阵接口已成型 |
| 信号层 | 已稳定可用 | 时序/截面信号、组合器、operators 已落地 |
| 组合层 | 已稳定可用 | selector、sizer、constraint、scheduler、blender 均已落地 |
| 回测层 | 已稳定可用，事件驱动路径已有最小闭环、稀疏调仓、成本和滑点能力 | 向量化路径成熟；`backtest/event/` 已补齐 market portal、engine、broker、recorder、target-weight adapter |
| 分析层 | 已稳定可用 | signal research 与 Decomposer 报告体系已落地 |
| 策略层 | 已形成多条实现链，并接入事件驱动范式 | 单层趋势策略、复合策略、vectorized/event-driven 协议并存 |
| 组件层 | 已进入正式实现 | `strategies/components/roll/` 已可运行 |

## 当前已落地的重点能力

### 1. 数据层

- `DataLoader` 统一承接行情、合约、日历、instrument
- 已支持 `KlineSchema.tushare()` 与 `KlineSchema.overseas()`
- 已提供 `load_continuous()`、`load_continuous_matrix()`、`load_returns_matrix()`
- 已支持 `contracts/{symbol}`、`instruments/{symbol}`、`calendars/{exchange}` 的默认回退
- 连续合约路径已包含向量化 fast path 与预构建 schedule 恢复能力

### 2. 信号层

- 已形成 `Signal` / `CrossSectionalSignal` 顶层抽象
- 动量类已覆盖 TSMOM、Sharpe、Absolute、Percentile、NLTSMOM、JPM t-stat、Dual Momentum、MultiFactorTrend、MultiFactorCrossSectionalMomentum
- 风险与反转信号已包含 TVS、MASS260
- `signals/operators/` 已成为正式研究辅助层，而不是临时工具函数

### 3. 组合层与回测层

- `TopBottomSelector` / `ThresholdSelector` 已明确承担 score -> 仓位意图桥接
- `EqualRiskSizer` / `RiskBudgetSizer` 已支持 `signal_mode`
- `CorrCapSizer` 已承接 JPM 趋势策略相关性截断定仓
- `blend()` 与 `StrategyBlender` 已支持多策略组合研究
- `BacktestResult` 已统一包含 `nav / returns / positions_df / turnover_series`
- `WalkForwardEngine` 已补上样本外验证能力
- `backtest/event/` 已补充事件驱动第一阶段领域对象：`Event`、`MarketSnapshot`、`Order`、`Fill`、`PortfolioState`、`StrategyState`、`SimulationContext`；这些对象保持轻量运行时依赖，先服务研究回测状态表达
- `backtest/event/` 第二阶段已形成最小运行闭环：`MarketDataPortal` 将 DataFrame 行情转为 snapshot，`EventDrivenBacktestEngine` 调用策略 hook，`SimulatedBroker` 立即成交 `MARKET` / `TARGET_WEIGHT` 订单，`EventRecorder` 输出标准 `BacktestResult`
- `backtest/event/` 第三阶段已加入 `TargetWeightStrategyAdapter`，可把现有 `weights_df` 作为事件驱动策略运行；新增测试已验证 `lag=0`、`fee=0`、每日权重口径下可与 `VectorizedBacktest` 对齐
- `backtest/event/` 第四阶段已支持 adapter 级 `execution_lag` 与稀疏调仓日期；`EventRecorder` 的 turnover 已改为基于真实成交 notional 记录，避免把非调仓日权重漂移误记为换手；新增测试已验证稀疏调仓漂移、延迟执行和 close-to-close 调仓对齐 `VectorizedBacktest(lag=1)`
- 回测层已新增统一轻量成本和滑点模型：`backtest/costs.py` 提供 `ZeroCostModel`、`ProportionalCostModel`、`DailyAccrualCostModel`、`CompositeCostModel`，`backtest/slippage.py` 提供 `NoSlippage` 与 `FixedBpsSlippage`；`VectorizedBacktest` 和 `EventDrivenBacktestEngine` 均已接入 `cost_model`，事件 broker 额外支持 `slippage_model`；向量化路径启用 `vol_target` 时已改为按 vol-target 后的有效执行权重计算换手和成本，并且 vol-target 热身期不再向前回填 scale
- 典型策略运行入口已开始统一交易成本与换手报告：`run_crossmom.py`、`run_dual_momentum.py`、`run_jpm.py`、`run_multifactor_cta.py`、`run_overseas.py`、`run_tsmom.py` 支持 `--cost-bps` 并输出 `turnover_cost*.csv`，`full_sample_summary.csv` 中增加平均换手、年化换手、总成本和年化成本拖累；`JPMConfig.transaction_cost_bps` 已作为 JPM 策略默认成本来源，`run_jpm.py` 与 `run_jpm_event.py` 在未显式传入成本参数时回退到该配置，且 `run_jpm.py` 的成本报告已改用回测结果中的有效换手
- **趋势+截面动量融合研究（2026-04-24 收口）**：新增 `scripts/run_jpm_crossmom_blend.py`，系统对比"时序 sleeve 选 JPM t-stat 还是 MF 多因子趋势"的融合策略。统一设置：target_vol=10%、成本=5bps、lag=1、中国期货全品种宇宙。JPM sleeve 在混合前先在 sleeve 层做单品种 clip(±10%) 和 gross 上限(1.5x)，MF CrossMOM sleeve 沿用 `MultiFactorCTAStrategy.build_cross_positions()`，两者按 ts_weight/cs_weight 加权平均后统一进入 `VectorizedBacktest` 做 vol-targeting 和扣费。全样本（2005-2026）回测结论：

  | 组合 | 年化收益 | Vol | Sharpe | 最大回撤 |
  |------|---------|-----|--------|---------|
  | **JPM+CS (1:2)** | **16.38%** | 10.42% | **1.572** | -21.66% |
  | JPM+CS (1:1) | 15.99% | 10.43% | 1.533 | -20.56% |
  | JPM+CS (2:1) | 15.43% | 10.46% | 1.475 | -19.13% |
  | MFTrend+CS (1:2) | 15.73% | 10.45% | 1.505 | -18.96% |
  | MFTrend+CS (1:1) | 15.32% | 10.46% | 1.465 | -17.14% |
  | MFTrend+CS (2:1) | 14.89% | 10.47% | 1.422 | -17.47% |

  JPM 趋势信号在所有混合比例下均优于 MF 多因子趋势，最佳组合为 **JPM+CS 1:2（SR=1.572）**，相比 MF CTA 基准（SR≈1.47）提升约 0.1 Sharpe。代价是 JPM sleeve 集中度更高，最大回撤扩大约 3pct（约 -19% → -22%）。JPM 优势集中在 2012、2017、2019、2020 等趋势性强的年份，这些年份 MF 多因子趋势信号出现负 Sharpe，而 JPM t-stat 信号仍有效捕获方向。本轮趋势+截面动量融合研究已在此收口，后续若需进一步提升需在信号质量、动态 sleeve 权重或风险约束细节层面深化。
- 第五阶段已把事件驱动范式接入策略层：`strategies/base/vectorized.py` 新增 `VectorizedStrategy`，`EventDrivenStrategy` 新增 `run_event_backtest(...)`，`strategies/examples/` 新增 `SimpleRelativeMomentumEventStrategy`，事件驱动 notebook 已改为从策略层导入样板策略
- 策略基类继承关系已进一步收口：`StrategyBase` 继承 `VectorizedStrategy`；`crossmom_backtest.CrossMOMStrategy` 与 `dual_momentum_backtest.DualMomentumStrategy` 已补充继承 `StrategyBase`，并覆盖收益率矩阵口径的 `run_vectorized()`
- `StrategyBase.run()` 保留为旧 `BacktestEngine` 权重矩阵状态推进兼容入口；新的 callback/order/broker 事件驱动范式统一走 `EventDrivenStrategy.run_event_backtest(...)`
- `strategies/base/trend.py` 与 `TrendFollowingStrategy` 已删除；趋势策略正式实现以 `tsmom_backtest/`、`jpm_trend_trade/` 等 implementation 包为准，`base/` 保持为策略范式基类与兼容入口层
- `jpm_trend_trade/` 已新增 `JPMEventDrivenStrategy`，作为正式策略包事件驱动样板；当前支持 baseline 与 CorrCap 两种路径，并已把 t-stat、sigma 与 CorrCap rolling cache 等市场特征前移到 `on_start()` 预计算，`on_bar()` 只负责读取当天特征、处理状态并发单；baseline 可执行 ex-ante vol-targeting，相关测试覆盖合成数据下的 baseline/CorrCap 事件回测与预计算次数；`scripts/run_jpm_event.py` 已补齐对应 CLI 入口，并支持 `--commission-bps`、`--slippage-bps`

### 4. 分析层

- `analysis/signal/` 已形成轻量因子研究入口
- `AnalysisContext` + `Decomposer` + `StrategyReport` 已成为当前正式报告架构
- 常用图表与输出流程已经沉淀在 `analysis/report/`

### 5. 策略实现

当前 `strategies/implementations/` 已有以下正式实现或目录化子系统：

- `crossmom.py`（兼容入口）
- `crossmom_backtest/`
- `tsmom_backtest/`
- `dual_momentum_backtest/`
- `jpm_trend_trade/`
- `multifactor_cta_backtest/`
- `overseas_backtest/`
- `gmat3/`

其中值得单独记录的有：

- `jpm_trend_trade/`：JPM t-stat + CorrCap 路径完整落地，并新增事件驱动版本 `JPMEventDrivenStrategy`；事件驱动版可通过 `scripts/run_jpm_event.py` 运行 baseline、CorrCap 或两者对比，脚本支持 `--start/--end` 做短区间事件回测 smoke test，也支持交易成本与固定 bps 滑点参数；向量化与事件驱动入口均已接入 `JPMConfig.transaction_cost_bps` 默认成本配置；向量化 CorrCap 路径已关闭回测阶段二次 vol-targeting，避免与 `CorrCapSizer` 的目标波动缩放重复
- `multifactor_cta_backtest/`：新增中国期货多因子 CTA 第一版，组合 `MultiFactorTrendSignal` 七因子趋势和 `MultiFactorCrossSectionalMomentumSignal` 四因子板块内截面动量；策略已升级为 sleeve-blend，趋势 sleeve 独立做 inverse-vol sizing 与单品种/gross 上限，截面动量 sleeve 默认做四因子行业内多空等权组合，并保留 `sector_inverse_vol` 行业中性 sleeve 风险预算实验分支，组合层按 `trend_weight/cross_weight` 混合持仓后统一回测、波控和扣费；截面动量已修正预热期行为，预热不足时不参与行业排名，避免前导零值形成假多空信号；`scripts/run_multifactor_cta.py` 是中国期货运行入口，`scripts/run_multifactor_cta_global.py` 是国内 + 境外期货全局品种池运行入口，均支持 `--start/--end` 区间控制
- `overseas_backtest/`：海外趋势对比研究已收口为 `OverseasTrendSuite`，策略包承接 JPM t-stat、TSMOM、Dual Momentum 三条研究路径的信号、定仓与回测，`scripts/run_overseas.py` 保留为 CLI 与输出入口
- `gmat3/`：已形成从数据接入到指数合成的完整目录化实现
- `crossmom_backtest/`：已完成第一版样板试验，并把包内 `context` 上提为框架级 `strategies/context.py`；当前通过共享 `StrategyContext` 集成核心依赖，策略包已补齐轻量 `run_pipeline`，`scripts/run_crossmom.py` 只负责 CLI 与输出。`tsmom_backtest/`、`dual_momentum_backtest/`、`jpm_trend_trade/` 也已同步接入共享 context 路径，对应入口脚本现已统一使用同一套 `StrategyContext`
- `dual_momentum_backtest/`：策略类已补齐 `StrategyBase` 继承；包级 `__init__.py` 使用懒加载导出策略类，避免 CrossMOM 与 DualMomentum 配置复用导致循环导入
- `tsmom_backtest/`：已将原 `strategy_v2.py` 中的费用、`run_vectorized` 与 `StrategyReport` 能力合并回主线 `strategy.py`，并通过包级导出暴露统一 `TSMOMStrategy`；`scripts/run_tsmom.py` 已成为唯一真实运行入口，`scripts/run_tsmom_v2.py` 仅作为兼容薄封装保留，差异只来自默认参数

策略层当前也已明确三种策略范式：

- `VectorizedStrategy`：矩阵信号、矩阵权重与 `VectorizedBacktest`
- `EventDrivenStrategy`：callback、状态、订单与 `EventDrivenBacktestEngine`
- `TargetWeightStrategyAdapter`：从既有 `weights_df` 迁移到事件驱动路径的桥接策略

### 6. Roll 组件层

`strategies/components/roll/` 当前已经完成第一阶段收口：

- `SingleAssetRollStrategy`
- `BundleRollStrategy`
- `RollStrategyProfile`
- `RollStrategyResult`
- `ValueComposer`
- `LookThroughResolver`
- `GMAT3SingleAssetRollStrategy`
- `run_gmat3_black_bundle(...)`

这说明 roll 资产构造已经不再只能依赖 `data.load_continuous()`。

## 当前项目更准确的定位

项目当前不是“只完成底座、还没有策略”的状态，也不是“已经彻底产品化”的状态。

更准确地说：

- 通用研究框架已经够用
- 核心接口边界已经比较清晰
- GMAT3 级别复杂策略已经证明平台可承载
- 仍有一部分共性抽象在继续回收中，尤其集中在 roll 组件、Look-Through、复合策略组装

## 当前验证方式

当前验证主要来自四类入口：

- `tests/` 下的单元测试与模块测试
- `scripts/` 下的真实数据运行脚本
- `notebooks/` 下的研究与复现笔记本
- GMAT3 与 roll 组件的小范围真实回归

## 当前文档与工程配套

- `DOC_SYNC.md`：记录 docs sync 工作流；当前规则已覆盖 `backtest/README.md`
- `scripts/docs_sync_check.py`：检查关键 Markdown 与代码是否同步
- 模块文档入口当前以 `README.md` 为主

## 下一阶段重点

当前最值得继续推进的方向有三条：

1. 继续收口 `strategies/components/roll/`，把已落地能力稳定成更清晰的公共组件
2. 基于 `backtest/event/` 继续补滑点/拒单/部分成交、费用细节，并把 JPM 事件驱动样板继续扩展到更多正式策略包，但不把项目扩张成生产交易系统
3. 继续强化 `signals / portfolio` 与复合策略之间的中间对象约定，并把复杂策略结果做成可穿透输出

## 不再继续保留的旧表述

以下表述在当前阶段已经不够准确，因此不再作为主叙事：

- “7 层核心框架刚刚搭完”
- “GMAT3 仍主要处于概念映射阶段”
- “roll 组件仍是纯设计草稿”

现在更合适的说法是：底座已经能跑，组件层已经出现，重点转向清晰化、复用化和验证深度。
