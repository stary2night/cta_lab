# Docs Sync Workflow

> 最后更新：2026-04-23

这个文件定义 `cta_lab` 的 docs sync 约定，用来减少“代码已更新，但 Markdown 仍停留在旧状态”的情况。
本次同步也包含了对 `data/README.md`、`signals/README.md`、`portfolio/README.md`、`backtest/README.md` 的最新收口，以及 `DomesticTSMOM` 删除、`crossmom_backtest` 目录化重构、共享 `strategies/context.py` 落地、`scripts/run_crossmom.py` 样板入口，`crossmom_backtest` 补齐轻量 `run_pipeline` 后的边界更新，`tsmom_backtest` / `dual_momentum_backtest` / `jpm_trend_trade` 与其入口脚本统一接入共享 context 后的项目文档清理，`StrategyContext.load_returns_matrix()` 支持 `start/end` 透传，`tsmom_backtest` 原 V2 实现合并回主线 `strategy.py`、包级导出统一 `TSMOMStrategy`、`run_tsmom_v2.py` 收敛为兼容薄入口后的状态更新，`overseas_backtest` 收口为 `OverseasTrendSuite` 后的脚本/策略边界更新，`StrategyBase` 继承 `VectorizedStrategy`、CrossMOM/DualMomentum 策略类补齐 `StrategyBase` 继承关系、`StrategyBase.run()` 收窄为旧 BacktestEngine 兼容路径、`TrendFollowingStrategy` 从 base 层删除、`JPMEventDrivenStrategy` 作为正式策略包事件驱动样板落地与 `scripts/run_jpm_event.py` 入口补齐、JPM 事件策略市场特征前移到 `on_start()` 预计算、`JPMConfig.transaction_cost_bps` 作为 JPM 向量化与事件驱动入口的默认成本配置、向量化 `vol_target` 路径按有效执行权重计算换手/成本且热身期不回填未来 scale、JPM 向量化 CorrCap 路径关闭回测阶段二次 vol-targeting、`MultiFactorTrendSignal` 七因子趋势信号、`MultiFactorCrossSectionalMomentumSignal` 四因子行业内截面动量信号及其预热期排名修正、`multifactor_cta_backtest` 与 `scripts/run_multifactor_cta.py` 落地中国期货多因子 CTA 第一版且已从 signal-blend 升级为趋势/截面 sleeve-blend，截面动量信号层同时提供四因子多空等权 portfolio weights 与 `sector_inverse_vol` 行业中性风险预算实验分支，`scripts/run_multifactor_cta.py` 支持 `--start/--end` 区间控制，`scripts/run_multifactor_cta_global.py` 支持国内 + 境外期货全局品种池的多因子 CTA 回测，`scripts/run_jpm_event.py` 支持 `--start/--end` 短区间 smoke test 与 `--commission-bps` / `--slippage-bps` 成本参数、典型向量化策略入口支持 `--cost-bps` 并输出换手/成本报告后的说明，以及 `backtest/event/` 与 `EventDrivenStrategy` 轻量事件驱动骨架、market portal、engine、broker、recorder 最小闭环、统一 `CostModel`、固定 bps 滑点、`TargetWeightStrategyAdapter` 正确性桥接、第四阶段稀疏调仓/执行延迟/真实交易换手口径和第五阶段策略层范式接入落地后的架构说明。

## 当前覆盖范围

当前 docs sync 工作流覆盖以下 Markdown 文档：

- `data/README.md`
- `signals/README.md`
- `portfolio/README.md`
- `backtest/README.md`
- `ARCH_REVIEW_ROADMAP.md`
- `DESIGN.md`
- `DEV_PROGRESS.md`
- `DOC_SYNC.md`

其中 `DOC_SYNC.md` 本身也属于被维护对象。只要覆盖范围、规则或文档职责变化，就需要同步更新它；当前规则已把 `backtest/README.md` 纳入自动检查。

## 当前对应的代码范围

检查脚本重点关注以下目录：

- `data/`
- `signals/`
- `portfolio/`
- `backtest/`
- `analysis/`
- `strategies/`
- `scripts/`

## 标准流程

每次完成代码修改后，建议按下面顺序收尾：

1. 先跑相关测试、脚本或最小真实数据验证。
2. 再运行 docs sync 检查：

```bash
cd /home/ubuntu/dengl/my_projects/cta_lab
python3 scripts/docs_sync_check.py
```

3. 若检查失败，先判断是文档内容缺失，还是代码时间戳已经晚于文档。
4. 更新对应 Markdown 后重新运行，直到通过。

## `scripts/docs_sync_check.py` 当前检查什么

脚本会做三类检查：

1. 文档是否存在
2. 文档中是否包含必须覆盖的关键片段
3. 文档修改时间是否晚于其负责覆盖的代码

这不是语义级的完美审查，但足够作为日常开发里的第一道提醒。

## 当前文档职责

### `data/README.md`

用于记录 data 层的实际入口与使用方式。当前至少应覆盖：

- `contracts/{symbol}`
- `instruments/{symbol}`
- `calendars/{exchange}`
- `load_continuous(..., nav_output=...)`
- `load_continuous_matrix(...)`
- `continuous/{symbol}_{adjust}_schedule`

### `signals/README.md`

用于记录信号层边界与语义。当前至少应覆盖：

- `Signal`
- `CrossSectionalSignal`
- `momentum/`
- `composite/`
- `operators/`

### `portfolio/README.md`

用于记录 signal 到权重的接口语义。当前至少应覆盖：

- `TopBottomSelector`
- `ThresholdSelector`
- `signal_mode`
- `blend()`

### `backtest/README.md`

用于记录回测层两种范式、成本模型和事件策略写法。当前至少应覆盖：

- `VectorizedBacktest`
- `EventDrivenBacktestEngine`
- `CostModel`
- `ProportionalCostModel`
- `FixedBpsSlippage`
- `on_start`
- `on_bar`

### `DESIGN.md`

负责记录项目当前真实架构，不记录过多历史阶段口号。需要持续反映：

- `data/`
- `signals/`
- `portfolio/`
- `backtest/`
- `analysis/`
- `strategies/`

### `DEV_PROGRESS.md`

负责记录当前项目状态、已落地能力和下一阶段重点。应能回答三个问题：

- 当前状态是什么
- 哪些能力已经不是草稿
- 目前还在继续收口的重点在哪里

### `ARCH_REVIEW_ROADMAP.md`

负责记录平台级复盘与下一阶段主线，当前应持续覆盖：

- `RollStrategyBase`
- `Look-Through`
- `Cross Asset Allocation`
- `signals / portfolio`
- 轻量事件驱动回测

## 当前维护原则

- 模块入口文档统一优先使用目录内 `README.md`
- 根目录文档负责项目级状态，不重复铺开模块实现细节
- 如果某个局部组件需要记录阶段状态，可以使用专门状态文档，例如 `strategies/components/roll/STATUS.md`
- 这类局部状态文档目前不在自动 docs sync 范围内，但内容应与根文档叙述保持一致

## 推荐开发习惯

- 改 data 接口，同步看 `data/README.md`
- 改 signals 语义，同步看 `signals/README.md`
- 改 portfolio 接口或 signal -> portfolio 约定，同步看 `portfolio/README.md`
- 改项目层边界，同步看 `DESIGN.md`
- 改项目阶段判断，同步看 `DEV_PROGRESS.md`
- 改平台主线判断，同步看 `ARCH_REVIEW_ROADMAP.md`
- 改完后把 `python3 scripts/docs_sync_check.py` 当成常规收尾步骤

## 当前结论

docs sync 的目标不是把 Markdown 写得越来越多，而是让关键 Markdown 始终能准确描述当前代码。

文档应该少而准，能直接回答“现在项目是什么样”“这个模块该怎么用”“最近改动是否已经同步到说明里”。
