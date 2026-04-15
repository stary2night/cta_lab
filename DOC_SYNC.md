# Docs Sync Workflow

> 最后更新：2026-04-10（portfolio 文档、Signals S2 operators、analysis/signal evaluator 更新与性能优化、load_continuous 重构、`load_continuous_matrix` 批量接口、向量化连续合约 fast path、GMAT3 策略目录化重构、GMAT3 README/G1 映射文档、GMAT3 G2 数据接入、GMAT3 G3 子组合与价值矩阵实现、GMAT3 G4 权重主干落地、GMAT3 G5 指数合成主干落地、GMAT3 更大范围真实回归脚本与结果、GMAT3 G6 正式策略入口、ARCH_REVIEW_ROADMAP 架构复盘文档、DEV_PROGRESS 同步更新及 signal->portfolio 接口约定已纳入、roll 组件 BLACK helper、125/120 历史窗口口径与文档同步）

这个文件定义 `cta_lab` 项目的文档同步工作流，目标是尽量减少“代码已经改了，但 Markdown 文档还停留在旧状态”的情况。

## 适用范围

当前工作流覆盖以下文档：

- `data/README.md`
- `signals/README.md`
- `portfolio/README.md`
- `ARCH_REVIEW_ROADMAP.md`
- `DESIGN.md`
- `DEV_PROGRESS.md`
- `DOC_SYNC.md`

其中 `DOC_SYNC.md` 本身也受 docs sync 工作流约束：当覆盖范围、检查规则或文档责任边界发生变化时，需要同步更新本文件。
当前模块文档入口约定优先使用目录内 `README.md`，例如数据层使用说明统一收敛到
`data/README.md`。信号层文档入口也已统一为 `signals/README.md`，并纳入 docs sync 检查范围。
组合层文档入口统一为 `portfolio/README.md`，用于记录 signal 到 target weights 的接口约定。

并重点关注这些代码范围：

- `data/`
- `signals/`
- `portfolio/`
- `backtest/`
- `analysis/`
- `strategies/`
- `scripts/`

## 标准流程

每次完成代码修改后，按下面顺序执行：

1. 先跑相关单元测试或真实数据验证，确认代码行为已经稳定。
2. 再运行 docs sync 检查：

```bash
cd /home/ubuntu/dengl/my_projects/cta_lab
python3 scripts/docs_sync_check.py
```

3. 如果检查失败，说明至少有一类问题：
- 文档缺失
- 文档未覆盖关键新能力
- 代码更新时间晚于文档，文档可能过期

4. 根据检查结果修改对应 Markdown 文档，再重新运行检查，直到通过。

## 检查脚本做了什么

`scripts/docs_sync_check.py` 会自动做三类检查：

1. 文档是否存在。
2. 文档中是否包含必须覆盖的关键片段。
3. 文档的修改时间是否晚于它负责覆盖的代码范围。

这不是语义级别的完美审查，但足够作为日常开发里的第一道自动提醒。
当前也已将 `ARCH_REVIEW_ROADMAP.md` 纳入检查范围，用于约束平台级架构复盘文档
随着 `gmat3`、`portfolio`、`backtest`、`data` 等关键模块演进同步更新。
同时，roadmap 的检查关键词也已按当前阶段目标收敛到更明确的主线，不再强制要求
尚未看清的 engine 抽象继续占据近期规划主路径。
当前规则也已把 `load_continuous_matrix` 这类批量研究入口纳入 `data/README.md`
的必备覆盖项，避免只改代码不补研究用法说明。
当 `DESIGN.md`、`DEV_PROGRESS.md` 或 `data/README.md` 的职责边界发生变化时，
也要同步更新本文件，避免 docs sync 规则本身落后于项目现状。
当 GMAT3 这类目录化策略实现推进阶段发生变化时，也要同步更新本文件中的“最后更新”
说明，确保 `DESIGN.md` 和 `DEV_PROGRESS.md` 的新增架构状态不会被 docs sync 判为过期。

## 当前约定

### 文档存放规范

- 根目录放全局文档：例如 `DESIGN.md`、`DEV_PROGRESS.md`、`DOC_SYNC.md`
- 模块目录放模块文档入口：统一优先使用 `README.md`
- 新增模块文档时，优先采用 `<module>/README.md`，避免继续扩散多个同名但语义模糊的 `Instructions.md`
- 后续如果 `signals/`、`portfolio/`、`backtest/`、`analysis/`、`strategies/` 等模块补文档，默认沿用这套结构
- 策略实现如果演进为目录化子模块，例如 `strategies/implementations/gmat3/`，也优先在子模块目录下维护 `README.md`

### 数据层文档

`data/README.md` 需要覆盖最近 data 层的关键能力，包括但不限于：

- `DataLoader` 对 `contracts/{symbol}`、`instruments/{symbol}`、`calendars/{exchange}` 的默认回退行为
- `load_continuous(..., nav_output="price" | "normalized")`
- `load_continuous_matrix(symbols, ...)`
- 预构建 continuous 的 schedule 恢复：
  - 价格文件自带 `contract` / `active_contract` / `contract_code`
  - 或 companion 文件 `continuous/{symbol}_{adjust}_schedule`
- `ContinuousSeries.build()` 在 `OIMaxRoll` / `StabilizedRule(OIMaxRoll)` 下的向量化 fast path

### 信号层文档

`signals/README.md` 需要覆盖最近 signals 层的关键能力，包括但不限于：

- `Signal` / `CrossSectionalSignal` 两类顶层抽象
- 时序信号、截面组合器、风险信号的职责边界
- `NaN` 与预热窗口的输出约定
- 组合器对缺失信号的处理方式

### 组合层文档

`portfolio/README.md` 需要覆盖最近 portfolio 层的关键能力，包括但不限于：

- `signal_df` 代表仓位意图，而不是任意原始 score
- `TopBottomSelector` / `ThresholdSelector` 这类 score -> position bridge
- `signal_mode="direction" | "raw"` 的定仓约定
- `blend()` 仅在子组合自身有效区间内 forward fill

### 架构文档

`DESIGN.md` 负责记录项目当前的模块分层与关键设计方向。
当 `analysis/signal/` 增加新的研究入口时，也需要同步记录 future return labels、
IC / Rank IC / IR 等研究框架能力。
当策略实现从单文件演进为包结构时，例如 `strategies/implementations/gmat3/`，
也要同步更新架构文档和进展文档中的目录说明。
当 `strategies/components/roll/` 这类新的平台级组件层进入实现阶段时，
也需要同步更新架构文档，记录：

- 当前组件层定位
- 单资产 / bundle 两类资产模式
- 当前已支持的最小能力边界
- 如果 bundle 进入第二阶段，也继续记录：
  - 静态 / 等权 / 外部动态权重的支持范围
  - 哪些复杂能力仍然暂未实现（如同步展期、正式 `BLACK` 迁移）
- 如果已加入同步展期 schema，也同步记录：
  - `sync_mode`
  - `sync_frequency`
  - `sync_components`
  - 以及这些字段目前只是 schema/hook，还是已经进入正式业务逻辑
- 如果开始把真实策略（如 `GMAT3 BLACK`）映射进组件层，也同步记录：
  - 当前是结构映射还是完整业务迁移
  - 哪些规则已经进入通用 profile / adapter
  - 哪些核心计算仍留在 legacy strategy 实现里

### 进展文档

`DEV_PROGRESS.md` 负责记录当前实现状态、已完成模块和验证方式。
如果某个组件层从“设计讨论”推进到“最小可运行代码”，也应及时记入进展文档，
例如 `Roll Strategy Layer` 的单资产与 bundle 第一版能力。
如果某个组件层开始进入阶段性收口，也建议补一份局部状态文档，
例如 `strategies/components/roll/STATUS.md`，用于记录：

- 当前阶段已完成能力
- 当前明确边界
- 适合支持的研究任务
- 下一阶段优先项

## 推荐开发习惯

- 改接口：同步改 `data/README.md`
- 改 signals 层接口或语义：同步改 `signals/README.md`
- 改 portfolio 层接口或 signal -> weight 语义：同步改 `portfolio/README.md`
- 改架构边界：同步改 `DESIGN.md`
- 改项目阶段状态或新增能力里程碑：同步改 `DEV_PROGRESS.md`
- 改某个模块且需要补说明：优先在对应模块目录下维护 `README.md`
- 改完代码后，把 `python3 scripts/docs_sync_check.py` 当作常规收尾步骤

## 后续可继续增强的方向

- 接入 `pre-commit` 或 CI，在提交前自动运行 docs sync 检查
- 为更多目录建立“代码范围 -> 文档”映射
- 引入更细粒度的规则，例如检测示例代码是否仍能运行
