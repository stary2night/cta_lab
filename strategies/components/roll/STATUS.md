# Roll Component Status

> 更新时间：2026-04-10

## 当前结论

`strategies/components/roll/` 的第一阶段已经成型。

它已经不是纯设计草稿，而是一个可运行、可教学、可用于第一批研究任务的组件层。

当前更适合把它理解成：

- 面向 `generic contract` 的单资产 roll asset 生成层
- 支持基础 bundle 组合与外部动态权重承接
- 能与上层 `signals -> portfolio` 工作流衔接

它暂时还不是：

- 对 legacy continuous alias 完整兼容的实现层
- 对所有 GMAT3 细节完全一比一复刻的通用替代层
- 完整的动态 bundle / 同步展期平台

## 已完成能力

### 1. Single Asset Roll

当前已支持：

- `SingleAssetRollStrategy`
- `GMAT3SingleAssetRollStrategy`
- 显式 rule config：
  - `LifecycleRuleConfig`
  - `MarketStateRuleConfig`
  - `ExecutionRuleConfig`
  - `SelectorRuleConfig`
- registry 风格 builder
- `profile-driven` 构造方式

当前单资产结果可稳定输出：

- `contract_plan`
- `roll_schedule`
- `value_series`
- `lookthrough_book`

### 2. Bundle Roll

当前已支持：

- `BundleRollStrategy`
- `weight_mode="static"`
- `weight_mode="equal"`
- `weight_mode="external"`
- `rebalance_frequency`
- `smoothing_window`
- `weight_min / weight_max`
- bundle sync schema / hook

当前 bundle 结果可稳定输出：

- `value_series`
- `component_values`
- `component_weights`
- `lookthrough_book`

### 3. GMAT3 案例的简洁映射

当前已具备：

- `GMAT3SingleAssetRollStrategy`
- `run_gmat3_black_bundle(...)`

这里的定位是：

- 用现有通用组件尽量简洁地承接 GMAT3 的代表性案例
- 不继续追求对 legacy 实现的逐行完全复刻

### 4. Notebook 与教学入口

当前已有 notebook：

- `roll组件使用指南_market_data实战.ipynb`
- `roll资产接入signals和portfolio示例.ipynb`
- `roll组件复现原子子组合价值_gmat3对照.ipynb`

这意味着：

- 组件不只是代码可导入
- 也已经具备可演示、可实验、可研究的入口

## 当前明确边界

### 1. 只处理 generic contract

当前阶段明确只处理：

- `CU0503.SHF`
- `CU0504.SHF`

这类 generic specific contracts。

当前阶段不处理：

- `CU.SHF`
- `CU00.SHF`
- `CU01.SHF`
- `CU02.SHF`

这类 continuous / alias contract。

### 2. 不追求对 ddb legacy 结果逐行完全复刻

当前与 `ddb` 的对照，主要用于：

- 检查业务方向是否合理
- 暴露平台还缺什么抽象

而不是继续把大量精力放在：

- alias contract 命名空间兼容
- legacy 中间对象逐项完全一致

### 3. bundle 目前更偏“组合已有 component result”

当前 bundle 已经是正式对象，但更偏：

- 组合已有 single-asset result

而不是：

- 完整协调多个 component 的同步主力选择与执行

## 现在适合做什么

用当前这套组件，已经适合开展：

- 单品种 roll rule 对比研究
- 多个 roll asset 的横截面 signal 研究
- `roll asset -> signals -> portfolio` 的上层配置实验
- 静态 / 等权 / 外部动态权重 bundle 研究
- 基础 look-through 检查

## 暂时不建议做什么

当前不建议继续重投入在：

- 与 `ddb` 的 alias contract 逐日硬对齐
- 为个别案例过度设计动态 bundle 框架
- 把所有 GMAT3 特殊逻辑一次性全部迁入 roll layer

## 下一阶段优先项

如果继续推进，优先级建议是：

1. 稳定并收口 single-asset rule families
2. 继续增强 bundle 对外部动态输入的承接能力
3. 逐步把 look-through schema 做得更正式
4. 只把确认具备通用价值的 GMAT3 共性逻辑回收到 roll layer

一句话说：

下一阶段重点不是“把 roll 组件做得更复杂”，而是“把现在这套通用能力做得更稳、更清楚、更容易被上层策略复用”。
