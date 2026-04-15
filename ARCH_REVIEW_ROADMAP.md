# cta_lab 架构复盘与通用化升级 Roadmap

这份文档记录 GMAT3 策略复现之后，对 `cta_lab` 平台架构的阶段性复盘。

结论不是 “GMAT3 没法在 `cta_lab` 中实现”，恰恰相反：
- GMAT3 已经在 `cta_lab` 中完成复现
- `value_df / weight_df / index_series` 已与旧 `ddb` 对齐

但这次复现也清楚暴露出一个问题：

**`cta_lab` 目前已经具备承载复杂策略的能力，但对复杂复合策略的通用抽象还不够。**

下一阶段的目标，不是继续快速适配更多策略，而是基于 GMAT3 的复现经验，
推进 `cta_lab` 的模块化、通用化和可穿透性升级。

这份 roadmap 特别强调一条原则：

**先做目标明确、边界清晰的任务；暂时看不清、共性还不稳定的部分先搁置。**

---

## 一、复盘结论

### 1. 当前平台的优势

- `data -> signals -> portfolio -> analysis -> strategy` 研究链路已经完整
- 普通 CTA、因子研究、notebook 验证、组合构建已经有较好基础
- GMAT3 的主链也已经跑通，说明平台底座没有问题

### 2. 当前平台的短板

GMAT3 的成功复现，更多体现为：
- `cta_lab` 能容纳一个复杂策略子系统

而不是：
- `cta_lab` 已经抽象出了支持复杂复合策略的正式通用模块

本次复现中，很多关键逻辑仍停留在：
- `strategies/implementations/gmat3/data_access.py`
- `strategies/implementations/gmat3/main_contract.py`
- `strategies/implementations/gmat3/roll_return.py`
- `strategies/implementations/gmat3/sub_portfolio.py`
- `strategies/implementations/gmat3/weights.py`
- `strategies/implementations/gmat3/index_builder.py`

这说明：
- 平台底层已经复用了一部分
- 但 Roll Strategy、资产级价值序列、组合调度、穿透输出等能力，
  还没有成为 `cta_lab` 的正式通用能力

---

## 二、GMAT3 带来的核心架构认识

### 1. GMAT3 本质是复合策略

GMAT3 可以拆成两层：

1. 底层资产层：
   - 各类 `Roll Strategy`
   - 每个底层策略输出一个可投资的资产价值序列 `V_c(t)`

2. 上层配置层：
   - 对这些底层资产做 `Cross Asset Allocation`
   - 通过 `Cross-Momentum / Cross-Reversal / Risk Control`
     形成组合构成与权重

因此，GMAT3 的本质是：

**Roll Strategy + Cross Asset Allocation 的复合策略。**

### 2. 复杂策略的关键不是“层数多”，而是“层之间可穿透”

未来平台必须避免一种假象：
- 上层只看到某个“组合资产”或“子策略净值”
- 却无法向下还原到底层真实交易资产是什么、比例是多少

如果只能看到最终策略表现，却无法穿透到底层持仓构成，
那平台就无法真正支持交易、风控和执行。

因此，下一阶段架构升级必须把“可穿透性”作为硬约束。

---

## 三、下一阶段只聚焦 3 条主线

这次复盘之后，当前最明确、最值得优先推进的方向只有 3 条：

### 主线 1：Roll Strategy 组件化

`data.load_continuous()` 远远不够表达复杂展期策略。

未来需要一个专门支持多样化展期策略的组件层，负责：
- 主力映射
- 展期收益
- 替代标的切换
- 资产级价值序列构造

### 主线 2：signals / portfolio 通用化增强

GMAT3 暴露出两个共性需求：
- `signals` 层不仅要能输出 score，还要能表达状态与筛选信号
- `portfolio` 层不仅要能定仓，还要能支持 selection、调度、预算和应用时点分离

### 主线 3：Look-Through 可穿透性

未来任何融合型策略都必须支持：
- 从上层策略穿透到底层子策略
- 再穿透到底层真实资产
- 最终还原出可交易资产及其比例

如果做不到这一点，平台就只能做研究表现展示，无法真正支撑交易。

---

## 四、明确的设计原则

### 原则 1：优先抽象清晰的中间对象

优先回收 GMAT3 中已经稳定的共性能力：
- `main_df`
- `roll_ret`
- `value_series`
- `selection / state`
- `calc_dates / adjust_dates`
- `weight_df`
- `lookthrough_book`

### 原则 2：优先“基类 + 配置 + 可插拔组件”，而不是大量继承子类

特别是在 `Roll Strategy` 这一层：
- 不建议为每种资产设计一个单独 class
- 更合理的是：
  - `RollStrategyBase`
  - `RollStrategyProfile`
  - 少量可插拔组件

例如：
- domestic equity roll profile
- domestic bond roll profile
- overseas futures roll profile
- black composite profile

这些更可能是不同 profile / config 实例，而不是一长串继承子类。

### 原则 3：平台输出必须同时包含“表现结果”和“穿透结果”

未来策略运行结果不能只返回：
- `nav`
- `index_series`

还必须能返回：
- `strategy_weights`
- `sub_strategy_weights`
- `underlying_asset_weights`
- `contract_level_positions`
- `lookthrough_records`

### 原则 4：暂时不对不清晰问题过度设计

例如：
- `IndexEngine`
- `Multi-Currency Engine`

这些方向未来很可能有价值，但当前还不够清晰。
现阶段不应把它们放在主线路径里，也不应为了“架构完整”过早拔高。

目前更合理的做法是：
- 先把它们视为 `backtest` 的后续增强项
- 等前面 3 条主线稳定之后再决定是否独立抽象

---

## 五、建议中的目标分层

当前阶段建议只围绕下面 4 层来重构平台视角：

### 1. Roll Strategy Layer

代表底层资产级展期策略。

输出：
- `main_df`
- `roll_ret`
- `value_series`
- `lookthrough_book`

### 2. Cross-Asset Signal Layer

以上层资产池为输入，做截面研究和信号判断。

输出：
- `score`
- `state`
- `selection_signal`

### 3. Portfolio Construction Layer

负责把 signal/state 转成真正的组合结构。

应包含：
- selection
- risk budget
- calc/apply separation
- staggered schedule
- blending

输出：
- `target_weights`
- `applied_weights`
- `rebalance_plan`

### 4. Composite Strategy Assembly Layer

负责把前面几层拼装成完整策略。

例如：
- GMAT3
- 未来的多层 CTA 复合配置策略

输出：
- 完整策略运行结果
- 表现视图
- 可穿透视图

---

## 六、关键模块化升级方向

### A. Roll Strategy 抽象

建议新增正式抽象：
- `RollStrategyBase`
- `RollStrategyProfile`
- `MainContractSelector`
- `RollReturnCalculator`
- `ValueComposer`
- `LookThroughResolver`

这里要特别注意：
- `DomesticEquityRollStrategy`、`DomesticBondRollStrategy` 等不一定是 class
- 更可能只是 `RollStrategyBase` 的不同 profile / config 实例

### B. 资产级价值序列对象

建议把“子组合价值序列”提升为正式对象，而不是只存在于某个策略内部。

未来可以有类似：
- `AssetValueSeries`
- `CompositeAssetValueSeries`

用于表达：
- 单资产 roll 策略结果
- 复合资产子组合结果

### C. signals 层增强

建议补强以下能力：
- cross momentum score
- cross reversal state
- risk state / risk penalty signal
- 多状态联合输出

重点不是再堆单一 signal 类，而是增强“截面状态表达”能力。

### D. portfolio 层增强

建议正式支持：
- selection
- risk budget
- `calc_dates`
- `adjust_dates`
- `computed vs applied`
- 多子组合错峰融合

将当前 GMAT3 中已经验证过的组合调度能力回收到平台层。

### E. Look-Through 抽象

建议正式引入：
- `AssetExposure`
- `ExposureTree`
- `LookThroughBook`

并要求所有复合策略支持：
- 向下穿透到底层真实资产
- 看到底层合约、比例和调仓变化
- 输出真实可交易目标持仓

---

## 七、关于 Roll Strategy 组件应该放哪

这是当前最值得先定下来的一个问题。

### 当前建议

**先不要直接把 Roll Strategy 提升为 `cta_lab` 一级组件。**

更合理的第一步是：
- 先放在 `strategies` 体系下
- 但不再停留在某个具体策略目录里

建议方向：
- `strategies/asset_strategies/roll/`
- 或 `strategies/components/roll/`

理由：
- 它已经不是某个策略专属逻辑
- 但它仍然属于“策略构造能力”，不是最底层基础设施
- 先放在 `strategies` 体系下，演化成本最低，也最符合当前理解

### 后续升级条件

只有当下面条件满足时，再考虑把它提升为一级组件：
- 至少 2 到 3 类策略直接复用
- 不再只是 CTA 内部的一种策略部件
- 已经形成稳定的数据对象和接口约定

---

## 八、建议的实施顺序

### Phase P1：平台能力边界梳理

目标：
- 明确哪些 GMAT3 模块继续留在策略专属层
- 哪些能力应回收进平台

产出：
- 平台通用能力边界表
- 策略专属能力边界表

### Phase P2：Roll Strategy Layer 抽象

目标：
- 建立 `RollStrategyBase + Profile + Components` 体系

优先内容：
- `main_contract`
- `roll_return`
- `value_series`
- 替代标的切换

### Phase P3：signals / portfolio 通用化增强

目标：
- 将 cross-asset signal + selection + schedule + budget 的共性能力平台化

优先内容：
- selection
- risk budget
- stateful signals
- calc/apply separation
- staggered schedule

### Phase P4：Look-Through 能力建设

目标：
- 让复合策略具备真实交易可落地的穿透输出

优先内容：
- 底层资产映射
- 合约级暴露
- 分层权重展开
- 可交易目标持仓

### Phase P5：Strategy Layer 重构

目标：
- 正式支持“底层资产策略 + 上层配置策略”的复合装配方式

重点：
- `Roll Strategy + Cross Asset Allocation`
- 统一运行结果对象
- 统一穿透结果对象

### Phase P6：后续观察项

以下主题暂时保留为后续观察项，不放入当前主线路径：
- `IndexEngine` 是否独立抽象
- `Multi-Currency Engine` 是否独立抽象
- 更通用的指数编制引擎是否需要单独成层

等前面主线完成、更多策略接入后，再决定是否正式提升。

---

## 九、当前阶段建议

短期内不建议继续快速接更多复杂策略。

更合理的顺序是：

1. 基于 GMAT3 的复现经验，完成平台架构 review
2. 先做 `Roll Strategy` 抽象
3. 再做 `signals / portfolio` 通用化增强
4. 同时把 `Look-Through` 设计当作硬约束纳入
5. 最后再回头决定不清晰的 engine 问题是否要升级成主线模块

这样后续再接其他复杂 CTA / 多层配置策略时，才不会再次大量重写策略专属模块。

---

## 十、阶段性判断

到目前为止，可以明确认为：

- GMAT3 复现工作已经完成
- 复现结果有效
- 平台当前的主要问题已经被真实策略暴露出来
- 下一阶段的方向已经足够清晰，不需要继续靠“再接一个复杂策略”来试错

因此，接下来的工作重点应该从“策略复现”切换到“平台升级”。

这份 roadmap 即作为下一阶段讨论和设计的基础版本。

补充说明：

- `Phase P2` 已开始进入最小实现阶段
- `strategies/components/roll/` 里已经形成：
  - `SingleAssetRollStrategy`
  - `BundleRollStrategy`
  - 显式 `rule config`
  - `profiles/` 样例
- 当前仍然坚持先完成目标明确的事项：
  - generic-contract-only 的单资产 roll
  - bundle roll 的第一版 schema 与结果对象
  - look-through 的统一输出
