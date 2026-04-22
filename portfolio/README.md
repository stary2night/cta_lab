# Portfolio Layer

> Last updated: 2026-04-20

`portfolio/` 负责把已经可定仓的信号矩阵转换为目标权重矩阵。它是 `signals/` 和
`backtest/` 之间的无状态组合构建层。

## 职责边界

- 输入：信号矩阵、波动率矩阵、调仓时间表、组合约束
- 输出：目标权重 `DataFrame(dates x symbols)`
- 不负责：未来收益评估、成交撮合、滑点费用、真实持仓状态

## 当前子模块

- `sizing/`
  - `EqualRiskSizer`
  - `RiskBudgetSizer`
  - `CorrCapSizer`
- `constraints/`
  - `WeightCap`
  - `WAF`
- `scheduler/`
  - `MonthlyScheduler`
  - `StaggeredScheduler`
- `selectors.py`
  - `TopBottomSelector`
  - `ThresholdSelector`
- `blender.py`
- `fx_handler.py`

其中 `blender.py` 当前同时提供：

- 低层 `blend()`：融合多个子组合权重矩阵
- 高层 `StrategyBlender`：管理多个策略并统一执行向量化回测

## 信号输入约定

`Sizer.compute(signal_df, vol_df)` 里的 `signal_df` 不是“任意 score”，而是已经可以用于
定仓的仓位意图矩阵：

- 方向型：`{-1, 0, +1}`
- 强度型：保留多空方向和强弱的浮点值

纯截面 rank 分数，例如 `RankCombiner` 输出的 `[0, 1]` score，不能直接交给
`RiskBudgetSizer` 或 `EqualRiskSizer`。这类 score 应先通过 selector 映射为
long / short / flat 仓位意图。

## Selectors

`selectors.py` 提供 signals -> portfolio 的桥接层：

- `TopBottomSelector(top_n=..., bottom_n=...)`
  - 取每日截面 top / bottom 品种，输出 `{-1, 0, +1}`
- `ThresholdSelector(long_threshold=..., short_threshold=...)`
  - 按 score 阈值映射为 long / short / flat

这一步解决的是“截面排序分数”和“可定仓仓位意图”之间的语义差距。

如果上游是连续强度型 score，例如 zscore、标准化动量、残差信号，可以先在
`signals/operators/` 中完成 `clip`、`winsorize`、`cross_sectional_rank`、
`normalize_by_abs_sum` 等处理，再进入 selector 或 raw-mode sizer。

## Sizer 的 signal_mode

`EqualRiskSizer` 和 `RiskBudgetSizer` 现在都支持：

- `signal_mode="direction"`
  - 只使用信号方向，等价于 `sign(signal_df)`
- `signal_mode="raw"`
  - 保留信号强度，调用方自行保证输入已做标准化/裁剪

默认仍然是 `direction`，适合传统 CTA 方向型信号；做研究或更细的权重映射时，可以切到
`raw`。

## Blender 约定

`blend()` 会把子组合权重对齐到日期并集，并在每个子组合自己的有效日期区间内 `ffill`。
这样可以保留“再平衡之间持有原权重”的行为，同时避免子组合在最后一个有效日期之后被无限期
向前延续。实现上会把有效区间外的值显式清零，而不是依赖无限 forward fill。

## 当前实现补充

- `CorrCapSizer` 主要承接 JPM 趋势策略相关性截断定仓
- `StaggeredScheduler` 用于多子组合错峰调仓
- `blend()` 和 `StrategyBlender` 让“单策略研究 -> 多策略组合”成为正式路径
