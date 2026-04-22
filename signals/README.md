# Signals Module

> Last updated: 2026-04-20

`signals/` 是 `cta_lab` 的信号定义层，负责把价格或价格矩阵转换为可研究、可组合的信号序列。

## 模块职责

- 定义原子信号：如 `momentum/`、`reversal/`、`risk/`
- 定义信号组合器：如 `composite/`
- 为 `portfolio/`、`analysis/signal/`、`strategies/` 提供统一的信号输出接口

`signals/` 不负责：

- future return 标签构造
- IC / IR / 分组收益等预测能力评估
- 权重映射、风险预算、约束、回测

这些内容应分别放在 `analysis/signal/`、`portfolio/`、`backtest/`。

一个更完整的研究链路应理解为：

`prices -> signal -> operators / score processing -> selector -> portfolio`

也就是说，`signals/` 负责输出原始预测信息；`portfolio/` 负责把已经处理好的仓位意图
转换为目标权重。截面 score 不能直接跳过 bridge 层进入定仓。

## 顶层抽象

### `Signal`

时序信号基类，输入单一品种的价格序列 `pd.Series`，输出同索引的信号序列。

适用场景：

- 绝对动量
- Sharpe 动量
- TVS
- MASS 反转

### `CrossSectionalSignal`

截面信号基类，输入 `DataFrame(dates x symbols)`，输出同 shape 的信号矩阵。

适用场景：

- 横截面排名
- 多因子综合打分

## 当前目录结构

```text
signals/
├── base.py
├── momentum/
├── reversal/
├── risk/
├── carry/
├── composite/
└── operators/
```

其中 `momentum/` 当前除了传统 TSMOM、Sharpe、Absolute、Percentile 之外，还包含：

- `jpm_tstat.py`
- `nltsmom.py`
- `dual_momentum.py`

## 当前输出语义约定

- 信号输出应为浮点序列/矩阵
- 允许两类信号并存：
  - 连续强度型：如 `SharpeMomentum`、`AbsoluteMomentum`
  - 方向型：如 `TSMOM`，输出 `-1 / 0 / +1`
- 前导 `NaN` 表示预热窗口不足
- 组合器必须显式处理 `NaN`，不能把“缺失信号”隐式当成“零信号”

## Phase S1 当前关注点

- 统一 signals 层接口语义
- 修复 `RankCombiner` 的缺失值加权逻辑
- 修复 `PercentileMomentum` 在 ties / 平盘时的系统性偏置
- 为 `signals/` 补独立测试

## 当前实现补充

- `Signal.compute_matrix(price_matrix)` 已成为矩阵研究的默认入口
- `DualMomentumSignal` 已提供板块内相对动量 + 绝对动量的截面实现
- `signals/operators/` 已从规划项变成正式模块

## Signal S2 Operators

`signals/operators/` 用来处理原始 signal 或 raw score，典型用途包括：

- 对时序强度信号做 `lag`、`smooth`、`rolling_zscore`
- 对连续值做 `clip`、`winsorize`
- 对截面 score 做 `cross_sectional_rank`
- 对截面强度做 `normalize_by_abs_sum`

这些 operators 本身不决定 long / short 选品；截面 score 经过 operators 后，仍建议交给
`portfolio/selectors.py` 做 `score -> position intent` 转换。
