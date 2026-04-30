# Signals Module

> Last updated: 2026-04-28

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
├── network/
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
- `multifactor_trend.py`
- `multifactor_crossmom.py`

`network/` 当前提供 `NetworkMomentumSignal` 及其配套特征构造、图学习与网络传播逻辑，
用于复现和扩展 *Network Momentum across Asset Classes* 一类跨资产网络动量研究。

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
- `MultiFactorTrendSignal` 已提供商品 CTA 风格七因子趋势信号，覆盖长周期累计收益、价格突破、收益均值突破和长短均值残差等趋势维度
- `MultiFactorCrossSectionalMomentumSignal` 已提供商品 CTA 风格四因子截面动量信号，在行业内按数量选择 top/bottom 分位并合成方向强度；预热期不足的因子值保持缺失，不参与截面排名，避免把前导零值误当成有效信号；同时提供四因子多空等权 portfolio weights，用于策略层按 sleeve 方式组合，并保留行业中性 sleeve inverse-vol 加权的实验分支
- `SkewReversalSignal` 已作为 `signals/reversal/` 的正式导出对象，与 `MASS260Reversal` 并列，承接基于偏度与持仓量变化的中国期货反转研究
- `NetworkMomentumSignal` 已形成正式信号实现：支持 `net_only` 与 `combo` 两种模式，内部包含网络图学习、网络特征传播与 walk-forward Ridge 训练；训练标签保留真实缺失，不再把缺失未来收益隐式当成零值，训练窗口内部也不再把末端网络结构回填给整段历史样本
- `signals/operators/` 已从规划项变成正式模块

## Signal S2 Operators

`signals/operators/` 用来处理原始 signal 或 raw score，典型用途包括：

- 对时序强度信号做 `lag`、`smooth`、`rolling_zscore`
- 对连续值做 `clip`、`winsorize`
- 对截面 score 做 `cross_sectional_rank`
- 对截面强度做 `normalize_by_abs_sum`

这些 operators 本身不决定 long / short 选品；截面 score 经过 operators 后，仍建议交给
`portfolio/selectors.py` 做 `score -> position intent` 转换。
