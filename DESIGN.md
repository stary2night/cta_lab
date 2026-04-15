# cta_lab 系统设计文档

> 本文档记录 cta_lab 项目的架构设计思路，持续更新中。

---

## 项目背景

将 `cta`（国内期货趋势策略研究）和 `ddb`（GMAT3 全球多资产配置策略）两个项目合并，
升级为一个通用的 CTA 策略研究框架，支持更丰富的策略类型和更完善的研究工具链。

---

## 整体分层架构

```
cta_lab/
├── data/          # 数据层
├── signals/       # 信号层
├── portfolio/     # 组合构建层
├── backtest/      # 回测层
├── analysis/      # 分析层
├── strategies/    # 策略层
├── utils/         # 通用工具
├── scripts/       # 可执行脚本
├── notebooks/     # 研究笔记本
└── tests/         # 测试
```

---

## 数据层（data/）设计思路

### 总体结构

```
data/
├── model/         # 领域对象模型
├── sources/       # 数据源适配
└── loader.py      # 统一数据服务
```

---

### 一、领域对象模型（data/model/）

#### 设计思路

数据层的核心是构建一套面向金融量化研究领域的**领域对象模型（Domain Model）**，
类似 ORM 之于数据库，但面向的是期货研究领域的核心业务概念。

**组织原则：按实体而非功能组织**

传统做法是按功能职责切分模块（contracts 工具集、universe 工具集），
但这会导致实体边界模糊——完成一件业务事情需要跨多个模块拼凑。

更好的做法是：**每个文件对应一个清晰的业务实体**，文件名回答的问题是
"这是什么"，而不是"这能做什么操作"。

#### 模型文件规划

```
data/model/
├── instrument.py    # 期货品种：静态定义 + 注册管理
├── contract.py      # 合约实例：单个合约的属性与生命周期
├── bar.py           # 行情数据：单根K线(Bar) + K线序列(BarSeries)
├── calendar.py      # 交易日历：单交易所 + 多交易所合并日历
├── roll.py          # 换仓规则：抽象接口 + 标准实现 → 产出合约时间表
└── continuous.py    # 连续合约序列：最终供策略层消费的标准价格对象
```

#### 核心实体关系

```
TradingCalendar / MultiExchangeCalendar
        │ 提供日期对齐与调度计算
        ▼
Instrument（InstrumentRegistry 统一管理）
        │ 1:N
        ▼
Contract（合约实例，含上市/到期等生命周期属性）
        │ 原始行情 → BarSeries
        ▼
BarSeries（通过 DataLoader 加载）
        │ + RollRule（换仓规则）
        ▼
ContractSchedule（合约切换时间表）
        │ + AdjustMethod（价格调整方式）
        ▼
ContinuousSeries（策略层直接消费的连续价格序列）
```

#### 实体方法的边界原则

模型对象应当携带"固有方法"，即：**对这个数据实体本身的自然属性描述或变换，
任何使用该实体的场景都可能需要它**。

放在 model 层的方法（示例）：
- `BarSeries.log_returns()`：价格序列的固有变换
- `BarSeries.ewm_vol()`：价格序列的统计特征
- `BarSeries.drawdown()`：价格路径的固有描述
- `TradingCalendar.offset(date, n)`：日历的基本偏移操作
- `Contract.days_to_expiry()`：合约实例的固有属性

**不**放在 model 层（属于上层模块）：
- 信号计算（TSMOM、动量排名等）→ signal 层
- 权重计算、风险预算 → portfolio 层
- 境外持仓 FX 估值 → portfolio/valuation 层

#### 连续合约构建的两层设计

连续合约构建 = **换仓决策**（when to roll）+ **价格拼接调整**（how to stitch）

这两个子问题的复杂度不同，归属不同层：

| 类型 | 内容 | 归属 |
|------|------|------|
| 简单机械规则 | OI 最大化、成交量最大化、固定日历换月 | `data/model/roll.py` |
| 研究型规则 | 基差驱动换仓时机、展期收益最优化 | `strategies/roll_research/` |

两者共用同一个 `RollRule` 抽象接口，可以在 `ContinuousSeries.build()` 中
互换传入，无需改动其他代码。价格拼接方法（NAV chain、ratio adjust、add adjust）
始终在 model 层实现。

---

### 二、数据源适配（data/sources/）

#### 设计思路

将"**如何读取数据**"与"**读取什么数据**"彻底分离。

`sources/` 只负责与存储介质对话，不包含任何业务语义：

```
data/sources/
├── base.py            # DataSource 抽象基类
├── parquet_source.py  # 本地 Parquet 文件（当前主要数据源）
├── ddb_source.py      # 本地 DolphinDB 数据库
├── sqlite_source.py   # 本地 SQLite/SQL 数据库
├── csv_source.py      # CSV 文件
└── binary_source.py   # 二进制格式（HDF5/Feather/NPY 等）
```

所有数据源实现相同的抽象接口，上层模块（DataLoader）无需感知底层存储格式，
切换数据源只需替换注入的 source 对象。

---

### 三、统一数据服务（data/loader.py）

#### 设计思路

**单一 DataLoader，包含所有数据接口方法**，是外部模块获取数据的唯一入口。

不设计多个独立 Loader（KlineLoader、ContractLoader 等），原因：
- 各类数据来自同一套数据源体系，没有独立管理的必要
- 多个 Loader 增加用户的认知负担（需要知道"该问哪个 loader"）
- 对研究框架而言，简洁的单一入口比严格的职责分离更实用

DataLoader 内部可以接受多个 DataSource 注入（不同数据类型可以来自不同数据源），
但对外始终呈现统一接口。

DataLoader 的职责是：**知道从哪里取什么数据，返回标准化的 model 层对象**。
它不做任何业务计算，只做数据加载、格式转换、缓存管理。

当前实现还支持一个实用的单源回退约定：当未显式传入 `contract_source` /
`calendar_source` 时，会从同一个根数据源下按 `contracts/{symbol}`、
`instruments/{symbol}`、`calendars/{exchange}` 路径查找对应数据。

第一版 `Instrument` 主数据也已从 `Contract` 元数据中解耦：`DataLoader` 支持独立的
`instrument_source` 和 `InstrumentSchema`。在没有专门 instrument 文件时，可以先从
国内/海外 `contract_info` 大表中抽取最小可用的品种静态信息，再逐步演进为真正独立的
instrument 主数据表。

对于 `KlineSchema.tushare()` 这类“一个品种一个混合大表”的行情布局，`load_continuous()`
内部会优先走“单次读取品种 parquet -> 一次性拆分所需合约 `BarSeries`”的路径，
避免按合约重复读取同一个品种文件。

为 notebook 研究场景，`DataLoader` 也补充了 `load_continuous_matrix(symbols, ...)`，
用于一次性返回 `dates × symbols` 的连续价格矩阵，减少研究层反复拼接单品种
`ContinuousSeries` 的开销。

在连续合约构建内部，`ContinuousSeries.build()` 对 `OIMaxRoll` 与
`StabilizedRule(OIMaxRoll)` 提供了宽表化 + 向量化 fast path：先构造
`open_interest` / `settle` pivot，再批量确定逐日主力与拼接价格；其余自定义规则仍保留
通用逐日选择逻辑。

---

## 信号层（signals/）设计思路

### 总体结构

```
signals/
├── base.py           # 抽象基类
├── momentum/         # 动量信号
├── reversal/         # 反转信号
├── risk/             # 风险信号
├── carry/            # 展期收益信号（预留目录，暂不实现）
└── composite/        # 信号组合器
```

---

### 一、两类信号的本质区别

信号层内部分为两种截然不同的信号类型，接口不同：

**时序信号（per-asset）**：每个品种独立计算，输入单一品种的价格序列，
输出该品种的信号值时间序列。TSMOM、Sharpe 动量、MASS260 反转、TVS 均属此类。

**截面信号（cross-sectional）**：需要所有品种同时参与，计算品种之间的
相对强弱关系，输出品种排名或选择矩阵。GMAT3 的多因子动量排名属此类。

两种信号的基类接口不同（见下文），是信号层最核心的架构决策。

---

### 二、信号输出类型

signals 层统一输出**浮点信号序列/矩阵**。

其中允许两类实现并存：

- **连续强度型**：保留信号强弱信息，适合后续做加权组合
- **方向型**：直接输出 `-1 / 0 / +1`，如 TSMOM 这类方向信号

需要离散化方向时，仍可通过基类的 `.to_direction()` 方法统一转换。

---

### 三、抽象基类（base.py）

```python
class Signal(ABC):
    """时序信号基类：per-asset，独立计算"""

    @abstractmethod
    def compute(self, series: pd.Series) -> pd.Series:
        # 输入：价格/净值序列（ContinuousSeries 或 BarSeries 的 settle 价）
        # 输出：信号值序列（float，通常 [-1, 1]，前导 NaN 表示数据不足）
        ...

    def to_direction(self, series: pd.Series) -> pd.Series:
        # 连续信号离散化为 {-1, 0, +1}
        return np.sign(self.compute(series))


class CrossSectionalSignal(ABC):
    """截面信号基类：需要所有品种同时参与"""

    @abstractmethod
    def compute(self, series_dict: Dict[str, pd.Series]) -> pd.DataFrame:
        # 输入：{symbol: price_series} 全历史价格字典
        # 输出：DataFrame，shape = (dates × symbols)，表示各品种各日的信号强度
        ...
```

---

### 四、各子模块定位

**momentum/**：动量信号，均为时序信号（Signal 子类）

| 文件 | 信号 | 来源 |
|------|------|------|
| `tsmom.py` | sign(rolling_sum(returns, N)) | CTA |
| `sharpe_mom.py` | N日累计收益 / N日年化波动率 | GMAT3 Mom1/Mom3 |
| `abs_mom.py` | V(t)/V(t-N) - 1 | GMAT3 Mom2 |
| `percentile_mom.py` | N日价格分位数位置 | GMAT3 Mom4 |

**reversal/**：反转信号，时序信号

- `mass_reversal.py`：MASS260，对过去 259 个 MA 窗口进行排序统计，
  衡量价格均线结构的有序程度（1=完全顺趋势，0=完全反转），来自 GMAT3。

**risk/**：风险信号，时序信号，供 portfolio 层消费

- `tvs.py`：TVS（Tail Volatility Sharpness），计算收益 Sharpe 序列与
  波动率序列的 260 日滚动相关性。TVS > 0 时风险乘数加强，否则减弱。
  **计算在信号层，使用（决定风险乘数 1.5x/1.0x/0.5x）在 portfolio 层。**

**carry/**：展期收益信号，**目录预留，暂不实现**，等 roll_research 成熟后填充。

**composite/**：信号组合器，消费上述单一信号，输出最终信号矩阵

- `linear_combiner.py`：线性加权组合（CTA 的 3 时间尺度等权平均）
- `rank_combiner.py`：截面排名综合，输出 raw score（GMAT3 的多因子选品逻辑，
  属于 CrossSectionalSignal 子类）
- `operators/`：对 raw signal / raw score 做轻量处理，如 `lag`、`smooth`、
  `rolling_zscore`、`clip`、`winsorize`、`cross_sectional_rank`

组合器的关键约定：

- 对 `NaN` 必须采用 skipna 式加权，不能把“缺失信号”隐式当成“零信号”
- 时序组合器输出单品种信号序列
- 截面组合器输出 dates × symbols 的综合得分矩阵
- raw score 如需变成持仓，应先经过 operators 和/或 selector，再进入 portfolio

`analysis/signal/` 则负责研究验证，而不是定仓：

- `labels.py`：future return / future log return 标签
- `evaluator.py`：IC、Rank IC、IR 的轻量评估
  - 对某日横截面若 signal 或 future return 无离散度，返回 `NaN`，避免数值 warning
  - IC / Rank IC 采用按日期向量化计算，避免逐日 Python 循环
  - `evaluate_signal` 支持复用预先构造好的 `future_returns`
- 其他专题分析模块：`persistence.py`、`long_short.py`、`correlation.py`

---

### 五、信号层边界（不在此层的内容）

| 内容 | 实际归属 |
|------|---------|
| 波动率估计（EWM vol、rolling vol）| `data/model/bar.py`（BarSeries 固有方法） |
| 权重计算、风险预算分配 | `portfolio/` 层 |
| 子指数错峰调度日历 | `portfolio/scheduler/` |
| 合约换月规则 | `data/model/roll.py` |
| 境外持仓 FX 估值 | `portfolio/valuation/` |
| 回测 P&L 计算 | `backtest/` 层 |

---

---

## 组合构建层（portfolio/）设计思路

### 总体结构

```
portfolio/
├── sizing/        # 信号 → per-asset 原始权重
├── constraints/   # 原始权重 → 约束后权重
├── scheduler/     # 再平衡时间表生成
├── selectors.py   # 截面 score → 仓位意图桥接
├── blender.py     # 多子组合权重融合
└── fx_handler.py  # FX 辅助工具（供 backtest 层使用）
```

---

### 一、职责边界原则

**portfolio 层是纯函数层**：输入信号与市场数据，输出目标权重 DataFrame（dates × symbols），不持有任何状态。

不属于 portfolio 层的内容：

| 内容 | 实际归属 |
|------|---------|
| 持仓状态追踪（h_cny, h_usd） | backtest 层 |
| VRS 波动率重置（操作持仓） | backtest 层 |
| 交易费、管理费计算 | backtest/fees/ |
| 每日 P&L 聚合与 NAV 计算 | backtest 层 |
| FX 汇率数据加载 | data/loader.py |

这样设计的核心好处：portfolio 层无副作用，可独立测试，可在不同回测引擎中复用。

---

### 二、sizing/ — 信号到原始权重

Sizer 是 portfolio 层的第一步：给定品种的仓位意图矩阵和波动率估计，输出每个品种的原始权重（未施加任何约束）。

```
Sizer ABC: compute(signal_df, vol_df) → raw_weight_df
```

这里的 `signal_df` 可以是：

- 方向型：`{-1, 0, +1}`
- 强度型：保留多空方向和强弱差异的连续浮点值

纯截面排名分数，例如 `[0, 1]` rank score，不应直接传入 sizer；这类 score 应先映射为
long / short / flat 仓位意图，再进入定仓。

**两种 Sizer 实现，对应两个项目的核心逻辑：**

- `equal_risk.py`（CTA）：`w_i = direction_i × (target_vol / sigma_i) / N_active`
  - 每个品种独立控制波动贡献，组合波动 ≈ `target_vol / sqrt(N)`
  - 通过除以当日有效持仓数 N_active 实现等风险分配
  - 支持 `signal_mode="direction" | "raw"`

- `risk_budget.py`（GMAT3）：`w_i = direction_i × (rb_unit / vol_divisor)`
  - `rb_unit = 10% / (N_mom + N_rev / 2)`，按动量/反转选中情况分配预算
  - `vol_divisor` 由 TVS 信号决定：TVS > 0 时使用更严格的多窗口最大波动率
  - 支持 `signal_mode="direction" | "raw"`

---

### 三、constraints/ — 权重约束

Sizer 产出原始权重后，由约束层依次处理：

- `weight_cap.py`：品种权重硬上界（GMAT3 各品种有独立的 `weight_ub`，如国债可到 0.60，股指和商品 0.10）
- `vol_scaler.py`：WAF（Weight Adjustment Factor），组合层面的波动缩放
  - 计算组合历史波动 `vol_max = max(vol_22, vol_65, vol_130)`
  - 若 `vol_max > 4.5%`：`WAF = min(1.5, 4% / vol_max)` → 缩小权重
  - 否则：`WAF = 1.0` → 不调整
  - 注意：WAF 是**权重**层面的控制，与 backtest 层 VRS（**持仓**层面）不同

---

### 四、scheduler/ — 再平衡时间表

Scheduler 负责生成两类日期序列：**计算日**（何时用信号产出权重）和**调仓日**（何时将权重应用到持仓）。

```
RebalanceScheduler ABC:
    produce_schedule(calendar, start, end)
        → DataFrame: columns=[calc_date, adjust_date, sub_index]
```

**两种 Scheduler 实现：**

- `monthly.py`（CTA）：每月最后一个交易日为计算日，次交易日为调仓日（1日滞后），单一子组合

- `staggered.py`（GMAT3）：通用多子组合错峰接口
  - 参数：`n_sub`（子组合数量）、`blend_weights`（融合权重，None 表示等权）
  - 子组合 1：每月第 2 个交易日计算，第 4 个交易日调仓
  - 子组合 2/3/4：各比前一个错后 1 个交易日
  - GMAT3 即 `StaggeredScheduler(n_sub=4)`，等权 1/4 融合
  - 设计为通用接口而非硬编码，支持未来扩展为其他数量或非等权的错峰配置

---

### 五、blender.py — 多子组合融合

当 scheduler 产出多个子组合的中间权重时，blender 负责将它们融合为最终目标权重：

```python
blend(sub_weights: Dict[int, pd.DataFrame], weights: List[float] = None)
    → pd.DataFrame   # 最终 target 权重（dates × symbols）
```

默认等权平均（GMAT3 的 1/4 融合是其特例），也支持非等权配置。
`ffill` 只在每个子组合自身的有效日期区间内生效，避免旧权重在子组合结束后被无限期延续；
有效区间外的权重会被显式视为 0。

---

### 五点五、selectors.py — score 到仓位意图的桥接

signals 层的截面组合器常输出 score，而不是可直接定仓的方向矩阵。为避免 portfolio 层直接把
`[0, 1]` score 误当成“全多头”，增加轻量桥接层：

- `TopBottomSelector(top_n, bottom_n)`：将截面 top / bottom 映射为 `{-1, 0, +1}`
- `ThresholdSelector(long_threshold, short_threshold)`：按阈值映射为 `{-1, 0, +1}`

---

### 六、fx_handler.py — FX 辅助工具

不是 portfolio 层的核心功能，而是一个工具模块，供 backtest 层在处理多货币持仓时调用：

- 提供 USD 持仓 → CNY 市值的转换计算
- 提供累积 USD P&L 的汇率重估辅助（每日 FX 变动 × 历史累积 USD P&L）
- 数据（FX 汇率序列）由 `data/loader.py` 加载，此处只做计算

---

---

## 回测层（backtest/）设计思路

### 总体结构

```
backtest/
├── engine.py      # BacktestEngine：主循环，可插拔组件
├── position.py    # PositionTracker：持仓状态（SimpleTracker / FXTracker）
├── fees/          # 费用模型
│   ├── base.py
│   ├── zero.py
│   ├── trading.py
│   └── tracking.py
├── execution/     # 执行模型
│   ├── lag.py     # 执行滞后
│   └── vrs.py     # VRS 波动率重置
└── result.py      # BacktestResult 输出对象
```

---

### 一、设计原则

**单一引擎 + 可插拔组件**：一个 `BacktestEngine` 核心，通过注入不同的
`PositionTracker`、`FeeModel` 列表和 `VRS` 开关，覆盖从简单到复杂的所有场景。
CTA 和 GMAT3 是同一引擎的不同配置，不存在两套独立循环。

**backtest 层只产出 NAV + 日收益**，不计算绩效指标（Sharpe、回撤等）。
绩效指标统一由 `analysis/metrics.py` 计算，analysis 层可被独立复用
（如直接分析外部 NAV 数据，无需经过回测引擎）。

**默认轻量输出，verbose 模式开启完整日志**：日常参数扫描只需 NAV 序列，
深度调试或归因分析时才需要持仓快照、费用明细、调仓记录。

---

### 二、engine.py — 主循环

`BacktestEngine` 负责驱动每日模拟循环，不含业务逻辑，只做组件编排：

```python
class BacktestEngine:
    def __init__(
        self,
        position_tracker: PositionTracker,  # 持仓模型
        fee_models: List[FeeModel],         # 可叠加多个费用
        vrs: VRS = None,                    # None = 不启用
        lag: int = 1,                       # 执行滞后天数
    ): ...

    def run(
        self,
        weight_df: pd.DataFrame,     # portfolio层产出的目标权重
        price_df: pd.DataFrame,      # 连续合约价格（data层）
        schedule: RebalanceSchedule, # 调仓时间表（portfolio/scheduler）
        fx_series: pd.Series = None, # 汇率（FXTracker需要）
        verbose: bool = False,
    ) -> BacktestResult: ...
```

**两种典型配置：**

```python
# CTA 简单模式
BacktestEngine(
    position_tracker=SimpleTracker(),
    fee_models=[ZeroFee()],
    vrs=None, lag=1,
)

# GMAT3 完整模式
BacktestEngine(
    position_tracker=FXTracker(currency_map=...),
    fee_models=[TradingFee(rate=0.0005), TrackingFee(annual_rate=0.005)],
    vrs=VRS(threshold=0.045, target=0.04),
    lag=1,  # GMAT3的2日滞后已在scheduler的calc/adjust间隔中体现
)
```

---

### 三、position.py — 持仓追踪

封装每日持仓状态，隔离 CTA（无显式持仓）与 GMAT3（双轨持仓）的实现差异：

**`SimpleTracker`**（CTA）：权重驱动，无显式持仓向量。
每日 P&L = `Σ(w_{t-1} × r_t)`，直接用权重乘收益，无需追踪实际头寸。

**`FXTracker`**（GMAT3）：双轨持仓，含 FX 重估：
- `h_cny[j]`：国内品种 CNY 头寸
- `h_usd[j]`：境外品种 USD 头寸
- `accum_usd_pnl[j]`：上次调仓以来累积 USD 收益（每日用 FX 变动重估）
- P&L 公式：`Σ(h_cny × r) + (fx_t - fx_{t-1}) × Σ(accum_usd_pnl) + fx_t × Σ(h_usd × r_usd)`

---

### 四、fees/ — 费用模型

可插拔、可叠加，引擎在调仓日和 VRS 日自动调用：

| 文件 | 说明 | 触发时机 |
|------|------|---------|
| `zero.py` | 无费用（CTA 默认） | — |
| `trading.py` | `rate × Σ\|Δholdings\|` | 调仓日 + VRS 日 |
| `tracking.py` | `annual_rate/365 × base_val` | 每日计提 |

---

### 五、execution/ — 执行模型

**`lag.py`**：控制权重计算到实际执行的滞后天数（`shift(lag)`）。
CTA 是 1 日滞后；GMAT3 的 2 日滞后已内嵌在 `StaggeredScheduler` 的
calc/adjust 日间隔中，引擎层 `lag=1` 即可。

**`vrs.py`**：波动率重置（Volatility Reset Signal）。
- 触发：`max(vol_22, vol_65, vol_130) > 4.5%` 且未来 2 日无调仓日
- 执行：`h_new = h × (4% / VRS)`，扣交易费，重置 `accum_usd_pnl`
- 仅对 `FXTracker` 有意义；`SimpleTracker` 模式下 VRS 不生效

---

### 六、result.py — 输出对象

```python
@dataclass
class BacktestResult:
    nav: pd.Series            # 每日 NAV（始终输出）
    returns: pd.Series        # 每日对数收益（始终输出）

    # verbose=True 时额外输出
    holdings_log: pd.DataFrame = None   # 每日持仓快照
    rebalance_log: pd.DataFrame = None  # 调仓记录（日期/旧权重/新权重/交易费）
    fee_log: pd.DataFrame = None        # 每日费用明细
```

`BacktestResult` 不计算绩效指标，仅作数据容器，通过 `.to_analysis_input()`
传递给 `analysis/metrics.py` 进行绩效分析。

---

### 七、边界确认

| 内容 | 实际归属 |
|------|---------|
| 绩效指标（Sharpe/回撤/胜率） | `analysis/metrics.py` |
| 危机 Alpha / 归因分析 | `analysis/` 层 |
| 可视化图表 | `analysis/report/` |
| 权重生成 | `portfolio/` 层 |
| 信号计算 | `signals/` 层 |
| WAF（权重级约束） | `portfolio/constraints/` |
| VRS（持仓级约束） | `backtest/execution/vrs.py` |

---

---

## 分析层（analysis/）设计思路

### 总体结构

```
analysis/
├── metrics.py       # 标准绩效指标（核心，全局复用）
├── attribution/     # 收益归因分析
├── crisis/          # 危机/尾部分析
├── signal/          # 信号质量分析
├── cost/            # 成本分析
└── report/          # 报告生成器
```

---

### 一、设计原则

**计算与可视化严格分离**

分析函数只做计算，返回标准 `pd.DataFrame` 或 `pd.Series`（可测试、可导出
CSV、可复用到不同图表）。图表函数独立在 `report/charts.py`，消费计算结果。

```python
# 两步调用：先算后画
table = crisis_alpha_analysis(nav, returns_df, benchmark="IF")
charts.plot_crisis_alpha(table)

# 或用报告生成器一键完成
StrategyReport(backtest_result).generate(output_dir="results/")
```

**metrics.py 是全局公共基础**，不属于任何子模块，任何层（backtest 层输出、
外部导入的 NAV 数据）都可直接调用，无需经过策略研究流程。

---

### 二、metrics.py — 绩效指标

接受任意 NAV 序列，输出标准指标字典或 DataFrame：

```python
performance_summary(nav, rf=0.02) -> dict
    # 年化收益率、年化波动率、Sharpe、Sortino
    # 最大回撤、最大回撤持续天数、Calmar
    # 胜率、盈亏比

rolling_metrics(nav, window=252) -> pd.DataFrame
    # 滚动窗口的 Sharpe / 年化波动 / 最大回撤

underwater_series(nav) -> pd.Series
    # 每日回撤水位（用于绘制水下曲线图）
```

---

### 三、各子模块定位

**attribution/** — 收益归因

| 文件 | 研究问题 | 输入 | 输出 |
|------|---------|------|------|
| `asset.py` | 哪些品种驱动了收益？ | 收益 DF + 权重列表 | 品种贡献 Series + 年度贡献热图 DF |
| `sector.py` | 哪些板块趋势信号最强？ | 收益 DF + 板块映射 | 板块 × 绩效指标矩阵 |

**crisis/** — 危机与尾部分析

| 文件 | 研究问题 | 输入 | 输出 |
|------|---------|------|------|
| `alpha.py` | 危机期间策略如何保护资本？ | NAV + 基准收益 + 危机事件表 | 事件 × 收益对比 DF |
| `convexity.py` | 策略是否具备凸性/微笑曲线？ | 策略收益 + 基准收益 | 分位数 × 平均收益 DF |

危机事件定义（国内7个、境外8个历史崩盘）作为常量内嵌在 `alpha.py` 中，
支持自定义传入，不强制使用内置列表。

**signal/** — 信号质量分析

| 文件 | 研究问题 | 输入 | 输出 |
|------|---------|------|------|
| `persistence.py` | 动量有持续性还是均值回归？ | 收益 DF 面板 | beta + t 统计量 vs 滞后期 DF |
| `long_short.py` | 多头/空头侧收益是否对称？ | 收益 DF + 波动率 DF | 多/空/双向三条 NAV 序列 |
| `correlation.py` | 策略与股债的分散化程度？ | NAV 字典 + 基准收益 | 相关系数矩阵 DF |

**cost/** — 成本分析

- `fee_decomp.py`：回答"费用拖累了多少收益"的研究问题。
  以不同费用配置（零费用 / 仅交易费 / 完整费用）多次调用 `BacktestEngine`，
  对比差值输出成本分解 DataFrame。
  归 `analysis/` 层而非 `backtest/` 层，因为它回答的是研究问题，
  而不是扩展引擎本身的执行能力。

---

### 四、report/ — 报告生成器

**计算与绘图分离**：所有图表函数集中在 `charts.py`，接受各分析函数的
标准 DataFrame 输出，不含任何计算逻辑。

**`StrategyReport`** 是报告编排器，接受 `BacktestResult` 和策略元数据，
按固定顺序调用分析函数 + 图表函数，自动输出完整报告：

```
标准策略报告（对应 CTA main.py 的8图流程）：
  图1  NAV 曲线（对数坐标，多策略对比）
  图2  绩效指标汇总表
  图3  危机 Alpha 柱状图
  图4  多空不对称 NAV 曲线
  图5  板块绩效热图
  图6  动量持续性（beta vs 滞后期）
  图7  危机凸性微笑曲线
  图8  收益归因（品种贡献条形 + 年度热图）
```

支持按需定制：传入 `include=["nav", "crisis", "attribution"]`
只生成指定图表，不强制输出全套。

---

### 五、边界确认

| 内容 | 实际归属 |
|------|---------|
| 绩效指标（Sharpe/回撤等）具体数值 | `analysis/metrics.py` |
| 回测执行循环 | `backtest/engine.py` |
| 图表样式/配色主题 | `report/charts.py` |
| 危机事件常量（默认值） | `analysis/crisis/alpha.py` |
| 日历工具（交易日计数） | `data/model/calendar.py` |

---

---

## 策略层（strategies/）设计思路

### 总体结构

```
strategies/
├── base/
│   ├── strategy.py          # StrategyBase ABC（顶层抽象接口）
│   ├── trend.py             # TrendFollowingStrategy（趋势族公共逻辑）
│   └── cross_sectional.py   # CrossSectionalStrategy（截面族公共逻辑）
│
├── implementations/
│   ├── domestic_tsmom.py    # DomesticTSMOM(TrendFollowingStrategy)
│   ├── crossmom.py          # CrossMOM(CrossSectionalStrategy)
│   └── gmat3/               # GMAT3Strategy 包：data_access/main_contract/roll_return/sub_portfolio/signals/schedule/weights/index_builder/strategy/universe
│
├── roll_research/           # 展期策略研究（策略性研究，区别于data层机械展期规则）
│   ├── rules.py             # 研究级展期规则：基差驱动、carry驱动、动量展期等
│   └── backtest.py          # 评估不同展期方式对成本/收益的影响
│
└── configs/                 # YAML 策略配置文件（参数与代码分离）
    ├── domestic_tsmom.yaml
    ├── crossmom.yaml
    └── gmat3.yaml
```

---

### 一、策略层的定位

策略层是**组装层**，将 signals/、portfolio/、backtest/ 三层的组件
按具体策略逻辑组合起来。它本身不重复实现信号计算或组合优化，
而是**规定每类策略族共用的组合方式**，并将可配置参数集中管理。

```
DataLoader → signals/ → portfolio/ → backtest/
                 ↑            ↑
           strategy.generate_signals()  strategy.build_weights()
```

---

### 二、base/strategy.py — 顶层抽象接口

所有策略的公共接口，规定最小协议：

```python
class StrategyBase(ABC):
    def __init__(self, loader: DataLoader, config: dict): ...

    @abstractmethod
    def generate_signals(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """返回连续浮点信号矩阵，shape: (dates, assets)"""

    @abstractmethod
    def build_weights(self, signals: pd.DataFrame, vol_df: pd.DataFrame) -> pd.DataFrame:
        """返回目标权重矩阵，shape: (dates, assets)"""

    def run(self, start: str, end: str) -> BacktestResult:
        """加载数据 → 信号 → 权重 → 回测，一键运行"""
```

---

### 三、base/trend.py — 趋势策略族公共逻辑

封装趋势跟踪策略族的两个核心共性：

**① 多周期信号合成**

```python
class TrendFollowingStrategy(StrategyBase):
    def __init__(self, lookbacks: list[int], vol_halflife: int,
                 target_vol: float, signal_weights: list[float] = None): ...

    def generate_signals(self, returns_df) -> pd.DataFrame:
        # 对每个 lookback 计算 TSMOM 信号
        # 按 signal_weights 加权合成（默认等权）
        # 返回复合信号矩阵
```

**② 等风险定仓公式**

```python
    def build_weights(self, signals, vol_df) -> pd.DataFrame:
        # w_i = direction_i × (target_vol / sigma_i) / N_active
        # 委托给 portfolio/sizers/equal_risk.py 计算
```

子类只需覆盖 `__init__` 传入具体参数，或覆盖
`generate_signals` 定制信号合成逻辑。

---

### 四、base/cross_sectional.py — 截面策略族公共逻辑

封装截面动量/反转策略族的两个核心共性：

**① 资产打分与多空分位数切分**

```python
class CrossSectionalStrategy(StrategyBase):
    def __init__(self, score_lookbacks: list[int],
                 top_pct: float = 0.2, bottom_pct: float = 0.2,
                 dollar_neutral: bool = True): ...

    def score_assets(self, returns_df) -> pd.DataFrame:
        # 多周期均值排名打分（复用 signals/trend/ 或 signals/cross_sectional/）
        # 返回截面得分矩阵

    def select_assets(self, scores) -> tuple[pd.DataFrame, pd.DataFrame]:
        # 按 top_pct / bottom_pct 切分多/空候选池
```

**② 多空组合权重分配**

```python
    def build_weights(self, long_assets, short_assets, vol_df) -> pd.DataFrame:
        # 默认：多空各自等风险，整体美元中性
        # 委托给 portfolio/sizers/ 计算
```

---

### 五、implementations/ — 具体策略

**DomesticTSMOM**（国内期货时序动量）继承 `TrendFollowingStrategy`：

```python
class DomesticTSMOM(TrendFollowingStrategy):
    # 参数默认值来自 configs/domestic_tsmom.yaml
    # lookbacks=[21, 63, 126, 252]
    # vol_halflife=60, target_vol=0.40
    # 可选：板块权重约束（委托 portfolio/constraints/）
```

**GMAT3Strategy** 仍作为统一入口暴露，但内部已拆为
`strategies/implementations/gmat3/` 包，
便于分别承接 universe、schedule、weights、index_builder 等复杂职责。
当前阶段已打通 `data_access`、`main_contract`、`roll_return`、`sub_portfolio`
以及 `value_df_full / value_df` 输入层，并已落下 `signals / weights / index_builder`
主干实现。项目中还新增了 `scripts/gmat3_broad_regression.py`，用于对
`value_df / weight_df / index_series` 执行更大范围的真实数据回归；
同时 `strategy.py` 也已升级为正式端到端入口，可直接返回 `GMAT3RunResult`。
当前主要剩余工作转向更长期区间的 GMAT3 全链路验证与对外运行脚本/配置收口。

其核心入口仍继承 `CrossSectionalStrategy`，
并覆盖 `build_weights` 以引入 TVS vol_divisor、WAF、StaggeredScheduler：

```python
class GMAT3Strategy(CrossSectionalStrategy):
    def build_weights(self, scores, vol_df) -> pd.DataFrame:
        # 多周期 Mom 打分 → 选品种
        # risk budget 定仓 → TVS vol_divisor 缩放 → WAF 约束
        # 4子组合交错执行（StaggeredScheduler）
        # 委托给 portfolio/ 各组件
```

---

### 六、roll_research/ — 展期策略研究

与 `data/model/roll.py`（机械展期规则基础设施）区分：
`roll_research/` 是**策略性研究**，回答"如何展期才能降低成本、减少滑点？"

```python
# rules.py：研究级展期规则
class BasisDrivenRoll(RollRule):
    """当前合约基差超过阈值时触发展期，捕捉 carry 机会"""

class CarryOptimizedRoll(RollRule):
    """选择 carry 最高的合约持有，不固定换月时间"""

class MomentumRoll(RollRule):
    """持有成交量动量最强的合约"""

# backtest.py：对比不同展期规则
def compare_roll_strategies(instrument, rules, start, end) -> pd.DataFrame:
    # 返回各展期规则的成本、滑点、NAV 对比表
```

---

### 七、configs/ — 参数与代码分离

每个策略对应一个 YAML 配置文件，策略类从配置加载默认参数，
支持研究时快速调参而无需修改代码：

```yaml
# configs/domestic_tsmom.yaml
lookbacks: [21, 63, 126, 252]
signal_weights: [0.25, 0.25, 0.25, 0.25]
vol_halflife: 60
target_vol: 0.40
universe: domestic_futures_main
rebalance_freq: monthly
```

---

### 八、边界确认

| 内容 | 实际归属 |
|------|---------|
| TSMOM 信号计算 | `signals/trend/tsmom.py` |
| 截面动量打分 | `signals/cross_sectional/momentum.py` |
| 等风险定仓公式 | `portfolio/sizers/equal_risk.py` |
| TVS / WAF / VRS | `signals/risk/` + `portfolio/constraints/` + `backtest/execution/` |
| StaggeredScheduler | `portfolio/scheduler.py` |
| 机械展期规则（OI最大等）| `data/model/roll.py` |
| 研究级展期规则 | `strategies/roll_research/rules.py` |
| 策略参数（lookbacks等）| `strategies/configs/*.yaml` |

---

---

## 完整目录结构总览

```
cta_lab/
│
├── data/                          # 数据层
│   ├── model/                     # 领域对象模型
│   │   ├── instrument.py          # Instrument：品种静态信息
│   │   ├── contract.py            # Contract：单一合约信息
│   │   ├── bar.py                 # Bar / OHLCV K线数据结构
│   │   ├── calendar.py            # TradingCalendar：交易日历
│   │   ├── roll.py                # RollRule ABC + 机械展期规则（OI最大、成交量最大、日历）
│   │   └── continuous.py          # ContinuousSeries：连续合约构建
│   │
│   ├── sources/                   # 数据源适配（读取方式抽象）
│   │   ├── base.py                # DataSource ABC
│   │   ├── parquet.py             # ParquetSource
│   │   ├── ddb.py                 # DDBSource（本地 DolphinDB）
│   │   ├── sql.py                 # SQLSource
│   │   ├── csv.py                 # CSVSource
│   │   └── binary.py              # BinarySource
│   │
│   └── loader.py                  # DataLoader：统一数据服务入口
│                                  #   load_klines() / load_contracts()
│                                  #   load_continuous() / load_calendar()
│
├── signals/                       # 信号层
│   ├── base.py                    # Signal ABC（时序信号） + CrossSectionalSignal ABC
│   │
│   ├── trend/
│   │   ├── tsmom.py               # TSMOM：时序动量信号
│   │   └── ewma_crossover.py      # 均线交叉信号
│   │
│   ├── cross_sectional/
│   │   ├── momentum.py            # 截面动量打分（多周期均值排名）
│   │   └── reversal.py            # 截面反转信号（MASS）
│   │
│   └── risk/
│       └── tvs.py                 # TVS：尾部波动锐度（GMAT3 风险信号）
│
├── portfolio/                     # 组合构建层（无状态，输出目标权重 DataFrame）
│   ├── sizing/
│   │   ├── base.py                # Sizer ABC：仓位意图矩阵 × 波动率 -> 原始权重
│   │   ├── equal_risk.py          # 等风险定仓：支持 direction/raw 两种信号模式
│   │   └── risk_budget.py         # 风险预算定仓：支持 direction/raw + TVS 波动切换
│   │
│   ├── constraints/
│   │   ├── weight_cap.py          # WeightCap：权重上界约束
│   │   └── vol_scaler.py          # WAF：组合波动率超阈值时缩减权重
│   │
│   ├── scheduler/
│   │   ├── base.py                # RebalanceRecord / RebalanceScheduler
│   │   ├── monthly.py             # MonthlyScheduler
│   │   └── staggered.py           # StaggeredScheduler：N子组合交错再平衡
│   ├── selectors.py               # TopBottomSelector / ThresholdSelector
│   ├── blender.py                 # 多子组合融合，仅在有效区间内 ffill
│   └── fx_handler.py              # FX 辅助换算与重估
│
├── backtest/                      # 回测层
│   ├── engine.py                  # BacktestEngine：可插拔执行引擎
│   │                              #   注入 PositionTracker / FeeModel / VRS
│   ├── trackers/
│   │   ├── simple.py              # SimplePositionTracker（CNY单币种）
│   │   └── fx.py                  # FXTracker（CNY+USD双账户，FX每日重估）
│   │
│   ├── fees/
│   │   ├── base.py                # FeeModel ABC
│   │   ├── fixed_rate.py          # 固定费率（交易费）
│   │   └── tracking_fee.py        # 跟踪费（每日计提）
│   │
│   ├── execution/
│   │   └── vrs.py                 # VRS：波动率重置信号（GMAT3，持仓层操作）
│   │
│   └── result.py                  # BacktestResult：NAV序列 + 收益DF + 元数据
│
├── analysis/                      # 分析层（计算与可视化分离）
│   ├── metrics.py                 # 绩效指标：performance_summary / rolling_metrics / underwater_series
│   │
│   ├── attribution/
│   │   ├── asset.py               # 品种贡献分析（总计 + 年度热图）
│   │   └── sector.py              # 板块绩效分析
│   │
│   ├── crisis/
│   │   ├── alpha.py               # 危机 Alpha：策略 vs 基准在历史崩盘中的表现
│   │   └── convexity.py           # 凸性微笑曲线（期权式收益结构）
│   │
│   ├── signal/
│   │   ├── persistence.py         # 动量持续性（面板 OLS，beta vs 滞后期）
│   │   ├── long_short.py          # 多空不对称分析
│   │   └── correlation.py         # 策略相关性 / 分散化分析
│   │
│   ├── cost/
│   │   └── fee_decomp.py          # 费用分解：交易费 vs 管理费 vs 零费用对比
│   │
│   └── report/
│       ├── charts.py              # 所有图表函数（接受计算结果 → 输出图）
│       └── strategy_report.py     # StrategyReport：标准8图报告编排器
│
├── strategies/                    # 策略层（组装层）
│   ├── base/
│   │   ├── strategy.py            # StrategyBase ABC
│   │   ├── trend.py               # TrendFollowingStrategy（多周期合成 + 等风险定仓）
│   │   └── cross_sectional.py     # CrossSectionalStrategy（打分/切分 + 多空权重）
│   │
│   ├── implementations/
│   │   ├── domestic_tsmom.py      # DomesticTSMOM(TrendFollowingStrategy)
│   │   ├── crossmom.py            # CrossMOM(CrossSectionalStrategy)
│   │   └── gmat3/                 # GMAT3Strategy 包：data_access / main_contract / roll_return / sub_portfolio / strategy / universe / schedule / weights / index_builder
│   │
│   ├── roll_research/             # 展期策略研究
│   │   ├── rules.py               # 研究级展期规则（基差驱动、carry优化、动量展期）
│   │   └── backtest.py            # 对比不同展期方式的成本/收益影响
│   │
│   └── configs/                   # YAML 策略配置（参数与代码分离）
│       ├── domestic_tsmom.yaml
│       ├── crossmom.yaml
│       └── gmat3.yaml
│
├── utils/                         # 通用工具
│   ├── date.py                    # 日期工具（非交易日相关，交易日归 data/model/calendar）
│   ├── logging.py                 # 日志配置
│   └── config.py                  # YAML 配置加载工具
│
├── scripts/                       # 可执行脚本（入口点）
│   ├── run_backtest.py            # 命令行运行回测
│   └── generate_report.py         # 生成策略报告
│
├── notebooks/                     # 研究笔记本
│   ├── data_exploration/
│   ├── signal_research/
│   └── strategy_development/
│
└── tests/                         # 测试
    ├── data/
    ├── signals/
    ├── portfolio/
    └── backtest/
```

---

## 层间依赖关系

```
scripts / notebooks
        ↓
   strategies/          ← configs/*.yaml
   ↙        ↘
signals/   portfolio/
   ↘        ↙
   backtest/
        ↓
   analysis/
        ↓
   report/

所有层共用：
  data/（DataLoader + 领域模型）
  utils/（日志、配置、日期工具）
```

依赖方向单向向下，禁止反向依赖（如 signals/ 不得引用 backtest/）。

---

## 设计原则汇总

| 原则 | 体现 |
|------|------|
| 领域模型驱动 | data/model/ 用业务概念（Instrument/Contract/Bar）组织，而非技术概念 |
| 单一数据入口 | DataLoader 统一对外，内部注入不同 DataSource 适配多种存储 |
| 无状态组合层 | portfolio/ 只产出目标权重 DataFrame，不持有任何执行状态 |
| 计算与可视化分离 | analysis/ 函数返回 DataFrame，图表函数在 report/charts.py |
| 可插拔回测引擎 | BacktestEngine 注入 PositionTracker / FeeModel / VRS，支持 CTA 和 GMAT3 两套逻辑 |
| 策略层为组装层 | strategies/ 不重复实现信号和定仓，只组合 signals/ + portfolio/ + backtest/ |
| 参数与代码分离 | 策略参数集中在 configs/*.yaml，调参不改代码 |
| 族模板封装共性 | TrendFollowingStrategy / CrossSectionalStrategy 固化族内公共逻辑，子类覆盖差异 |

---

## P2 进展补充

`strategies/components/roll/` 正在承接 `Phase P2` 的通用 `Roll Strategy Layer`。

当前已经有两条正在成型的路径：

- `SingleAssetRollStrategy`
  面向 generic contract universe 的单资产 roll asset
- `BundleRollStrategy`
  面向多组件 bundle 资产的第一版组合骨架

这层的定位不是替代 `data.load_continuous()`，而是输出更适合上层配置策略消费的资产级对象：

- `value_series`
- `contract_plan`
- `roll_schedule`
- `lookthrough_book`

当前 bundle 第一版先支持：

- 预先计算好的 component results
- 静态权重 / 等权组合 / 外部动态权重
- 统一的 component-level look-through 汇总

同时，bundle 第二阶段已经开始补 bundle-level sync schema：

- `sync_mode`
- `sync_frequency`
- `sync_components`

当前先把同步展期意图和结果注记正式放进 `roll_schedule`，
后续再逐步补更复杂的同步调仓逻辑。

另外，`GMAT3 BLACK` 的结构性业务规则也已经开始映射到这层：

- `build_gmat3_black_bundle_profile()`
- `external` 动态权重模式
- 年度 rebalance + 10 日平滑
- `sync_mode="rebalance"`

这一步先解决“规则结构如何放进通用 bundle profile”，
后续再继续迁移真实动态权重计算。

当前还补了一条更简洁的落地路径：

- `compute_gmat3_black_component_target_weights()`
- `build_gmat3_black_bundle_market_data()`
- `run_gmat3_black_bundle()`

这组 helper 直接复用现有 `BundleRollStrategy(weight_mode="external")`，
先把 GMAT3 旧 `BLACK` 的真实动态权重计算接进 roll layer，
避免为了一个案例把 bundle 抽象过度做重。
当前 helper 的权重计算口径也已明确为：
- 至少 `125` 个交易日历史
- 用最近 `120` 个交易日的持仓金额均值做 base weight source

后续再逐步推进到动态权重、同步展期与 `BLACK` 这类跨品种 bundle。

---

*架构设计文档完成。下一步：按层逐步实施。*
