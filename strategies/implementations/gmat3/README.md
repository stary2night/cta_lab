# GMAT3 On `cta_lab`

这份文档是 `cta_lab` 上实现 GMAT3 策略的 **Phase G1** 产物。

目标不是立刻把 `ddb/gmat3` 全量照搬进来，而是先把 GMAT3 的业务逻辑、关键中间对象、旧实现到 `cta_lab` 的落点关系冻结清楚。后续 G2-G6 的代码实现，都应以这份映射为准。

---

## 一、GMAT3 要解决的业务问题

GMAT3 不是普通的单层趋势策略，而是一条完整的指数构建流水线：

1. 加载国内外期货、替代标的、交易日历、汇率数据
2. 为每个子组合确定主力合约序列
3. 基于主力合约计算展期收益率
4. 构造每个子组合的价值序列 `V_c(t)`
5. 在指数计算日上计算动量/反转/风险信号
6. 生成 4 个子指数错峰调仓的目标权重
7. 叠加交易费、跟踪费、VRS、FX 重估，合成最终指数点位

这意味着，GMAT3 不是单一 `signal + sizer + backtest`，而是一个横跨 `data / signals / portfolio / backtest / strategy` 五层的复杂策略系统。

---

## 二、`ddb/run.py` 主流水线与 `cta_lab` 映射

`ddb/run.py` 的 7 个步骤，可以先映射成下面这张表：

| `ddb` 步骤 | 旧模块 | 业务含义 | `cta_lab` 落点 |
|---|---|---|---|
| Step 1 | `gmat3.data_loader.DataLoader` | 加载交易日历、合约信息、日线、替代标的、FX | `data/` + `strategies/implementations/gmat3/universe.py` + GMAT3 专用 loader/config |
| Step 2 | `MainContractEngine` | 各子组合主力合约序列 | `data/model/roll.py` + `data/model/continuous.py` 的扩展，必要时由 `strategies/implementations/gmat3/` 封装 |
| Step 3 | `SubPortfolioEngine` | 子组合价值序列 `V_c(t)` | `strategies/implementations/gmat3/` 的子组合价值构建逻辑 |
| Step 4 | `run.py` 内部流程 | 指数计算日、预热期、价值矩阵对齐 | `strategies/implementations/gmat3/schedule.py` + `backtest/` 扩展 |
| Step 5 | `WeightCalculator` | 动量/反转/风险预算/WAF/4 子指数 schedule | `signals/` + `portfolio/` + `strategies/implementations/gmat3/weights.py` |
| Step 6 | `IndexCalculator` | FX、TC、Tracking Fee、VRS、指数点位 | `strategies/implementations/gmat3/index_builder.py` + `backtest/` 扩展 |
| Step 7 | `run.py` 导出结果 | 指数、子组合价值、权重、调仓日历 | notebook / script / strategy report |

结论是：GMAT3 不能只放在 `signals` 或 `portfolio` 层处理，它必须作为 `strategies/implementations/gmat3/` 下的策略子系统来实现。

---

## 三、GMAT3 的关键中间对象

在 `cta_lab` 中实现 GMAT3 时，最重要的不是直接盯最终 `index_series`，而是先把下面这些中间对象一层层对齐：

### 1. `contract_info`

来源：
- `ddb/data/raw/domestic/contract_info_domestic.parquet`
- `ddb/data/raw/overseas/contract_info.parquet`

作用：
- 提供合约生命周期
- 计算 `last_holding_date`
- 支撑不同品种的展期规则

这类信息在 `cta_lab` 中应主要落到：
- `data/loader.py`
- `data/model/contract.py`
- GMAT3 专用 schema / adapter

### 2. `main_df`

旧实现中，`MainContractEngine.compute(variety)` 返回单品种主力合约结果表。

这个对象不是单纯价格序列，而是“主力合约调度表”，后续展期收益率和子组合价值都依赖它。

在 `cta_lab` 中，它更接近：
- `ContractSchedule`
- 或者 GMAT3 专用 `main_contract` DataFrame

这层是 G3 的重点。

### 3. `roll_ret`

来源：
- `RollReturnCalculator.compute(variety, main_df)`

它是 GMAT3 的关键收益定义。很多品种不是简单使用 `ContinuousSeries.pct_change()`，而是按说明书口径的主力切换与展期方式构造。

所以这里不能直接假设：
- `ContinuousSeries.log_returns()` 就等价于 GMAT3 的展期收益率

需要逐品种核对。

### 4. `V_c(t)` 子组合价值序列

来源：
- `SubPortfolioEngine.compute(variety, main_dfs)`

它的业务规则很关键：
- 切换日期前，使用替代标的收益
- 切换日期后，使用期货展期收益
- 黑色系 `BLACK` 不是单一品种，而是动态权重组合
- 境外子组合在这一步仍保留 USD 计价，不在此处转 CNY

这意味着 `V_c(t)` 不是一般意义的策略净值，而是“指数输入层”的子组合价值对象。

### 5. `value_df_full` / `value_df`

来源：
- `run.py` Step 4

定义：
- `value_df_full`：含预热期的价值矩阵，供信号计算
- `value_df`：正式指数计算区间，供指数点位合成

这一层是 notebook/研究中最容易忽略但非常重要的地方。GMAT3 许多信号需要长历史窗口，不能只截正式期。

### 6. `weight_df` + `schedule`

来源：
- `WeightCalculator.compute(value_df_full, calc_days)`

业务特点：
- 指数交易日不是单一交易所日历，而是多子组合共同可交易的日期体系
- 4 个子指数各自有 `calc_dates` 与 `adjust_dates`
- 权重只在调整日切换，计算日只是更新“待应用”的中间权重

这和普通 CTA 的“每日权重 + shift(1)”差异很大。

### 7. `index_series`

来源：
- `IndexCalculator.compute(...)`

业务特点：
- 叠加交易费与跟踪费
- 境外品种先积累 USD PnL，再按当日汇率统一重估到 CNY
- 调仓日重置 USD 累积盈亏
- 存在 VRS 触发的额外波动率重置

这说明 GMAT3 的最终指数层，本质更接近“指数编制引擎”，而不是普通 backtest NAV。

---

## 四、GMAT3 特有业务逻辑清单

这些逻辑都需要在 `cta_lab` 中明确承接，不能在实现过程中被“简化掉”：

### 1. 多类主力规则并存

`ddb/gmat3/main_contract.py` 中并不是单一 `OIMaxRoll`：
- 境内股指
- 境内国债
- 境内商品
- 境外股指/国债窗口规则
- Brent 月历规则

结论：
- `cta_lab` 现有 `RollRule` 基础设施可复用
- 但 GMAT3 需要专门实现一批说明书口径的 rule

### 2. 替代标的切换

一些境内股指/国债子组合在期货上市前，先用替代指数构造价值序列：
- `IF -> 000300.SH`
- `IC -> 000905.SH`
- `TS / TF / T` 也有替代标的

这类逻辑不是通用 continuous 能自然覆盖的，需要 GMAT3 子组合层显式实现。

### 3. 黑色系复合子组合

`BLACK` 不是一个单品种，而是：
- `RB/HC/I/J/JM`
- 基于历史持仓金额动态分配权重
- 每年 4 月按规则重新估计目标权重
- 10 个工作日线性平滑过渡

这是典型的“策略内部子组合构造逻辑”，应落在 `strategies/implementations/gmat3/`，而不是通用 `portfolio/`。

### 4. 三类信号共同作用

旧实现中有三类信号：
- 动量
- 反转
- 风险（TVS）

并且不是简单线性相加，而是：
- 动量决定入选集
- 反转决定额外风险预算
- 风险信号决定波动率惩罚方式

因此，GMAT3 的信号层不是“单一 score matrix”，而是带业务含义的多路状态。

### 5. 4 子指数错峰调仓

GMAT3 权重不是单一组合的即时目标权重，而是：
- 子指数 1~4 各有自己的 `calc_dates` / `adjust_dates`
- 最终总权重是 4 个已应用子指数权重的均值

这部分需要 `schedule.py` 与 `weights.py` 共同承接。

### 6. FX 处理在指数层，而非子组合层

境外子组合在 `SubPortfolioEngine` 中保持 USD 计价，直到 `IndexCalculator` 才转 CNY。

这意味着：
- FX 不应过早混入 `value_df`
- `backtest` 层需要支持“本币 PnL 累积 + 汇率重估”的逻辑

---

## 五、`ddb/gmat3` 到 `cta_lab` 的文件级映射

### 旧实现

- `ddb/gmat3/constants.py`
- `ddb/gmat3/data_loader.py`
- `ddb/gmat3/main_contract.py`
- `ddb/gmat3/roll_return.py`
- `ddb/gmat3/sub_portfolio.py`
- `ddb/gmat3/signals.py`
- `ddb/gmat3/weight.py`
- `ddb/gmat3/index.py`

### `cta_lab` 新落点

- `strategies/implementations/gmat3/config.py`
  - 参数、缺省值、常量桥接
- `strategies/implementations/gmat3/universe.py`
  - `SUB_PORTFOLIOS`、`BLACK_COMPONENTS`、资产分组与元数据
- `strategies/implementations/gmat3/schedule.py`
  - 4 子指数错峰调仓与指数计算日体系
- `strategies/implementations/gmat3/weights.py`
  - 风险预算、WAF、中间权重
- `strategies/implementations/gmat3/index_builder.py`
  - FX、TC、Tracking Fee、VRS、指数合成
- `strategies/implementations/gmat3/strategy.py`
  - 对外统一入口 `GMAT3Strategy`

同时依赖 `cta_lab` 现有公共层：
- `data/`
- `signals/`
- `portfolio/`
- `backtest/`
- `analysis/`

---

## 六、现阶段可复用能力与缺口

### 已可复用

- `data` 层的 source / schema / calendar / contract / continuous 基础能力
- `signals` 层的时序/截面信号抽象
- `portfolio` 层的 selector / risk budget / WAF 基础骨架
- `backtest` 层的 fee / tracker / VRS 基础设施

### 还缺的关键能力

- GMAT3 专用数据接入与 schema
- 多类主力规则的说明书级实现
- `V_c(t)` 子组合价值构造
- 子指数 `calc_dates` / `adjust_dates`
- FX 累积 PnL 重估
- 最终指数点位编制

这也解释了为什么当前 `cta_lab` notebook 能跑基础 CTA 研究，但还不能直接对齐 `ddb/results`。

---

## 七、G2-G6 实施顺序

### G2：数据接入

目标：
- 让 `cta_lab` 能直接读取 `ddb/raw/domestic`、`ddb/raw/overseas`、`GMAT3_USDCNY`

优先产物：
- GMAT3 专用 DataLoader 配置
- 国内/海外/FX 验证 notebook 或脚本

当前进展：
- 已新增 `GMAT3DataAccess`
- 已支持读取：
  - `trading_calendar.parquet`
  - `domestic/contract_info_domestic.parquet`
  - `domestic/daily_{index,bond,commodity}_futures.parquet`
  - `overseas/contract_info.parquet`
  - `overseas/daily_{ES,NQ,TU,FV,TY,LCO}.parquet`
  - `domestic/substitute_indices.parquet`
  - `overseas/fx_usdcny.parquet`
- 当前统一入口：
  - `get_contract_info(variety)`
  - `get_daily(variety)`
  - `get_substitute_price(wind_code)`
  - `get_fx_rate()`
  - `trading_days(exchange)`

G2 仍未完成的部分：
- `last_holding_date` 的说明书级计算尚未迁入
- 仍未产出 `cta_lab/data` 层的通用 schema 适配
- 还没有把 GMAT3 数据接入与 `DataLoader` / `ContinuousSeries` 深度打通

### G3：主力合约与子组合价值

目标：
- 逐品种复现 `main_df`
- 逐子组合复现 `V_c(t)`

优先产物：
- `main_contract` 对照结果
- `sub_portfolio value` 对照结果

当前进展：
- 已新增 `MainContractEngine`
- 已新增 `RollReturnCalculator`
- 已完成代表性品种主力序列与展期收益的小范围对照测试
- 已新增 `SubPortfolioEngine` 的第一版单资产实现
- 已补齐 `BLACK` 黑色系复合子组合的动态权重实现
- 已补齐 `value_df_full / value_df` 的构建入口
- 当前已支持：
  - 单资产子组合 `V_c(t)`
  - 替代标的阶段 → 期货阶段切换
  - 境外子组合保持本币计价，FX 延后到指数层处理
  - 黑色系 `RB/HC/I/J/JM` 的动态持仓金额权重、4 月调权与 10 天线性过渡
  - 按 `ddb/run.py` 口径构造全量计算日与正式指数计算日
  - 从各子组合价值序列拼接 `value_df_full / value_df`

当前仍未完成：
- `V_c(t)` 全量 universe 对照验证
- `value_df_full / value_df` 的全量 universe 对照验证

---

## 九、G4 当前进展

G3 收口后，GMAT3 在 `cta_lab` 中已经具备：

- `main_df`
- `roll_ret`
- `V_c(t)`
- `value_df_full / value_df`

这意味着 G4 可以正式开始。当前已经落下两块核心能力：

- `signals.py`
  - `SignalCalculator`
  - 动量 / 反转 / 风险信号基础计算
- `weights.py`
  - `WeightCalculator.compute(value_df_full, calc_days)` 已迁入旧 `ddb/gmat3/weight.py` 的主干逻辑
  - 4 子指数 `calc_dates / adjust_dates`
  - 动量入选与 22 日收益 tie-break
  - 反转入选
  - 风险预算基准倍率
  - TVS 驱动的波动率惩罚
  - `WAF` 风险缩放
  - `weight_ub` 上限约束
  - `build_gmat3_weights()` 仍保留为通用 bridge

目前 `cta_lab` 版 GMAT3 已可以基于 `value_df_full + calc_days` 产出可对照的 `weight_df`。

下一步的重点会转向：
- 继续扩大 G4 的真实数据回归覆盖
- 开始 G5：`index_builder.py`
- 将 FX、交易成本、Tracking Fee、VRS 与最终指数点位接起来

### G4：信号与权重

目标：
- 复现动量/反转/风险信号
- 复现风险预算与 4 子指数 schedule

优先产物：
- `weight_df`
- `schedule`

### G5：指数合成

目标：
- 复现 `index_series`

优先产物：
- `GMAT3IndexBuilder.compute()`
- FX 重估
- 交易成本与 Tracking Fee 计提
- VRS 触发与持仓缩放
- 与旧 `ddb` 指数合成逻辑的小窗口对照

### G5 当前验证状态

已新增 [`scripts/gmat3_broad_regression.py`](/home/ubuntu/dengl/my_projects/cta_lab/scripts/gmat3_broad_regression.py)，
用于在更大 universe 与更长时间窗口下执行 `cta_lab` 与旧 `ddb` 的全链路真实回归。

当前已验证到：
- 截止 `2016-12-31`
- 16 个子组合：`IF / IC / IM / ES / NQ / TS / TF / T / TU / FV / TY / LCO / AU / CU / M / BLACK`
- `value_df`、`weight_df`、`index_series` 三层结果与旧 `ddb` 完全一致

## 十、G6 当前进展

[`strategy.py`](/home/ubuntu/dengl/my_projects/cta_lab/strategies/implementations/gmat3/strategy.py)
已经从轻量占位入口升级为正式的端到端策略封装，当前提供：

- `GMAT3Strategy.run_pipeline(...)`
  - 负责串起 `data_access -> main_contract -> sub_portfolio -> value_df -> weight_df -> index_series`
- `GMAT3Strategy.run(...)`
  - 默认走 GMAT3 端到端流水线
  - 如显式传入 `price_df + engine`，仍兼容通用 `StrategyBase.run` 路径
- `GMAT3RunResult`
  - 回传 `main_dfs / sub_portfolio_values / value_df_full / value_df / weight_df / schedule / index_series`

当前还支持：
- `raw_root`：切换 GMAT3 原始数据根目录
- `sub_portfolios`：按子集运行，便于测试、调试和 notebook 研究

### G6：正式策略封装

目标：
- 将上述能力沉淀为正式 `GMAT3Strategy`
- 提供 notebook、run script、最小回归测试

---

## 八、Phase G1 的结论

GMAT3 可以作为 `cta_lab` 的第一块复杂策略试金石，但实现方式必须是：

- 以 `strategies/implementations/gmat3/` 为策略子系统承载
- 逐层对照 `main_df -> roll_ret -> V_c(t) -> weight_df -> index_series`
- 不从最终 NAV 直接反推

换句话说，G1 的结论不是“GMAT3 已经能在 `cta_lab` 中跑通”，而是：

**我们已经明确了 GMAT3 的业务边界、关键中间对象、旧实现到新框架的落点关系，后续可以开始按 G2-G6 有序落地。**
