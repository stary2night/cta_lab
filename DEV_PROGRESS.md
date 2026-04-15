# cta_lab 开发进展记录

> 最后更新：2026-04-08

---

## 项目背景

将 `cta`（国内期货趋势策略 TSMOM）和 `ddb`（GMAT3 全球多资产配置）两个项目合并，
升级为通用 CTA 策略研究框架。完整架构设计见 [DESIGN.md](DESIGN.md)。
基于 GMAT3 复现后的平台复盘与通用化升级思路，已另行整理为
[ARCH_REVIEW_ROADMAP.md](ARCH_REVIEW_ROADMAP.md)。

---

## 当前状态：7 层核心框架已全部实现并验证

| 层 | 目录 | 状态 | 验证方式 |
|----|------|------|---------|
| 数据模型层 | `data/model/` | ✅ 完成 | 随机数 + 真实 parquet 数据 |
| 数据访问层 | `data/sources/` + `data/loader.py` | ✅ 完成 | 随机数 + 真实 parquet 数据 |
| 信号层 | `signals/` | ✅ 完成 | 单测 + 真实数据验证 |
| 组合构建层 | `portfolio/` | ✅ 完成 | 单测 + 真实 signal->portfolio 验证 |
| 回测层 | `backtest/` | ✅ 完成 | 随机数验证 |
| 分析层 | `analysis/` | ✅ 完成 | 随机数验证 |
| 策略层 | `strategies/` | ✅ 完成 | 随机数 + 真实数据端到端 |

**端到端真实数据验证**：使用 `cta/FutureData/dayKline/` 中 I/J/JM/M/P 五个品种，
`KlineSchema.tushare()` 对接原始 parquet，构建连续合约 → DomesticTSMOM 回测 → 绩效分析，全链路贯通。

**文档同步工作流**：已新增 [DOC_SYNC.md](DOC_SYNC.md) 和 `scripts/docs_sync_check.py`，
用于在代码修改后自动检查关键 Markdown 文档是否缺失、过期或未覆盖新增能力。
数据层模块文档入口也已规范化为 [data/README.md](data/README.md)。当前 docs sync
规则也已覆盖 `load_continuous_matrix` 这类新增数据层入口，确保批量研究接口改动后文档同步更新。

**Instrument 主数据第一版**：已新增 `instrument_source` + `InstrumentSchema`，
支持从独立 `instruments/{symbol}` 文件加载，也支持先从国内/海外 `contract_info`
大表提取最小可用的品种静态信息。

**数据层性能优化（连续合约路径）**：`load_continuous()` 在 `KlineSchema.tushare()`
 这类混合大表场景下，已改为“单次读取品种 parquet -> 一次性拆分所需合约 `BarSeries`”，
 不再按合约重复读取同一个品种文件。

**数据层性能优化（批量研究入口）**：已新增 `load_continuous_matrix(symbols, ...)`，
用于直接返回 `dates × symbols` 的连续价格矩阵；同时 `ContinuousSeries.build()`
针对 `OIMaxRoll` / `StabilizedRule(OIMaxRoll)` 增加了向量化主力选择 fast path。
在真实 `market_data` 上，6 个品种连续价格矩阵构建耗时已从此前约 `28.54s`
进一步降到约 `7.58s`。

**Signals Phase S1**：已新增 [signals/README.md](signals/README.md)，并完成第一轮
signals 层收口：补独立测试、修复 `RankCombiner` 的缺失值加权逻辑、修复
`PercentileMomentum` 在 ties / 平盘场景下的系统性偏置。

**Signals Phase S2（第一版）**：已新增 `signals/operators/`，提供 `lag`、`smooth`、
`clip`、`zscore`、`rolling_zscore`、`winsorize`、`cross_sectional_rank`、
`normalize_by_abs_sum` 等通用算子，用于 raw signal / raw score 的研究阶段处理。

**analysis/signal 第一版研究框架**：已新增 future return label 与 IC/IR evaluator，
包括 `forward_return`、`forward_log_return`、`build_forward_returns`、
`information_coefficient`、`information_ratio`、`evaluate_signal`，
可直接在 notebook 中用于轻量因子研究。当前 evaluator 已处理“常数截面”场景，
这类日期会返回 `NaN`，避免 notebook 中出现相关系数的 RuntimeWarning。
同时，IC / Rank IC 已改为按日期向量化计算，`evaluate_signal` 也支持复用
预构建的 `future_returns`，更适合 notebook 中批量比较多个信号。

**Portfolio 接口收口**：已新增 [portfolio/README.md](portfolio/README.md)，补充
`signal -> portfolio` 语义约定；新增 `TopBottomSelector` / `ThresholdSelector`
作为截面 score 到仓位意图的桥接；`EqualRiskSizer` / `RiskBudgetSizer` 新增
`signal_mode="direction" | "raw"`；`blend()` 只在子组合自身有效区间内 `ffill`，并将区间外权重显式清零。

**GMAT3 Phase G1**：已在 [strategies/implementations/gmat3/README.md](strategies/implementations/gmat3/README.md)
沉淀第一版“业务逻辑拆解 + `ddb/gmat3` → `cta_lab` 映射表”，明确了
`main_df -> roll_ret -> V_c(t) -> weight_df -> index_series` 这条落地链路，
并将 GMAT3 从单文件策略重构为目录化实现包。

**GMAT3 Phase G2（第一步）**：已新增 `GMAT3DataAccess`，在
`strategies/implementations/gmat3/` 下统一接入 `ddb/raw` 中的交易日历、
国内外合约信息、国内外日线、替代标的与 FX 数据；并补充了针对真实 `ddb/raw`
文件的最小 smoke/regression tests。

**GMAT3 Phase G3（当前阶段）**：已新增 `MainContractEngine`、
`RollReturnCalculator` 与 `SubPortfolioEngine` 第一版实现；当前已打通
单资产子组合 `V_c(t)` 与替代标的切换逻辑，并对 `IF / TF / ES` 等代表性路径完成
和旧 `ddb/gmat3` 的小范围对照测试。`BLACK` 黑色系复合子组合也已补齐，并完成
短窗口对照验证；同时已补上 `value_df_full / value_df` 构建入口，G3 主链基本收口。

**GMAT3 Phase G4（进行中）**：已新增 `signals.py` 并完成 `WeightCalculator`
主干迁移，当前已能基于 `value_df_full + calc_days` 计算 `weight_df`，承接
GMAT3 的动量 / 反转 / 风险信号、4 子指数错峰调仓、风险预算、TVS 惩罚、
WAF 风险缩放与权重上限约束；并已补充小窗口对照测试验证与旧 `ddb` 结果一致。

**GMAT3 Phase G5（已启动）**：已在 `index_builder.py` 中实现第一版
`GMAT3IndexBuilder.compute()`，接通 FX 重估、交易成本、Tracking Fee、
VRS 与最终指数点位合成；并已补充与旧 `ddb/gmat3/index.py` 的小窗口对照测试。

**GMAT3 更大范围真实回归**：已新增 `scripts/gmat3_broad_regression.py`，
可在更大 universe 与更长时间窗口下，对比 `cta_lab` 与旧 `ddb` 的
`value_df / weight_df / index_series`，用于后续 GMAT3 全链路回归。当前已完成
截止 `2016-12-31`、16 个子组合的真实回归，三层结果与旧 `ddb` 一致。

**GMAT3 Phase G6（已启动）**：`strategy.py` 已升级为正式的端到端策略入口，
新增 `GMAT3Strategy.run_pipeline()`、`GMAT3Strategy.run()` 与 `GMAT3RunResult`，
现在可以直接通过策略对象串起 `main_df -> V_c(t) -> value_df -> weight_df -> index_series`
整条流水线，并按 `sub_portfolios` 子集快速运行。

**平台升级复盘文档**：已新增 [ARCH_REVIEW_ROADMAP.md](ARCH_REVIEW_ROADMAP.md)，
用于记录 GMAT3 复现后的平台通用化、模块化与可穿透性升级方向，并已纳入 docs sync 规则。
当前 roadmap 已进一步收敛到 `Roll Strategy`、`signals / portfolio` 通用化与
`Look-Through` 三条主线，不再强制把暂不清晰的 engine 主题放入近期主线路径。

**Roll 组件第一阶段收口**：`strategies/components/roll/` 已形成第一阶段可用能力，
包括 single-asset roll、profile/rule-config 驱动、static/equal/external bundle、
bundle sync schema、GMAT3 `BLACK` 的简洁映射路径，以及与 `signals -> portfolio`
 的 notebook 级衔接示例。当前阶段性状态与边界已整理到
[strategies/components/roll/STATUS.md](strategies/components/roll/STATUS.md)。

---

## 完整目录结构

```
cta_lab/
├── data/
│   ├── model/
│   │   ├── instrument.py      # Instrument dataclass + InstrumentRegistry 单例
│   │   ├── contract.py        # Contract dataclass（days_to_expiry / is_active）
│   │   ├── bar.py             # Bar dataclass + BarSeries（log_returns/ewm_vol/drawdown/切片）
│   │   ├── calendar.py        # TradingCalendar + MultiExchangeCalendar
│   │   ├── roll.py            # RollRule ABC + OIMaxRoll / VolumeMaxRoll / CalendarRoll + ContractSchedule
│   │   └── continuous.py      # ContinuousSeries.build()（NAV/RATIO/ADD/NONE 四种调整方式）
│   ├── sources/
│   │   ├── base.py            # DataSource ABC（read_dataframe/write_dataframe/list_keys/exists）
│   │   ├── parquet_source.py  # ParquetSource（主力，支持 start/end 过滤）
│   │   ├── csv_source.py      # CSVSource
│   │   ├── binary_source.py   # BinarySource（feather / hdf5）
│   │   ├── sqlite_source.py   # SQLiteSource（pandas + sqlite3）
│   │   └── ddb_source.py      # DDBSource（存根，NotImplementedError）
│   └── loader.py              # DataLoader + KlineSchema + ContractSchema + InstrumentSchema
│
├── signals/
│   ├── base.py                # Signal ABC + CrossSectionalSignal ABC
│   ├── momentum/
│   │   ├── tsmom.py           # TSMOM：sign(rolling_sum(log_ret, N))
│   │   ├── sharpe_mom.py      # SharpeMomentum：累计收益 / 年化波动
│   │   ├── abs_mom.py         # AbsoluteMomentum：V(t)/V(t-N) - 1
│   │   └── percentile_mom.py  # PercentileMomentum：历史分位数位置
│   ├── reversal/
│   │   └── mass_reversal.py   # MASS260Reversal：均线结构有序度（向量化 5 MA）
│   ├── risk/
│   │   └── tvs.py             # TVS：Sharpe序列与波动率的滚动相关
│   ├── carry/                 # 目录预留，暂未实现
│   ├── composite/
│       ├── linear_combiner.py # LinearCombiner：skipna 加权平均（时序信号合成）
│       └── rank_combiner.py   # RankCombiner：截面排名加权（GMAT3 多因子选品）
│   └── operators/
│       ├── __init__.py        # operators 导出
│       └── transforms.py      # lag / smooth / clip / zscore / winsorize 等
│
├── portfolio/
│   ├── sizing/
│   │   ├── base.py            # Sizer ABC（仓位意图矩阵 -> 原始权重）
│   │   ├── equal_risk.py      # EqualRiskSizer：支持 direction/raw 两种信号模式
│   │   └── risk_budget.py     # RiskBudgetSizer：支持 direction/raw，支持 TVS 切换
│   ├── constraints/
│   │   ├── weight_cap.py      # WeightCap：品种权重上界（支持全局/per-symbol 配置）
│   │   └── vol_scaler.py      # WAF：组合波动超阈值时缩减整体权重
│   ├── scheduler/
│   │   ├── base.py            # RebalanceRecord dataclass + RebalanceScheduler ABC
│   │   ├── monthly.py         # MonthlyScheduler：月末计算 + lag 日调仓
│   │   └── staggered.py       # StaggeredScheduler：N 子组合错峰（GMAT3 n_sub=4）
│   ├── selectors.py           # TopBottomSelector / ThresholdSelector：score -> 仓位意图
│   ├── blender.py             # blend()：多子组合权重加权融合
│   └── fx_handler.py          # revalue_usd_pnl() / usd_to_cny()
│
├── backtest/
│   ├── engine.py              # BacktestEngine（可插拔 tracker/fees/vrs，lag 参数）
│   ├── position.py            # SimpleTracker（CTA）+ FXTracker（GMAT3 双轨 + FX 重估）
│   ├── result.py              # BacktestResult（nav/returns + verbose: holdings/fee/rebalance log）
│   ├── fees/
│   │   ├── base.py            # FeeModel ABC
│   │   ├── zero.py            # ZeroFee
│   │   ├── trading.py         # TradingFee：rate × Σ|Δw|，调仓日触发
│   │   └── tracking.py        # TrackingFee：annual_rate/252，每日计提
│   └── execution/
│       ├── lag.py             # apply_lag()：weight_df.shift(lag)
│       └── vrs.py             # VRS：max(vol_22/65/130) > threshold 时缩减持仓
│
├── analysis/
│   ├── metrics.py             # performance_summary() / rolling_metrics() / underwater_series()
│   ├── attribution/
│   │   ├── asset.py           # asset_contribution() / annual_contribution()
│   │   └── sector.py          # sector_performance()
│   ├── crisis/
│   │   ├── alpha.py           # crisis_alpha_analysis()（内置 5 个国内危机事件）
│   │   └── convexity.py       # convexity_analysis()（微笑曲线）
│   ├── signal/
│   │   ├── labels.py          # future return / future log return 标签
│   │   ├── evaluator.py       # IC / Rank IC / IR 轻量评估器
│   │   ├── persistence.py     # momentum_persistence()（面板 OLS，numpy lstsq）
│   │   ├── long_short.py      # long_short_asymmetry()（多/空/双向三条 NAV）
│   │   └── correlation.py     # correlation_analysis()（相关矩阵）
│   ├── cost/
│   │   └── fee_decomp.py      # fee_decomposition()（三场景回测对比：零费/交易费/完整费）
│   └── report/
│       ├── charts.py          # 8 个图表函数（返回 Figure，不调用 show()）
│       └── strategy_report.py # StrategyReport：8 图编排器，缺失数据项自动跳过
│
├── strategies/
│   ├── base/
│   │   ├── strategy.py        # StrategyBase ABC（generate_signals/build_weights/run/from_yaml）
│   │   ├── trend.py           # TrendFollowingStrategy：多周期 TSMOM + EqualRiskSizer
│   │   └── cross_sectional.py # CrossSectionalStrategy：截面打分 + 分位数多空切分
│   ├── implementations/
│   │   ├── domestic_tsmom.py  # DomesticTSMOM(TrendFollowingStrategy)
│   │   ├── crossmom.py        # CrossMOM(CrossSectionalStrategy)
│   │   └── gmat3/             # GMAT3Strategy 包：strategy / config / universe / schedule / weights / index_builder
│   ├── roll_research/
│   │   ├── rules.py           # BasisDrivenRoll / CarryOptimizedRoll / MomentumRoll
│   │   └── backtest.py        # compare_roll_strategies()
│   └── configs/
│       ├── domestic_tsmom.yaml
│       ├── crossmom.yaml
│       └── gmat3.yaml
│
├── DESIGN.md                  # 完整架构设计文档（各层设计思路 + 全目录结构总览）
└── DEV_PROGRESS.md            # 本文件
```

---

## 关键设计决策速查

### 数据层

**KlineSchema / ContractSchema**（`data/loader.py`）

DataLoader 接受 Schema 对象，描述实际存储中的列名，规范化后再构造领域对象。
DataSource 只负责读写存储介质，不关心列名格式。

```python
# 对接 cta/FutureData/dayKline/ 原始数据（Tushare 格式）
loader = DataLoader(
    kline_source=ParquetSource("cta/FutureData/dayKline"),
    kline_schema=KlineSchema.tushare(),
)

# 对接 cta_lab 标准格式（已预处理数据）
loader = DataLoader(
    kline_source=ParquetSource("cta_lab/data"),
    kline_schema=KlineSchema.default(),  # 列名：open/high/low/close/settle/volume/open_interest
)
```

预置 Schema：
- `KlineSchema.default()`：标准列名，index 为 DatetimeIndex
- `KlineSchema.tushare()`：`trade_date` 列为日期，`contract_code` 标识合约，`settle_price`/`interest` 等原始列名

**连续合约构建**（`data/model/continuous.py`）

```python
cs = ContinuousSeries.build(
    symbol, bar_data, contracts,
    roll_rule=OIMaxRoll(),        # 或 CalendarRoll(5) / 自定义 RollRule
    adjust=AdjustMethod.NAV,      # NAV（推荐）/ RATIO / ADD / NONE
    calendar=cal,
)
```

### 信号层

所有信号统一输出**连续浮点值**，通过 `.to_direction()` 离散化为 {-1, 0, +1}。

```python
sig = TSMOM(252).compute(prices)          # 时序信号：输入单品种价格 Series
sig_cs = RankCombiner([...]).compute(price_matrix)  # 截面信号：输入品种×日期矩阵
```

### 回测层

两种典型配置：

```python
# CTA
BacktestEngine(SimpleTracker(syms), [ZeroFee()], lag=1)

# GMAT3
BacktestEngine(
    FXTracker(syms, currency_map),
    [TradingFee(0.0005), TrackingFee(0.005)],
    vrs=VRS(threshold=0.045),
    lag=1,
)
```

`adjust_dates` 由调用方从 `RebalanceScheduler` 提取：

```python
sched = MonthlyScheduler().produce_schedule(cal, start, end)
adj_dates = {r.adjust_date for r in sched}
result = engine.run(weight_df, price_df, adj_dates)
```

### 策略层

策略是**组装层**，不重复实现信号或定仓逻辑：

```python
strat = DomesticTSMOM({'lookbacks': [21, 63, 126, 252], 'target_vol': 0.40})
result = strat.run(price_df, adj_dates, engine)

# 或从 YAML 加载参数
strat = DomesticTSMOM.from_yaml("strategies/configs/domestic_tsmom.yaml")
```

---

## 真实数据对接验证结果

**数据来源**：`cta/FutureData/dayKline/`（I/J/JM/M/P 五个 DCE 品种）

**验证链路**：原始 parquet → `KlineSchema.tushare()` → `BarSeries` → `ContinuousSeries.build(OIMaxRoll, NAV)` → `DomesticTSMOM` → `BacktestEngine` → `performance_summary`

| 指标 | 值 | 说明 |
|------|-----|-----|
| 价格矩阵 | 3024 日 × 5 品种 | 2013-10-18 ~ 2026-03-25 |
| 年化收益 | -3.61% | 5 品种集中，无分散化 |
| 年化波动 | 30.50% | 高集中度导致高波动 |
| Sharpe | -0.18 | 正常研究起点，非框架问题 |
| MaxDD | -87.27% | 5 品种全黑色/农产品，高相关 |

绩效差的原因是品种单一、高相关，与框架实现无关。

---

## 待完成事项

### 高优先级

- [ ] **Roll Strategy Layer P2**：继续把 `strategies/components/roll/` 从单资产骨架推进到可复用组件层
- [ ] **bundle roll 第二阶段**：在 `BundleRollStrategy` 已支持静态/等权/外部动态权重与同步展期 schema 的基础上，继续补真正的同步调仓逻辑
- [ ] **utils/ 层**：`date.py`（非交易日工具）/ `logging.py` / `config.py`（YAML 加载）
- [ ] **全品种数据适配**：`cta/FutureData/dayKline_full_period/` 中更多品种（目前 dayKline 目录缺少 RB/HC/CU 等）
- [ ] **连续合约预构建脚本**：`scripts/build_continuous.py`，一次性构建所有品种的连续合约并缓存
- [ ] **真实 ContractSchema 验证**：用 `future_basic_info.csv` + `ContractSchema.tushare()` 通过 `DataLoader.load_contracts()` 加载

### 中优先级

- [ ] **更多品种端到端测试**：扩展到 20+ 品种，覆盖金属/能化/农产品多板块
- [ ] **FXTracker 真实验证**：对接境外品种数据和 FX 汇率序列
- [ ] **StrategyReport 完整输出**：传入真实 weights_df 和 sector_map，生成完整 8 图报告
- [ ] **signals/carry/**：展期收益信号（等 roll_research 成熟后填充）

### 低优先级

- [ ] **tests/ 目录**：补充单元测试，覆盖边界条件
- [ ] **scripts/**：`run_backtest.py` / `generate_report.py` 命令行入口
- [ ] **notebooks/**：数据探索 / 信号研究 Notebook 模板

---

## 已知问题 & 注意事项

1. **Roll Strategy Layer 仍在 P2 阶段**：`SingleAssetRollStrategy` 和 `BundleRollStrategy` 已有最小闭环，
   bundle 当前已支持静态/等权/外部动态权重，并已加入同步展期 schema；
   `BLACK` 也已开始进入结构映射阶段（bundle profile + sync hook），并新增了一条基于
   `compute_gmat3_black_component_target_weights()` 与 `run_gmat3_black_bundle()` 的简洁落地路径；
   该 helper 当前按“至少 125 个交易日历史 + 最近 120 个交易日持仓金额均值”的口径计算年度目标权重；
   但尚未覆盖完整同步调仓逻辑和更通用的动态 bundle weighting 抽象。

1. **CrossSectionalStrategy.generate_signals 性能**：逐日循环，品种数多时较慢。
   可用 `price_df.rank(axis=1, pct=True)` 向量化替代，但逻辑正确性已验证。

2. **BarSeries 构造时要求列完整**：`open/high/low/close/settle/volume/open_interest` 全部必须存在。
   用 `KlineSchema.tushare()` 时 `_normalize_kline` 会自动映射，缺失列不报错但会在 BarSeries 构造时失败。

3. **`dayKline/` 目录缺部分品种**：RB/HC/CU/AL/ZN 等主力品种不在该目录，
   可能在 `dayKline_full_period/` 或其他子目录中，需确认路径后配置 `ParquetSource` 根目录。

4. **VRS 仅对 FXTracker 有意义**：在 `SimpleTracker` 模式下 VRS 即使配置了也不生效，
   `BacktestEngine` 内部已做判断，不会报错。

5. **InstrumentRegistry 是单例**：跨测试共享状态，如需隔离测试用例，需手动清空 `_instruments`。
