# cta_lab data 数据层设计说明与使用指南

## 一、整体设计思路

### 为什么要有一个独立的数据层？

策略研究的核心问题是"信号有没有效"，而不是"怎么读文件"。但在实际工程中，读数据往往混在策略逻辑里：某处直接 `pd.read_parquet(...)`，某处手工做列名映射，某处又把日期处理写一遍。一旦数据格式变了，或要切换数据源，改动会散布在各个角落。

data 层的目标是：**把"从哪里读、读到什么格式"和"用数据做什么"彻底分开**。策略层、分析层只跟领域对象（`BarSeries`、`ContinuousSeries`…）打交道，不关心底下是 parquet、CSV 还是数据库。

### 三层结构

```
┌──────────────────────────────────────────────────────┐
│  策略层 / 分析层                                       │
│  使用 BarSeries、ContinuousSeries、TradingCalendar 等  │
└──────────────────────┬───────────────────────────────┘
                       │ 只看领域对象，不看文件
┌──────────────────────▼───────────────────────────────┐
│  DataLoader（loader.py）                              │
│  · 唯一入口：将 DataFrame 转换为领域对象               │
│  · 管理 Schema 映射（列名翻译）                        │
│  · 内置缓存，同一请求不重复 IO                         │
└───────────┬──────────────────────┬────────────────────┘
            │ 原始 DataFrame        │ 原始 DataFrame
┌───────────▼──────┐    ┌──────────▼────────────────────┐
│ DataSource 适配层 │    │ DataSource 适配层               │
│ (sources/)       │    │ (sources/)                     │
│                  │    │                                 │
│ ParquetSource    │    │ ColumnKeyedSource               │
│ CSVSource        │    │ SQLiteSource …                  │
└───────────┬──────┘    └──────────┬────────────────────┘
            │                      │
┌───────────▼──────────────────────▼────────────────────┐
│  磁盘 / 数据库                                         │
│  market_data/kline/     market_data/contracts/         │
│  market_data/calendar/  …                              │
└───────────────────────────────────────────────────────┘
```

---

## 二、核心组件详解

### 2.1 DataSource 适配层（`data/sources/`）

DataSource 是一个**抽象接口**，只定义四个方法：

```python
read_dataframe(key, **kwargs) -> pd.DataFrame
write_dataframe(key, df, **kwargs) -> None
list_keys(prefix="") -> list[str]
exists(key) -> bool
```

每个具体实现负责与一种存储格式对话，返回 **原始 DataFrame**，不做任何业务处理。

| 实现类 | 访问模式 | 典型用途 |
|--------|---------|---------|
| `ParquetSource(root_dir)` | 每个 key 对应 `root_dir/{key}.parquet` | K 线数据（每个品种一个文件） |
| `CSVSource(root_dir)` | 每个 key 对应 `root_dir/{key}.csv` | 已拆分的 CSV 文件目录 |
| `ColumnKeyedSource(file_path, filter_col)` | 单一大表文件，按列值过滤 | 合约信息（`fut_code` 列）、日历（`exchange` 列） |
| `SQLiteSource` | 单一 .db 文件，按表名索引 | 嵌入式数据库场景 |

**`ColumnKeyedSource` 是专为"大表"设计的**：所有合约信息放在一张 CSV 里，`read_dataframe("RB")` 返回 `fut_code == "RB"` 的所有行；大表在首次访问时懒加载进内存，后续过滤不再读磁盘。

### 2.2 领域模型层（`data/model/`）

这是数据层的"语言"——策略代码看到的全是这些对象，而非裸 DataFrame。

#### `Bar` / `BarSeries`

最基础的数据单元。`BarSeries` 封装了单合约的 K 线序列，约定 **`settle`（结算价）为主价格**，因为期货盈亏以结算价计算。

```
BarSeries
├── symbol: str                   合约代码，如 "RB2410.SHF"
├── data: DataFrame               index=DatetimeIndex
│   ├── open / high / low / close
│   ├── settle                    ← 主价格
│   ├── volume
│   └── open_interest
└── 分析方法
    ├── log_returns()             对数日收益率
    ├── pct_returns()             百分比收益率
    ├── ewm_vol(halflife)         EWM 年化波动率
    ├── rolling_vol(window)       滚动年化波动率
    └── drawdown()                水下回撤序列 [-1, 0]
```

`BarSeries` 支持日期切片：`bs["2023-01-01":"2024-01-01"]` 返回新的 `BarSeries`。

#### `Contract`

单个合约实例的生命周期描述：

```
Contract
├── symbol: str          品种代码 "RB"
├── code: str            合约代码 "RB2410.SHF"（带交易所后缀）
├── exchange: str        "SHFE"
├── list_date: date      上市日
├── expire_date: date    到期日
└── last_trade_date: date 最后交易日
```

两个核心方法：
- `is_active(ref_date)` — 合约是否处于 `[list_date, last_trade_date]` 内
- `days_to_expiry(ref_date)` — 距最后交易日的自然日天数

#### `Instrument`

品种静态属性（不随时间变化的）：合约乘数（`lot_size`）、最小报价单位（`tick_size`）、保证金比例（`margin_rate`）等。通过 `InstrumentRegistry` 单例管理，注册一次，全局可查。

#### `TradingCalendar` / `MultiExchangeCalendar`

交易日历，核心用途：
- `is_trading_day(date)` — 日期判断
- `offset(date, n)` — 交易日偏移（n>0 向后，n<0 向前）
- `next_trading_day(date)` / `prev_trading_day(date)`
- `trading_days_between(start, end)` — 统计交易日数
- `get_dates_in_range(start, end)` — 取范围内所有交易日
- `get_month_end_dates(start, end)` — 取每月最后一个交易日

`MultiExchangeCalendar` 取多个交易所日历的**并集**，适用于跨市场研究。

#### `RollRule` / `ContractSchedule` / `ContinuousSeries`

这三个对象解决期货**连续合约**问题：期货合约有到期日，策略需要跨合约拼成连续的价格序列。

**换仓规则（RollRule）** 是一个抽象接口，每天从活跃候选合约中选一个持有。开箱即用的实现：

| 实现 | 逻辑 |
|------|------|
| `OIMaxRoll` | 持有当日持仓量最大的合约（市场最活跃） |
| `VolumeMaxRoll` | 持有当日成交量最大的合约 |
| `CalendarRoll(days_before_expiry=5)` | 到期前 N 日强制换到下一个合约 |
| `StabilizedRule(base, stability_days=3)` | 包装任意规则，新合约需连续 N 天领先才确认切换，避免噪声触发来回跳动 |

`StabilizedRule` 是**装饰器**，不是独立规则——它包装基础规则并在其上加稳定性过滤层：
```python
rule = StabilizedRule(OIMaxRoll(), stability_days=3)
```

**换仓时间表（ContractSchedule）** 记录每次换仓的日期、换入换出合约，并提供 `get_active_contract(date)` 查询任意日应持有哪个合约。

**连续序列（ContinuousSeries）** 将多个合约拼成单一价格序列，支持四种价格跳跃消除方式：

| AdjustMethod | 说明 | 适用场景 |
|-------------|------|---------|
| `NONE` | 不处理，原始拼接 | 调试、查看原始价格跳跃 |
| `NAV` | Buy-and-Roll 净值指数，各合约独立计算收益率后累乘 | **推荐**：策略研究首选，换仓基差不计入损益 |
| `RATIO` | 按价格比例向历史调整，保持最新合约价格不变 | 需要绝对价格水平时 |
| `ADD` | 按价差向历史平移，保持最新合约价格不变 | 价差稳定的品种；注意历史价格可能出现负值 |

**NAV 方法的核心算法**（与 cta/module1_data.py 一致）：
- 用宽表对每个合约独立计算 `pct_change()`，换仓基差永不出现在收益序列中
- `shift(1)` 延迟：换仓确认当日收益仍来自旧合约，次日才切换
- 支持 `transition_days > 1` 线性分天移仓（参见 3.6 节示例）

`ContinuousSeries` 暴露的接口与 `BarSeries` 对齐：同样支持日期切片、`log_returns()`、`ewm_vol()` 等分析方法，通过 `.prices` 属性取出底层 `pd.Series`。

补充说明：

- 对 `KlineSchema.tushare()` 这类“一个品种一个混合大表”的路径，`load_continuous()`
  现在会优先走“单次读取品种 parquet -> 一次性拆分所需合约 `BarSeries`”的内部路径，
  避免按合约重复读取同一个品种文件。
- 对 `OIMaxRoll` 与 `StabilizedRule(OIMaxRoll)` 这类最常见的主力切换规则，
  `ContinuousSeries.build()` 内部会优先走宽表化 + 向量化的 fast path：
  先构造 `open_interest` / `settle` pivot，再批量选出逐日主力与拼接价格，
  notebook 研究场景下明显比逐日 Python 循环更快。
- 这类优化不会改变外部 API，但对 notebook 研究场景的连续价格构建速度更友好。

### 2.3 DataLoader（`loader.py`）

DataLoader 是**唯一对外入口**，做三件事：

1. **持有数据源引用**：`kline_source`、`contract_source`、`calendar_source` 可以分别配置，互相独立
2. **Schema 映射**：通过 `KlineSchema` 和 `ContractSchema` 描述原始数据的列名，内部自动规范化为领域对象所需的标准列名
3. **内置缓存**：同一个 `(方法, 参数)` 组合只读一次磁盘，后续直接从内存返回

如果没有显式注入 `contract_source` / `calendar_source`，`DataLoader` 会按约定路径回退到
同一个 `kline_source` 下查找：

- `contracts/{symbol}`
- `instruments/{symbol}`
- `calendars/{exchange}`

#### KlineSchema 预置

```python
KlineSchema.default()    # 标准列名（data 已预处理）
KlineSchema.tushare()    # Tushare 原始格式：trade_date, contract_code,
                         # open_price, high_price, … , interest
```

#### ContractSchema 预置

```python
ContractSchema.default() # 标准列名
ContractSchema.tushare() # Tushare 格式：ts_code(合约代码含后缀),
                         # exchange, list_date, delist_date, last_ddate
```

---

## 三、使用指南（Jupyter 版）

### 3.1 初始化 DataLoader

```python
from pathlib import Path
from data.loader import DataLoader, KlineSchema, ContractSchema, InstrumentSchema
from data.sources import ParquetSource, ColumnKeyedSource

MARKET_DATA = Path("/home/ubuntu/dengl/my_projects/market_data")

loader = DataLoader(
    kline_source=ParquetSource(MARKET_DATA / "kline" / "china_daily_full"),
    contract_source=ColumnKeyedSource(
        MARKET_DATA / "contracts" / "china" / "contract_info.parquet",
        filter_col="fut_code",          # parquet 中标识品种的列
    ),
    calendar_source=ColumnKeyedSource(
        MARKET_DATA / "calendar" / "china_trading_calendar.parquet",
        filter_col="exchange",          # parquet 中标识交易所的列
    ),
    instrument_source=ColumnKeyedSource(
        MARKET_DATA / "contracts" / "china" / "contract_info.parquet",
        filter_col="fut_code",          # 第一版 instrument 主数据可从 contract_info 提取
    ),
    kline_schema=KlineSchema.tushare(),
    contract_schema=ContractSchema.tushare(),
    instrument_schema=InstrumentSchema.china_from_contracts(),
)
```

> `contract_source`、`calendar_source`、`instrument_source` 都是可选的。
> 其中 `instrument_source` 优先级最高；若未显式传入，`load_instrument()` 会优先回退到
> `instruments/{symbol}`，再回退到 `contract_source`（如已配置）。
> 只做行情分析时，只传 `kline_source` 即可。

---

### 3.2 加载单合约 K 线（BarSeries）

```python
# 合约代码需与 kline parquet 中 contract_code 列的格式一致
# tushare 格式：带交易所后缀，如 "RB2410.SHF"
bs = loader.load_bar_series("RB2410.SHF")
print(bs)
# BarSeries(symbol='RB2410.SHF', rows=241, range=[2023-11-01 ~ 2024-10-14])

# 查看原始数据
bs.data.head()

# 日期切片（返回新的 BarSeries）
bs_2024 = bs["2024-01-01":"2024-06-30"]

# 分析方法
bs.log_returns()          # 对数日收益率
bs.ewm_vol(halflife=60)   # EWM 年化波动率
bs.rolling_vol(window=20) # 滚动年化波动率
bs.drawdown()             # 水下回撤序列
```

---

### 3.3 查询合约信息（Contract）

```python
# 加载某品种的全部合约
contracts = loader.load_contracts("RB")
print(f"共 {len(contracts)} 个合约")
print(contracts[0])
# Contract(symbol='RB', code='RB2701.SHF', exchange='SHFE',
#          list_date=2026-01-16, expire_date=2027-01-15, last_trade_date=2027-01-19)

# 查询某日期活跃合约
from datetime import date
active = loader.load_contracts("RB", active_only=True, ref_date="2024-01-15")
print([c.code for c in active])
# ['RB2402.SHF', 'RB2401.SHF', ...]

# 合约方法
c = active[0]
c.is_active(date(2024, 1, 15))   # True/False
c.days_to_expiry(date(2024, 1, 15))  # 距最后交易日天数
c.month_code()                   # "2402"
```

---

### 3.4 加载品种静态信息（Instrument）

```python
# 标准 instrument 文件：instruments/RB.parquet
inst = loader.load_instrument("RB")
print(inst)
# Instrument(symbol='RB', name='螺纹钢', exchange='SHFE',
#            currency='CNY', lot_size=10.0, tick_size=1.0, margin_rate=0.1)

# 从全局注册表查询（已 load_instrument 的品种自动注册）
from data.model import InstrumentRegistry
reg = InstrumentRegistry()
reg.get("RB")
reg.list_all()
reg.list_by_exchange("SHFE")
```

如果项目里还没有独立的 instrument 主数据文件，也可以用第一版 `instrument_source`
直接从 contract 元数据提取：

```python
loader = DataLoader(
    kline_source=ParquetSource(MARKET_DATA / "kline" / "china_daily_full"),
    instrument_source=ColumnKeyedSource(
        MARKET_DATA / "contracts" / "china" / "contract_info.parquet",
        filter_col="fut_code",
    ),
    kline_schema=KlineSchema.tushare(),
    instrument_schema=InstrumentSchema.china_from_contracts(),
)

inst = loader.load_instrument("RB")
inst.lot_size    # 来自 per_unit
inst.currency    # 默认 "CNY"
```

`InstrumentSchema` 当前内置三种预置：

- `InstrumentSchema.default()`：标准 instrument 文件格式
- `InstrumentSchema.china_from_contracts()`：从国内 `contract_info.parquet` 提取
- `InstrumentSchema.overseas_from_contracts()`：从海外 `contract_info.parquet` 提取

> 注：第一版 `instrument_source` 仍允许字段部分缺失。
> 对缺失的 `currency` / `tick_size` / `margin_rate`，会使用 schema 默认值。

---

### 3.5 加载交易日历（TradingCalendar）

```python
# exchange 参数需与 calendar parquet 中 exchange 列的值一致
# china_trading_calendar.parquet 中使用 "SHF"（不是 "SHFE"）
cal = loader.load_calendar("SHF")
print(f"{len(cal._dates)} 个交易日")

# 常用查询
cal.is_trading_day("2024-01-02")          # True
cal.is_trading_day("2024-01-01")          # False（元旦）
cal.next_trading_day("2024-01-01")        # Timestamp('2024-01-02')
cal.prev_trading_day("2024-01-02")        # Timestamp('2023-12-29')
cal.offset("2024-01-02", 5)              # 后移 5 个交易日
cal.offset("2024-01-10", -3)             # 前移 3 个交易日
cal.trading_days_between("2024-01-01", "2024-12-31")  # 约 243
cal.get_dates_in_range("2024-01-01", "2024-01-31")    # 1月所有交易日
cal.get_month_end_dates("2024-01-01", "2024-12-31")   # 每月最后一个交易日

# 多交易所合并日历
# cal_multi = loader.load_multi_calendar(["SHF", "DCE", "CFF"])
```

---

### 3.6 构建连续合约（ContinuousSeries）

#### 基础用法

```python
# 最简调用：OIMaxRoll + NAV（默认）
cs = loader.load_continuous("RB")
print(cs)
# ContinuousSeries(symbol='RB', rows=4130, range=[2009-03-27 ~ 2026-03-27])

# NAV 输出模式
cs_price = loader.load_continuous("RB", adjust="nav", nav_output="price")
cs_norm  = loader.load_continuous("RB", adjust="nav", nav_output="normalized")
cs_price.prices.iloc[0]   # 首日原始价格
cs_norm.prices.iloc[0]    # 1.0

# 指定调整方式
cs_ratio = loader.load_continuous("RB", adjust="ratio")
cs_none  = loader.load_continuous("RB", adjust="none")
cs_add   = loader.load_continuous("RB", adjust="add")

# 截取时间段（bar_data 全量加载保证换仓正确，日历裁剪控制输出范围）
cs = loader.load_continuous("RB", start="2015-01-01", end="2024-12-31")

# 取价格序列（pd.Series）
cs.prices

# 分析方法与 BarSeries 相同
cs.log_returns()
cs.ewm_vol(60)
cs.drawdown()
```

#### 批量加载连续价格矩阵（研究推荐入口）

当 notebook 需要一次性加载多个品种并做信号研究时，优先使用
`load_continuous_matrix()`，避免在外层循环里重复拼接 `ContinuousSeries`：

```python
price_df = loader.load_continuous_matrix(
    ["RB", "HC", "CU", "AL"],
    start="2018-01-01",
    end="2024-12-31",
    adjust="nav",
    nav_output="price",
    stability_days=3,
)

price_df.tail()
# index: trade_date
# columns: symbol
```

这个接口内部会复用 `load_continuous()` 的缓存与混合大表单次读取优化，适合：
- 批量构造 `price_df`
- 后续重复计算多个 signals / labels / IC
- notebook 中快速比较多个 horizon 或多个因子

`nav_output` 仅在 `adjust="nav"` 时生效：
- `"price"`：输出锚定到首日原始价格的连续价格链（默认）
- `"normalized"`：输出从 `1.0` 开始的标准化净值链，适合做归一化比较

#### 换仓稳定性过滤（stability_days）

```python
# stability_days=3：新合约需连续 3 天保持最高持仓量才确认切换
# 避免两合约持仓量交替时来回跳动，等价于 cta 的 roll_stability_days=3
cs = loader.load_continuous("RB", stability_days=3)

# 对比：无过滤 vs 稳定性过滤的换仓次数
cs_raw    = loader.load_continuous("RB", stability_days=1)
cs_stable = loader.load_continuous("RB", stability_days=3)

print(len(cs_raw.schedule.events))    # 换仓次数更多（含噪声切换）
print(len(cs_stable.schedule.events)) # 换仓次数更少

# 查看换仓记录
cs_stable.schedule.to_series()                              # 以换仓日为 index 的 Series
cs_stable.schedule.get_active_contract(pd.Timestamp("2024-06-01"))  # 当日合约
```

> 预构建 continuous 文件如果自带 `contract` / `active_contract` / `contract_code`
> 列，`load_continuous()` 会自动恢复 `schedule`。如果价格文件不带该列，也可以额外
> 提供 companion 文件 `continuous/{symbol}_{adjust}_schedule.parquet`，其中至少包含
> `to_contract` 列和换仓日期 index（或 `date` 列）。

#### 分天移仓（transition_days，仅 NAV 模式）

```python
# transition_days=3：换仓确认次日起线性分 2 天过渡，第 3 天全切换
# 第 i 日（确认）：100% 旧合约（shift(1) 保证）
# 第 i+1 日：2/3 旧 + 1/3 新
# 第 i+2 日：1/3 旧 + 2/3 新
# 第 i+3 日：100% 新合约
cs = loader.load_continuous("RB", stability_days=3, transition_days=3)
```

#### 直接调用 ContinuousSeries.build()（自定义规则）

当需要 `VolumeMaxRoll`、`CalendarRoll` 或完全自定义规则时，绕过 loader 直接构建：

```python
from data.model import (
    OIMaxRoll, VolumeMaxRoll, CalendarRoll,
    StabilizedRule, ContinuousSeries, AdjustMethod
)

# 示例：成交量规则 + 稳定性过滤 + 分 5 天移仓
bar_data = {c.code: loader.load_bar_series(c.code) for c in loader.load_contracts("RB")}
contracts = loader.load_contracts("RB")
cal = loader.load_calendar("SHF")

rule = StabilizedRule(VolumeMaxRoll(), stability_days=5)
cs = ContinuousSeries.build(
    symbol="RB",
    bar_data=bar_data,
    contracts=contracts,
    roll_rule=rule,
    adjust=AdjustMethod.NAV,
    calendar=cal,
    transition_days=5,
)

# 日历换月规则（到期前 5 个交易日切换）
rule_cal = CalendarRoll(days_before_expiry=5)
cs_cal = ContinuousSeries.build(
    symbol="RB",
    bar_data=bar_data,
    contracts=contracts,
    roll_rule=rule_cal,
    adjust=AdjustMethod.NAV,
    calendar=cal,
)
```

> 补充说明：
> `ContinuousSeries.build()` 目前对 `OIMaxRoll` 和 `StabilizedRule(OIMaxRoll)`
> 已提供向量化 fast path；对 `VolumeMaxRoll`、`CalendarRoll` 或自定义 `RollRule`
> 仍会回退到通用逐日选择逻辑。这保证了扩展性，同时把研究中最常见的主力规则先优化到位。

> **关键设计细节**：
> - `bar_data` 全量加载（不传 start/end），保证 OIMaxRoll 比较各合约持仓量时不受裁剪影响
> - `calendar` 裁剪到 [start, end]，只控制输出范围，不影响换仓决策
> - 首次调用较慢（全量 IO），结果缓存在 loader 内存，同参数二次调用瞬时返回
> - `stability_days=1`（默认）和 `transition_days=1`（默认）退化为最简行为，无额外开销

---

### 3.7 底层：直接使用 DataSource

如需访问原始 DataFrame（不经过 loader 的 schema 转换）：

```python
from data.sources import ParquetSource, ColumnKeyedSource

# 读取品种 K 线大表（原始列名）
src = ParquetSource(MARKET_DATA / "kline" / "china_daily_full")
df_rb = src.read_dataframe("RB")
df_rb.columns  # ['contract_code', 'trade_date', 'open_price', ...]

# 列出所有可用 key（品种）
src.list_keys()  # ['AL', 'AU', 'CU', ...]

# 读取合约大表（过滤某品种）
csrc = ColumnKeyedSource(
    MARKET_DATA / "contracts" / "china_future_basic_info.csv",
    filter_col="fut_code"
)
df_hc = csrc.read_dataframe("HC")  # 沪铜所有合约行
csrc.list_keys()  # ['AL', 'AU', 'CU', 'RB', ...]
csrc.exists("RB")  # True
```

---

## 四、设计边界说明

### data 层负责的事

- 从磁盘/数据库读取数据，转换为领域对象
- 列名映射和格式规范化（Schema 层）
- 内存缓存（DataLoader 生命周期内）
- 连续合约构建逻辑（拼接 + 价格调整）

### data 层**不**负责的事

| 不在此处 | 实际归属 |
|---------|---------|
| 信号计算（动量、均值回归…） | `signals/` 层 |
| 仓位权重生成 | `portfolio/` 层 |
| 回测撮合循环 | `backtest/` 层 |
| 绩效分析（Sharpe、回撤…） | `analysis/metrics.py` |
| 数据下载 / 爬取 | `scripts/` 目录 |
| 数据预处理 / 格式转换 | `scripts/` 目录 |

---

## 五、已知局限与待完善项

| 项目 | 现状 | 计划 |
|------|------|------|
| 海外 K 线 Schema | `overseas_daily_full` 列名（`FutContrID`、`TradeDate`…）与 tushare 不同，暂无预置 Schema | 增加 `KlineSchema.overseas()` |
| `load_bar_matrix` | 依赖预构建的 `continuous/{symbol}.parquet` 文件，尚未生成 | 增加 `scripts/build_continuous.py` |
| 分钟级数据 | `china_minute/` 目录数据格式待确认 | 增加 `KlineSchema.tushare_minute()` |
| `Instrument` 字段完整性 | `tick_size` / `margin_rate` 在 tushare 数据中缺失，目前使用默认值 | 建立品种静态配置文件补充 |
| 海外换仓规则 | `load_continuous` 目前固定用 `OIMaxRoll`，海外品种可能需要不同基础规则 | 增加 `roll_rule` 参数直传 |
