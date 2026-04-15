# How to Get Data from QAD Database

> 基于 Refinitiv QA Direct / Datastream 数据库
> 文档版本：v1.0
> 创建日期：2026-03-09

---

## 一、数据库概览

QAD 数据库（QA Direct）提供 Refinitiv Datastream 的结构化历史数据，通过 SQL 查询访问。

### 1.1 期货相关核心表

| 表名 | 类型 | 用途 |
|------|------|------|
| `DSFutContr` | 维度表 | 期货合约品种总表，含品种名称和交易所 |
| `DSFutClass` | 维度表 | 合约类别表，连接品种与具体合约序列 |
| `DSFutContrInfo` | 维度表 | **单合约静态信息**，含到期日、货币、状态 |
| `DSFutContrVal` | 行情表 | **单合约每日行情**，含结算价、持仓量、成交量 |
| `DSFutTrdCycle` | 维度表 | 各品种合约交易月份（如季月/月月）|
| `DSFutCalcSerInfo` | 维度表 | Datastream 自建连续序列信息 |
| `DSFutCalcSerVal` | 行情表 | Datastream 自建连续序列行情 |

### 1.2 汇率相关核心表

| 表名 | 类型 | 用途 |
|------|------|------|
| `DS2FxCode` | 维度表 | 汇率合约信息，含货币对代码 |
| `DS2FxRate` | 行情表 | 每日汇率数据（MidRate / BidRate / OfferRate）|

### 1.3 表关系示意

```
DSFutContr (品种)
    └── DSFutClass (类别，含交易平台)
            └── DSFutContrInfo (单合约静态，含 FutCode)
                    └── DSFutContrVal (单合约日行情)

DSFutClass ←→ DSFutTrdCycle (交易月份)
DSFutClass ←→ DSFutCalcSerInfo (连续序列)
                    └── DSFutCalcSerVal (连续序列日行情)

DS2FxCode (汇率合约)
    └── DS2FxRate (汇率日行情)
```

### 1.4 关键字段说明

| 字段 | 所在表 | 含义 |
|------|--------|------|
| `ContrCode` | DSFutContr / DSFutClass | 品种级别 ID |
| `ClsCode` | DSFutClass / DSFutContrInfo | 类别级别 ID（同品种可有多个平台）|
| `FutCode` | DSFutContrInfo / DSFutContrVal | **单合约 ID**，行情查询主键 |
| `DSMnem` | DSFutContrInfo | Datastream 合约助记符，格式 `{品种}{MMYY}`，如 `CEN0903` |
| `ContrDate` | DSFutContrInfo | 到期年月，MMYY 格式，如 `0903` 表示 2009年3月 |
| `LastTrdDate` | DSFutContrInfo | 最后交易日，展期逻辑关键字段 |
| `TrdStatCode` | DSFutContrInfo / DSFutClass | `A`=活跃，`D`=已退市 |
| `Settlement` | DSFutContrVal | **结算价**，策略计算核心字段 |
| `OpenInterest` | DSFutContrVal | **持仓量**，主力合约判断关键字段 |

---

## 二、操作步骤：从品种到行情

### Step 1：通过品种名找 ContrCode

当不知道品种代码时，通过关键词搜索：

```sql
-- 方式A：通过 DSContrID（品种标准代码）精确查找
SELECT ContrCode, DSContrID, ContrName, SrcCode
FROM DSFutContr
WHERE DSContrID IN ('LCO', 'CL', 'GC')

-- 方式B：通过 ContrName 模糊搜索（推荐，更可靠）
SELECT ContrCode, DSContrID, ContrName, SrcCode
FROM DSFutContr
WHERE ContrName LIKE '%S&P 500%'
   OR ContrName LIKE '%NASDAQ 100%'

-- 方式C：通过 SrcCode 过滤交易所后浏览（先查交易所代码）
SELECT DISTINCT Code, Desc_
FROM DSFutCode
WHERE Type_ = 1
  AND Desc_ LIKE '%CME%'
```

> **注意**：同一品种可能有多条记录（不同交易平台或历史版本），需结合
> Step 2 的合约数量和历史起点进一步甄别。

---

### Step 2：确认候选品种，选出主流系列

同一经济含义的品种（如10年美债）可能存在多个 ContrCode，
通过合约数量和历史覆盖度来判断哪个是主流：

```sql
SELECT cls.ContrCode, c.DSContrID, c.ContrName,
       COUNT(ci.FutCode)    AS contract_count,
       MIN(ci.StartDate)    AS earliest_date,
       MAX(ci.LastTrdDate)  AS latest_expiry
FROM DSFutContrInfo ci
JOIN DSFutClass cls ON ci.ClsCode = cls.ClsCode
JOIN DSFutContr  c  ON cls.ContrCode = c.ContrCode
WHERE cls.ContrCode IN (417, 458, 334, 452)   -- 候选 ContrCode 列表
GROUP BY cls.ContrCode, c.DSContrID, c.ContrName
ORDER BY c.DSContrID, contract_count DESC
```

**判断标准：**
- 合约数越多、历史起点越早 → 主流系列
- `latest_expiry` 在当前日期之后 → 仍活跃
- `latest_expiry` 停在某年份 → 已切换平台或退市，选更新的系列

---

### Step 3：获取合约静态信息（含到期日）

确定 ContrCode 后，取该品种所有历史合约的静态信息：

```sql
SELECT ci.FutCode,
       ci.DSMnem,
       ci.ContrDate,        -- MMYY 格式的到期年月
       ci.StartDate,        -- Datastream 数据起始日
       ci.LastTrdDate,      -- 最后交易日（展期逻辑关键）
       ci.SttlmntDate,      -- 结算日
       ci.ISOCurrCode,      -- 计价货币
       ci.TrdStatCode,      -- A=活跃 / D=已退市
       ci.LDB,              -- 数据库分类：FUT/LIF/COM/CIE
       cls.ContrCode,
       c.DSContrID
FROM DSFutContrInfo ci
JOIN DSFutClass cls ON ci.ClsCode = cls.ClsCode
JOIN DSFutContr  c  ON cls.ContrCode = c.ContrCode
WHERE cls.ContrCode IN (1381, 323, 463, 452, 458, 2639)  -- 目标品种
  AND ci.StartDate >= '2004-01-01'
ORDER BY cls.ContrCode, ci.LastTrdDate
```

**ContrDate 解析说明：**

| DSMnem | ContrDate | 含义 |
|--------|-----------|------|
| `CEN0903` | `0903` | 2009年3月到期 |
| `CEN1203` | `1203` | 2012年3月到期 |
| `LCO0227` | `0227` | 2027年2月到期 |

解析方式：
```python
# ContrDate 为 MMYY 格式字符串
def parse_contr_date(contr_date: str) -> pd.Timestamp:
    mm = contr_date[:2]
    yy = contr_date[2:]
    # 判断世纪：yy >= 80 认为是 1900s，否则 2000s
    century = "19" if int(yy) >= 80 else "20"
    return pd.Timestamp(f"{century}{yy}-{mm}-01")
```

---

### Step 4：获取单合约每日行情

用 Step 3 拿到的 `FutCode` 列表查询日行情：

```sql
SELECT v.FutCode,
       v.Date_          AS trade_date,
       v.Open_          AS open_price,
       v.High           AS high_price,
       v.Low            AS low_price,
       v.Settlement     AS settle_price,   -- 结算价（核心字段）
       v.OpenInterest   AS open_interest,  -- 持仓量（主力合约判断）
       v.Volume         AS volume
FROM DSFutContrVal v
WHERE v.FutCode IN (95463, 225641, 264075)  -- FutCode 列表
  AND v.Date_ BETWEEN '2004-01-01' AND '2026-03-09'
ORDER BY v.FutCode, v.Date_
```

> **注意**：合约上市首日（= StartDate）通常 OpenInterest=0、Volume=0，
> 属正常现象，在策略计算中需跳过或做保护。

**大批量查询建议（FutCode 超过100个时分批）：**

```python
def fetch_in_batches(fut_codes, batch_size=500):
    all_dfs = []
    for i in range(0, len(fut_codes), batch_size):
        batch = fut_codes[i:i+batch_size]
        codes_str = ", ".join(str(c) for c in batch)
        sql = f"""
            SELECT v.FutCode, v.Date_, v.Settlement,
                   v.OpenInterest, v.Volume
            FROM DSFutContrVal v
            WHERE v.FutCode IN ({codes_str})
              AND v.Date_ >= '2004-01-01'
        """
        all_dfs.append(QAD.download_as_df(sql))
    return pd.concat(all_dfs, ignore_index=True)
```

---

### Step 5：查询合约交易月份

判断某品种是季月合约还是月月合约：

```sql
SELECT tc.ClsCode, tc.TrdMth
FROM DSFutTrdCycle tc
JOIN DSFutClass cls ON tc.ClsCode = cls.ClsCode
WHERE cls.ContrCode = 323   -- NQ 的 ContrCode
ORDER BY tc.TrdMth
```

| TrdMth | 含义 |
|--------|------|
| `MAR`, `JUN`, `SEP`, `DEC` | 季月合约（股指、国债常见）|
| `ALL` | 每月均有合约（商品期货常见）|

---

## 三、操作步骤：获取汇率数据

### 获取 USD/CNY 汇率

```sql
-- 先找到 USD/CNY 的 ExRateIntCode
SELECT ExRateIntCode, ExRateCode, FromCurrCode, ToCurrCode, ExRateDesc
FROM DS2FxCode
WHERE FromCurrCode = 'USD' AND ToCurrCode = 'CNY'

-- 再查日行情
SELECT r.ExRateDate  AS date_,
       r.MidRate     AS usdcny_mid,
       r.BidRate     AS usdcny_bid,
       r.OfferRate   AS usdcny_offer
FROM DS2FxRate r
JOIN DS2FxCode c ON r.ExRateIntCode = c.ExRateIntCode
WHERE c.FromCurrCode = 'USD'
  AND c.ToCurrCode   = 'CNY'
  AND r.ExRateDate  >= '2004-01-01'
ORDER BY r.ExRateDate
```

> ⚠️ **重要：汇率存储方向**
>
> QAD 中 `FromCurrCode='USD', ToCurrCode='CNY'` 对应的
> `MidRate` 存储的是 **每1 CNY 折合多少 USD**（约 0.14~0.17），
> 并非通常理解的"每1 USD 折合多少 CNY"（约 6~8）。
>
> 使用时需取倒数：
> ```python
> fx["usdcny"] = 1 / fx["usdcny_mid"]   # 转为每1 USD=多少 CNY
> ```

---

## 四、Datastream 连续序列（备用方案）

如果不需要自定义展期逻辑，可直接使用 Datastream 预先计算好的连续序列：

```sql
-- 查找某品种的连续序列
SELECT csi.CalcSeriesCode, csi.DSMnem, csi.CalcSeriesName,
       csi.RollMethodCode, csi.PositionFwdCode
FROM DSFutCalcSerInfo csi
JOIN DSFutClass cls ON csi.ClsCode = cls.ClsCode
WHERE cls.ContrCode = 323    -- NQ 的 ContrCode

-- 查询连续序列日行情
SELECT csv.CalcSeriesCode, csv.Date_,
       csv.Open_, csv.High, csv.Low,
       csv.Settlement, csv.OpenInterest, csv.Volume
FROM DSFutCalcSerVal csv
WHERE csv.CalcSeriesCode = 12345   -- 对应的 CalcSeriesCode
  AND csv.Date_ >= '2004-01-01'
ORDER BY csv.Date_
```

> **说明**：GMAT3 策略使用自定义展期规则（5套），与 Datastream 标准展期不同，
> 因此策略计算**不使用**连续序列，只使用单合约原始数据（DSFutContrVal）。
> 连续序列可作为验证参考。

---

## 五、Python 调用模板

```python
import pandas as pd

# ── 假设已有 QAD 模块 ─────────────────────────────────────
# QAD.download_as_df(sql) 返回 pandas DataFrame

# 1. 查找品种
def find_contract(keyword: str) -> pd.DataFrame:
    sql = f"""
        SELECT ContrCode, DSContrID, ContrName, SrcCode
        FROM DSFutContr
        WHERE ContrName LIKE '%{keyword}%'
        ORDER BY ContrName
    """
    return QAD.download_as_df(sql)

# 2. 获取合约静态信息
def get_contract_info(contr_codes: list, start_date: str = "2004-01-01") -> pd.DataFrame:
    codes_str = ", ".join(str(c) for c in contr_codes)
    sql = f"""
        SELECT ci.FutCode, ci.DSMnem, ci.ContrDate,
               ci.StartDate, ci.LastTrdDate, ci.ISOCurrCode,
               ci.TrdStatCode, cls.ContrCode, c.DSContrID
        FROM DSFutContrInfo ci
        JOIN DSFutClass cls ON ci.ClsCode = cls.ClsCode
        JOIN DSFutContr  c  ON cls.ContrCode = c.ContrCode
        WHERE cls.ContrCode IN ({codes_str})
          AND ci.StartDate >= '{start_date}'
        ORDER BY cls.ContrCode, ci.LastTrdDate
    """
    df = QAD.download_as_df(sql)
    df["StartDate"]   = pd.to_datetime(df["StartDate"])
    df["LastTrdDate"] = pd.to_datetime(df["LastTrdDate"])
    return df

# 3. 获取日行情（自动分批）
def get_daily_prices(fut_codes: list, start_date: str = "2004-01-01",
                     batch_size: int = 500) -> pd.DataFrame:
    all_dfs = []
    for i in range(0, len(fut_codes), batch_size):
        batch = fut_codes[i:i+batch_size]
        codes_str = ", ".join(str(c) for c in batch)
        sql = f"""
            SELECT v.FutCode, v.Date_ AS trade_date,
                   v.Settlement  AS settle_price,
                   v.OpenInterest AS open_interest,
                   v.Volume      AS volume
            FROM DSFutContrVal v
            WHERE v.FutCode IN ({codes_str})
              AND v.Date_ >= '{start_date}'
            ORDER BY v.FutCode, v.Date_
        """
        all_dfs.append(QAD.download_as_df(sql))
    df = pd.concat(all_dfs, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    # 去除上市首日无效数据
    df = df[~((df["open_interest"] == 0) & (df["volume"] == 0))]
    return df

# 4. 获取 USD/CNY 汇率
def get_fx_usdcny(start_date: str = "2004-01-01") -> pd.DataFrame:
    sql = f"""
        SELECT r.ExRateDate AS date_,
               r.MidRate    AS usdcny_mid_raw
        FROM DS2FxRate r
        JOIN DS2FxCode c ON r.ExRateIntCode = c.ExRateIntCode
        WHERE c.FromCurrCode = 'USD'
          AND c.ToCurrCode   = 'CNY'
          AND r.ExRateDate  >= '{start_date}'
        ORDER BY r.ExRateDate
    """
    df = QAD.download_as_df(sql)
    df["date_"] = pd.to_datetime(df["date_"])
    # 注意：MidRate 存储方向为 CNY/USD，取倒数得到 USD/CNY
    df["usdcny"] = 1 / df["usdcny_mid_raw"]
    df["usdcny"] = df["usdcny"].ffill()   # 填充极少量空值
    return df
```

---

## 六、已确认的 GMAT3 境外品种参数

| GMAT3 标的 | ContrCode | DSContrID | 合约类型 | 数据起点 |
|-----------|-----------|-----------|---------|---------|
| ES（E-mini S&P 500） | 1381 | ISM | 季月（Mar/Jun/Sep/Dec） | 2004-03-19 |
| NQ（E-mini Nasdaq-100） | 323 | CEN | 季月（Mar/Jun/Sep/Dec） | 2004-03-19 |
| TU（2年期美债） | 463 | CZT | 季月 | 2004-03-30 |
| FV（5年期美债） | 452 | CZF | 季月 | 2004-01-02 |
| TY（10年期美债） | 458 | CZN | 季月 | 2004-03-24 |
| LCO（Brent原油） | 2639 | LCO | 月月（每月均有） | 2007-09-18 |

---

## 七、常见问题

**Q：持仓量大量为零，数据有问题吗？**
A：正常现象。期货市场中非主力合约（远月合约）在大多数时候 OI 极低甚至为零，
只有临近主力切换时才会快速积累。策略逻辑会自动处理。

**Q：同一品种有多个 ContrCode，选哪个？**
A：优先选 `latest_expiry` 在当前日期之后（仍活跃）的系列，且合约数量多、
历史起点早的。如有新旧两个系列，取新系列（`StartDate` 更近但 `latest_expiry` 更远）。

**Q：为什么 DSMnem 里年份 `09` 可能是 2009 年也可能是 1909 年？**
A：使用以下规则区分：`YY >= 80` 认为是 1900s，`YY < 80` 认为是 2000s。
期货合约一般不会跨越这个边界出现歧义。

**Q：连续序列（DSFutCalcSerVal）和单合约（DSFutContrVal）有什么区别？**
A：连续序列是 Datastream 按照其自定义展期方法拼接的价格序列，便于直接使用，
但无法控制展期时点和方式。单合约数据是原始数据，灵活性高，需要自行实现展期逻辑。
GMAT3 策略使用单合约数据。
