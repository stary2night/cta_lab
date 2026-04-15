# Roll Component Layer

这个目录用于承接 `Phase P2` 中的 `Roll Strategy Layer` 设计与实现。

阶段性状态总结见 [STATUS.md](STATUS.md)。

它的定位不是：
- 某个具体策略的实现目录
- 也不是单纯的研究资料目录

而是：

**`cta_lab` 中面向复合策略的通用 Roll Strategy 组件层。**

---

## 这个目录解决什么问题

GMAT3 复现表明，`data.load_continuous()` 并不足以支撑复杂展期策略。

未来平台需要一个更正式的组件层，负责：
- 主力/目标合约选择
- 展期调度与执行
- 展期收益率计算
- 资产级价值序列构造
- 底层合约可穿透输出

当前阶段的明确边界：
- 只处理 `generic contract`
- 不处理 `CU.SHF`、`CU00.SHF`、`CU01.SHF` 这类连续链 / alias contract
- 对 `GMAT3` 旧实现的对照，优先比较业务逻辑与 generic contract 上的 roll rule，不追求兼容所有 legacy chain code

这个目录就是为这件事准备的。

---

## 与其他目录的边界

### 与 `strategy_ideas/roll_strategy/` 的区别

`strategy_ideas/roll_strategy/` 用来放：
- paper
- 报告
- 阅读笔记
- 业务启发材料

这里则用来放：
- 抽象设计草案
- 组件边界定义
- profile 草稿
- selector / executor / composer 的结构设计
- 后续正式实现代码

一句话：
- `strategy_ideas/` 是资料库
- `strategies/components/roll/` 是组件设计与实现区

### 与 `strategies/roll_research/` 的区别

`strategies/roll_research/` 更适合放：
- roll rule 对比研究
- 回测比较
- 实验性规则

这里更偏：
- 平台通用组件
- 供多个策略复用的 Roll Strategy 基础层

---

## 当前建议的组件方向

未来这里可能逐步形成以下核心对象：

- `RollStrategyBase`
- `RollStrategyProfile`
- `RollStrategyResult`
- `MainContractSelector`
- `RollExecutor`
- `ValueComposer`
- `LookThroughResolver`

其中要特别强调：
- 不建议一开始就做很多资产子类
- 更推荐“基类 + profile + 可插拔组件”

例如：
- domestic equity roll profile
- domestic bond roll profile
- overseas futures roll profile
- black composite profile

这些更可能是不同实例配置，而不是不同 class。

---

## 最小上手

当前最常用的两个入口是：

- `SingleAssetRollStrategy`
- `BundleRollStrategy`
- `GMAT3SingleAssetRollStrategy`

### 1. 用真实 GMAT3 数据跑一个单资产 roll asset

```python
from strategies.components.roll import GMAT3SingleAssetRollStrategy
from strategies.implementations.gmat3 import GMAT3DataAccess

access = GMAT3DataAccess()
strategy = GMAT3SingleAssetRollStrategy(access, "CU")
result = strategy.run_from_access(end="2005-12-31")
```

常用输出：

```python
result.value_series.tail()
result.contract_plan.head()
result.roll_schedule.head()
result.lookthrough_view().head()
```

含义：

- `value_series`
  用于上层 signal / allocation 消费的资产级价值序列
- `contract_plan`
  每个交易日目标应持有哪个 generic contract
- `roll_schedule`
  old/new 合约的逐日执行路径
- `lookthrough_book`
  当前最小版本中的底层持仓穿透结果

### 2. 用自定义数据跑一个最小单资产策略

```python
import pandas as pd

from strategies.components.roll import RollStrategyProfile, SingleAssetRollStrategy

contracts = pd.DataFrame({
    "contract_id": ["RB01", "RB02"],
    "last_trade_date": pd.to_datetime(["2026-01-06", "2026-01-10"]),
    "last_holding_date": pd.to_datetime(["2026-01-06", "2026-01-10"]),
})

prices = pd.DataFrame(...)
open_interest = pd.DataFrame(...)

profile = RollStrategyProfile(
    name="rb_demo",
    asset_key="RB",
)

strategy = SingleAssetRollStrategy(profile)
result = strategy.run(
    market_data={
        "contracts": contracts,
        "prices": prices,
        "open_interest": open_interest,
    }
)
```

### 3. 用 profile 驱动构造策略

如果你不想手动组装 rules，可以通过 profile 直接声明 rule config：

```python
from strategies.components.roll import (
    ExecutionRuleConfig,
    LifecycleRuleConfig,
    MarketStateRuleConfig,
    RollStrategyProfile,
    SelectorRuleConfig,
    build_single_asset_strategy_from_profile,
)

profile = RollStrategyProfile(
    name="cu_profile",
    asset_key="CU",
    roll_days=3,
    lifecycle_rule_config=LifecycleRuleConfig(
        kind="fixed_days_before_expiry",
        params={"roll_days": 3, "date_field": "last_holding_date"},
    ),
    market_state_rule_config=MarketStateRuleConfig(
        kind="gmat3_domestic_commodity",
        params={},
    ),
    execution_rule_config=ExecutionRuleConfig(
        kind="linear",
        params={"roll_days": 3},
    ),
    selector_rule_config=SelectorRuleConfig(
        kind="prefer_selected",
        params={},
    ),
)

strategy = build_single_asset_strategy_from_profile(profile)
```

这里的关键点是：
- `LifecycleRuleConfig` 决定什么时候必须/可以 roll
- `MarketStateRuleConfig` 决定怎么在候选合约里选目标
- `ExecutionRuleConfig` 决定 old/new 合约如何过渡
- `SelectorRuleConfig` 决定如何综合 lifecycle 和 market-state 结果

`rule_profile` 目前仍然保留，但已经更接近兼容字段。
优先建议直接写显式 rule config。

### 4. 用 config-only 方式驱动最小策略

如果你不想依赖任何预设 profile 名称，也可以只靠显式 config：

```python
from strategies.components.roll import (
    ExecutionRuleConfig,
    LifecycleRuleConfig,
    MarketStateRuleConfig,
    RollStrategyProfile,
    SelectorRuleConfig,
    build_single_asset_strategy_from_profile,
)

profile = RollStrategyProfile(
    name="rb_config_only",
    asset_key="RB",
    lifecycle_rule_config=LifecycleRuleConfig(
        kind="fixed_days_before_expiry",
        params={"roll_days": 3, "date_field": "last_holding_date"},
    ),
    market_state_rule_config=MarketStateRuleConfig(
        kind="field_max",
        params={"field_name": "open_interest"},
    ),
    execution_rule_config=ExecutionRuleConfig(
        kind="linear",
        params={"roll_days": 3},
    ),
    selector_rule_config=SelectorRuleConfig(
        kind="hybrid",
        params={},
    ),
)

strategy = build_single_asset_strategy_from_profile(profile)
```

### 5. 用预先算好的 component results 构造一个 bundle roll asset

第一版 bundle roll 先支持：
- 静态权重 bundle
- 等权 bundle
- 统一输出 `value_series / component_weights / lookthrough_book`

最简单的方式是把底层单资产结果先算好，再喂给 `BundleRollStrategy`：

```python
import pandas as pd

from strategies.components.roll import (
    BundleRollStrategy,
    BundleRule,
    RollComponentProfile,
    RollStrategyProfile,
    RollStrategyResult,
)

dates = pd.date_range("2026-01-02", periods=4, freq="D")

leg_a = RollStrategyResult(
    value_series=pd.Series([1.0, 1.01, 1.02, 1.03], index=dates),
    lookthrough_book=pd.DataFrame(
        {"trade_date": dates, "contract_id": ["RB2601", "RB2601", "RB2602", "RB2602"], "weight": [1, 1, 1, 1]}
    ),
)
leg_b = RollStrategyResult(
    value_series=pd.Series([1.0, 1.00, 1.01, 1.015], index=dates),
    lookthrough_book=pd.DataFrame(
        {"trade_date": dates, "contract_id": ["HC2601", "HC2601", "HC2602", "HC2602"], "weight": [1, 1, 1, 1]}
    ),
)

profile = RollStrategyProfile(
    name="black_like_bundle",
    asset_key="BLACK_LIKE",
    asset_mode="bundle",
    components=[
        RollComponentProfile(component_key="rb", symbol="RB"),
        RollComponentProfile(component_key="hc", symbol="HC"),
    ],
    bundle_rule=BundleRule(
        weight_mode="static",
        static_weights={"rb": 0.6, "hc": 0.4},
    ),
)

strategy = BundleRollStrategy(profile)
result = strategy.run(
    market_data={"component_results": {"rb": leg_a, "hc": leg_b}}
)
```

当前 bundle 的定位是：
- 先把 bundle 作为正式资产对象立住
- 现在已经支持静态 / 等权 / 外部时间变化权重
- 后面再继续支持更复杂的同步 roll、跨品种组合规则

### 6. 用时间变化的外部权重驱动一个 bundle

如果你已经在别处算好了 bundle 层目标权重，也可以直接喂给 `BundleRollStrategy`：

```python
target_weights = pd.DataFrame(
    {
        "rb": [0.7, 0.7, 0.4, 0.4],
        "hc": [0.3, 0.3, 0.6, 0.6],
    },
    index=dates,
)

profile = RollStrategyProfile(
    name="dynamic_bundle",
    asset_key="RB_HC_DYNAMIC",
    asset_mode="bundle",
    components=[
        RollComponentProfile(component_key="rb", symbol="RB"),
        RollComponentProfile(component_key="hc", symbol="HC"),
    ],
    bundle_rule=BundleRule(
        weight_mode="external",
        rebalance_frequency="monthly",
        smoothing_window=2,
    ),
)

strategy = BundleRollStrategy(profile)
result = strategy.run(
    market_data={
        "component_results": {"rb": leg_a, "hc": leg_b},
        "component_target_weights": target_weights,
    }
)
```

这条路径的意义是：
- `BundleRollStrategy` 先不负责推导所有复杂动态权重逻辑
- 但已经可以消费外部提供的时间变化目标权重
- 很适合作为 `BLACK` 或其他跨资产配置规则的过渡接入点

### 7. bundle 级同步展期 schema

当前还没有把复杂的 bundle 同步展期业务逻辑完全实现，但已经正式支持一版 schema 与结果 hook。

你现在可以在 `BundleRule` 中声明：

- `sync_mode="none" | "rebalance" | "external_dates"`
- `sync_frequency`
- `sync_components`

例如：

```python
bundle_rule = BundleRule(
    weight_mode="external",
    rebalance_frequency="monthly",
    smoothing_window=2,
    sync_mode="rebalance",
    sync_frequency="monthly",
    sync_components=["rb", "hc"],
)
```

当前这版的意义是：
- 在 `roll_schedule` 中正式留下 bundle-level sync 注记
- 让上层能看见“哪些日期、哪些 component 被视为同步调仓点”
- 为后续更复杂的 bundle roll 规则提供正式落点

相关字段会出现在 `result.roll_schedule` 中：

- `bundle_sync_mode`
- `bundle_sync_trigger`
- `bundle_sync_scope`
- `bundle_sync_components`
- `bundle_sync_group`

### 8. 用现有 roll 组件简洁运行 GMAT3 BLACK

如果你已经先算好了 5 个底层单资产 roll result：

- `rb`
- `hc`
- `i`
- `j`
- `jm`

那么现在不需要等完整的 BLACK 专属组件落地，就可以直接用现有 `BundleRollStrategy` 跑一个真实 BLACK bundle。

推荐入口：

```python
from strategies.components.roll import run_gmat3_black_bundle
from strategies.implementations.gmat3 import GMAT3DataAccess

access = GMAT3DataAccess()
black_result = run_gmat3_black_bundle(
    access,
    component_results={
        "rb": rb_result,
        "hc": hc_result,
        "i": i_result,
        "j": j_result,
        "jm": jm_result,
    },
)
```

这条路径会自动完成两件事：

- 按 GMAT3 旧 BLACK 规则，从原始日线里的 `open_interest * settle_price` 计算年度动态目标权重
- 把这些权重喂给当前 `BundleRollStrategy(weight_mode="external")`

如果你只想拿到可直接喂给 bundle 的中间输入，也可以用：

```python
from strategies.components.roll import build_gmat3_black_bundle_market_data

market_data = build_gmat3_black_bundle_market_data(
    access,
    component_results={
        "rb": rb_result,
        "hc": hc_result,
        "i": i_result,
        "j": j_result,
        "jm": jm_result,
    },
)
```

`market_data` 里会包含：

- `component_results`
- `component_target_weights`
- `bundle_sync_dates`

这让你可以在不扩大 bundle 抽象复杂度的前提下，先把 BLACK 的真实动态权重需求接进现有组件层。

### 9. GMAT3 BLACK 的当前映射进度

`BLACK` 当前还没有完整迁入 roll layer，但结构性业务逻辑已经开始映射：

- 组件构成：`RB / HC / I / J / JM`
- 权重模式：`external`
- 年度 rebalance
- 10 日平滑过渡
- `weight_min / weight_max`
- bundle-level `sync_mode="rebalance"`

当前这些结构可以通过 `build_gmat3_black_bundle_profile()` 得到。
这一步的目标不是一次写完 `BLACK`，而是先把真实业务规则正式落到：

- `BundleRule`
- `sync hook`
- bundle profile

后续再把真实动态权重计算逐步接进来。

---

## 当前 profile schema

`RollStrategyProfile` 目前最重要的字段有：

- `name`
- `asset_key`
- `asset_mode`
- `currency`
- `rule_profile`
- `roll_days`
- `lifecycle_date_field`
- `market_state_field`
- `lifecycle_rule_config`
- `market_state_rule_config`
- `execution_rule_config`
- `selector_rule_config`

其中：

- `rule_profile`
  指定使用哪类规则组合
- `roll_days`
  指定展期执行窗口长度
- `lifecycle_date_field`
  指定生命周期判断依据，例如 `last_holding_date`
- `market_state_field`
  默认市场状态字段，例如 `open_interest`
- `lifecycle_rule_config / market_state_rule_config / execution_rule_config`
- `lifecycle_rule_config / market_state_rule_config / execution_rule_config / selector_rule_config`
  更正式的显式规则配置，未来会逐步成为主入口
- `components`
  bundle 资产内部的 roll component 定义
- `bundle_rule`
  bundle 层的权重、同步展期与重平衡规则，当前支持 `static`、`equal`、`external`

这部分 schema 还会继续收紧，但现在已经建议：
- 高频字段不要继续塞进 `metadata`
- 新代码优先使用显式 rule config

---

## 子目录说明

### `ideas/`

放更偏概念和抽象的想法，例如：
- Roll Strategy 分类
- 不同 roll 方法的共性与差异
- 来自 paper / report 的抽象总结

### `drafts/`

放更接近实现的草稿，例如：
- `RollStrategyBase` 接口草案
- `Profile` 字段设计
- `Result` 数据对象草图
- selector / executor / composer 的分工草图

### `profiles/`

未来可放：
- 各类 roll profile 的配置样例
- profile schema

当前已经有第一批样例：
- [`profiles/single_asset_default.yaml`](/home/ubuntu/dengl/my_projects/cta_lab/strategies/components/roll/profiles/single_asset_default.yaml)
- [`profiles/single_asset_gmat3_domestic_commodity.yaml`](/home/ubuntu/dengl/my_projects/cta_lab/strategies/components/roll/profiles/single_asset_gmat3_domestic_commodity.yaml)
- [`profiles/bundle_static_two_legs.yaml`](/home/ubuntu/dengl/my_projects/cta_lab/strategies/components/roll/profiles/bundle_static_two_legs.yaml)
- [`profiles/bundle_black_style.yaml`](/home/ubuntu/dengl/my_projects/cta_lab/strategies/components/roll/profiles/bundle_black_style.yaml)
- [`profiles/README.md`](/home/ubuntu/dengl/my_projects/cta_lab/strategies/components/roll/profiles/README.md)

### `selectors/`

未来可放：
- OI / volume / calendar / dynamic roll yield 等目标合约选择器

### `executors/`

未来可放：
- 3 日 / 5 日线性 roll
- 固定 window roll
- 更复杂的执行调度器

### `composers/`

未来可放：
- 展期收益生成
- 资产级价值序列生成
- 替代标的切换
- 复合资产 value series 生成

---

## 当前阶段建议

正式进入 P2 开发前，优先在这里沉淀：

1. `Roll Strategy` 的策略形态分类
2. `RollStrategyBase / Profile / Result` 的第一版草案
3. `Selector / Executor / Composer` 的边界
4. 与 `Look-Through` 的连接方式

先把抽象设计讲清楚，再进入正式实现。
