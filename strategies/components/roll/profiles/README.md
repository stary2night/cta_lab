# Roll Profiles

这个目录用来存放 `Roll Strategy Layer` 的 profile 样例。

当前这些文件的定位是：
- 不是最终稳定配置协议
- 也不是运行时唯一入口

而是：

**帮助我们把 `RollStrategyProfile + rule configs` 的结构具体化。**

---

## 当前建议

这里的 profile 样例优先覆盖：

1. 通用单资产最小示例
2. `GMAT3` 风格单资产示例
3. bundle / composite 示例

---

## 当前样例

- `single_asset_default.yaml`
  - 通用单资产 roll strategy 示例
  - 使用 `field_max + linear + hybrid`

- `single_asset_gmat3_domestic_commodity.yaml`
  - `GMAT3` 国内商品单资产示例
  - 使用 `gmat3_domestic_commodity + linear + prefer_selected`

- `bundle_static_two_legs.yaml`
  - 静态双腿 bundle 示例
  - 适合作为近次月双腿或双资产组合的最小起点

- `bundle_black_style.yaml`
  - `BLACK` 风格跨品种 bundle schema 示例
  - 当前主要用来表达 bundle 结构与权重约束

- `bundle_dynamic_external.yaml`
  - 时间变化目标权重的 bundle 示例
  - 适合表达“外部先算好目标权重，再喂给 bundle”的路径

- `bundle_sync_rebalance.yaml`
  - bundle-level rebalance sync schema 示例
  - 适合表达 `BLACK` 这类“年度重构 + 平滑过渡”的同步意图

这些 bundle profile 后续也会继续扩展同步展期字段，例如：

- `sync_mode`
- `sync_frequency`
- `sync_components`

如果要把 GMAT3 `BLACK` 逐步迁入 roll layer，当前可以直接参考：

- `bundle_black_style.yaml`
- `bundle_dynamic_external.yaml`
- `bundle_sync_rebalance.yaml`

---

## 当前边界

这些样例默认都基于：

- `generic contract`
- 不处理 `CU.SHF`、`CU00.SHF`、`CU01.SHF` 这类连续链 / alias contract

---

## 后续方向

后面这里会继续补：

- 同品种近次月 bundle 示例
- 动态权重 bundle 示例
- 更完整的 bundle weight / composition 示例
