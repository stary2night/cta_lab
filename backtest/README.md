# Backtest Layer

> Last updated: 2026-04-28

`backtest/` 同时支持向量化研究回测和轻量事件驱动回测。两种范式共享
`BacktestResult`，但职责边界不同：

- `VectorizedBacktest`：输入收益率矩阵和目标权重矩阵，适合快速研究和参数扫描。
- `backtest/event/`：通过 `MarketDataPortal -> Strategy hooks -> Broker -> PortfolioState` 逐日推进状态，适合验证订单、成交、费用、滑点和组合状态。
- 旧 `BacktestEngine`：保留给已有权重矩阵状态推进路径使用，不作为新的事件驱动范式入口。

## Cost Model

新的轻量成本模型位于 `backtest/costs.py`，用于统一向量化与事件驱动路径的费用语义：

- `ZeroCostModel`：无交易成本。
- `ProportionalCostModel(rate)`：按成交 notional 或权重换手收取固定比例费用。
- `DailyAccrualCostModel(annual_rate)`：每日按年化费率计提管理费或 tracking fee。
- `CompositeCostModel([...])`：组合多个成本模型。

向量化路径中，`ProportionalCostModel` 作用于 `turnover_series`，成本是收益率扣减项；若启用 `vol_target`，`VectorizedBacktest` 会先生成 vol-target 后的有效执行权重，再按有效权重变化计算换手和成本，避免用未缩放的原始信号权重计费。vol-target 的 EWMA 波动率只使用原始执行权重非零后的活跃收益样本，热身完成前 scale 为 0，避免用未来波动率或未建仓期零收益放大初始仓位。事件驱动路径中，`ProportionalCostModel` 作用于 broker 生成的成交 notional，成本从现金中扣除。

旧接口仍兼容：

- `VectorizedBacktest(fee_rate=...)`
- `EventDrivenBacktestEngine(commission_rate=...)`

但新代码优先使用 `cost_model=...`，这样向量化和事件驱动可以复用同一套成本定义。

当前正式策略脚本已开始统一暴露成本参数和换手/成本报告：

- `scripts/run_crossmom.py --cost-bps`
- `scripts/run_dual_momentum.py --cost-bps`
- `scripts/run_jpm.py --cost-bps`
- `scripts/run_multifactor_cta.py --cost-bps`
- `scripts/run_netmom.py --cost-bps`
- `scripts/run_overseas.py --cost-bps`
- `scripts/run_tsmom.py --cost-bps`，并保留旧 `--fee-rate`
- `scripts/run_jpm_event.py --commission-bps --slippage-bps`

向量化脚本会输出 `turnover_cost*.csv`，并在 `full_sample_summary.csv` 中增加平均换手、年化换手、总交易成本和年化成本拖累。
其中 `jpm_trend_trade/JPMConfig` 已包含 `transaction_cost_bps` 默认配置；`run_jpm.py` 未显式传入 `--cost-bps` 时使用该默认值，`run_jpm_event.py` 未显式传入 `--commission-bps` 或正的 `--commission-rate` 时也回退到该默认值。

需要特别说明的是，`VectorizedBacktest` 现在的换手与成本口径已经对齐“有效执行权重”：

- 先对 `weights_df` 做 `lag` 对齐，得到实际执行权重
- 若启用 `vol_target`，先基于未扣费组合收益估计 EWMA scale，再把 scale 作用到执行权重
- 换手、成本、`max_abs_weight`、`max_gross_exposure` 都在这个有效执行权重层面计算
- vol-target 热身期 scale 保持为 0，不向前回填未来波动率，避免早期仓位被未来信息放大

## Slippage Model

滑点模型位于 `backtest/slippage.py`，当前主要服务事件驱动路径：

- `NoSlippage`：按 snapshot price 成交。
- `FixedBpsSlippage(bps)`：买入价格上移，卖出价格下移。

向量化回测不模拟成交价，若需要表达滑点，优先折算成 `ProportionalCostModel`。

事件成交日志中：

- `commission` 是现金扣除项。
- `slippage` 是执行价偏离 snapshot price 的估算实现成本。
- `daily_cost` 是每日计提成本。
- `total_cost` 是用于报告的总实现成本，不额外参与 NAV 重复扣减。

## Event Strategy Hooks

事件驱动策略推荐写法：

- `__init__`：只接收配置和依赖，不做重计算。
- `on_start(context)`：预计算与路径状态无关的市场特征，例如 signal、vol、corr cache、调仓日历。
- `on_bar(context)`：读取当前 snapshot、portfolio 和 strategy_state，决定是否发单。
- `on_order(context)`：记录订单状态，通常不放策略主逻辑。
- `on_fill(context)`：记录成交、风控状态或策略内部交易统计。
- `on_finish(context)`：做收尾检查或轻量状态汇总。

边界原则：

- 市场特征不是策略状态，应尽量在 `on_start()` 预计算。
- `PortfolioState` 只表示现金、NAV、持仓、权重和敞口。
- `SimulatedBroker` 只负责订单执行、滑点、费用、成交和交易流水。
- `StrategyState` 只保存策略自己的运行时状态，例如上次调仓日、风控开关、最新信号日期。

`JPMEventDrivenStrategy` 是当前正式策略包中的事件驱动样板：它在 `on_start()` 预计算 t-stat、sigma 和 CorrCap rolling cache，在 `on_bar()` 只读取当天特征并生成 target-weight 订单。
