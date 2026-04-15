# Roll Strategy Layer Draft v3

## 1. Positioning

`Roll Strategy Layer` is a reusable strategy-component layer for constructing investable futures-based assets.

Its purpose is broader than continuous-series generation.

It should support:

- single-contract roll assets
- multi-contract synchronized roll assets
- substitute-index-to-futures assets
- composite futures bundles across symbols

The primary output is an asset-level strategy result that upper layers can allocate to.

## 2. Core Design Shift

The layer should not be centered on asset category names such as equity, bond, or commodity.

It should be centered on:

1. lifecycle-driven roll constraints
2. market-state-driven target selection
3. execution scheduling
4. value composition
5. look-through exposure

This leads to a component-based design:

- `RollStrategyBase`
- `RollStrategyProfile`
- `LifecycleRule`
- `MarketStateRule`
- `ContractSelector`
- `RollExecutor`
- `ValueComposer`
- `LookThroughResolver`
- `RollStrategyResult`

## 3. Single vs Bundle Roll

The layer must support two equally valid strategy forms.

### 3.1 Single Roll Asset

A single roll asset is built from one logical exposure stream.

Examples:

- one futures symbol rolling through time
- one target contract chosen from a curve
- one substitute index transitioning into one futures stream

### 3.2 Bundle Roll Asset

A bundle roll asset is built from multiple coordinated exposure streams.

Examples:

- near-month + next-month bundle within one symbol
- multi-symbol bundle such as `BLACK`
- any synchronized basket whose components each have their own contract lifecycle

This means `BLACK` should not be treated as an exception.

It is one instance of a more general design requirement:

`Roll Strategy Layer` must support both single exposures and bundled exposures.

## 4. What A Bundle Needs

Once bundle support is accepted, the design must explicitly model:

- bundle composition
- component eligibility
- component weight rules
- bundle-level roll synchronization
- component-level look-through

Bundle weight rules may include:

- static target weights
- dynamic holding-value weights
- capped weights
- floored weights
- smoothed rebalance weights

So weight constraints such as:

- `weight_min`
- `weight_max`
- rebalance frequency
- smoothing window

should not be hidden inside ad hoc strategy code.

## 5. Minimum Conceptual Model

At a high level, one roll strategy instance should follow:

`profile -> component universe -> lifecycle state -> market-state decision -> target plan -> execution schedule -> value composition -> look-through result`

For bundle assets, this flow happens at two levels:

1. component level
2. bundle composition level

## 6. Proposed Core Objects

### 6.1 `RollStrategyProfile`

`RollStrategyProfile` defines one asset-level roll strategy.

It should be configuration-oriented and should support both single and bundle assets.

Suggested minimum fields:

- `name`
- `asset_key`
- `asset_mode`
- `currency`
- `components`
- `bundle_rule`
- `lifecycle_rule`
- `market_state_rule`
- `execution_rule`
- `value_rule`
- `substitute_rule`
- `lookthrough_rule`
- `metadata`

Interpretation:

- `asset_mode`: `"single"` or `"bundle"`
- `components`: one or many logical exposure components
- `bundle_rule`: how components are weighted and synchronized
- `lifecycle_rule`: when roll becomes required or allowed
- `market_state_rule`: which contract(s) should be preferred
- `execution_rule`: how old/new positions are transitioned
- `value_rule`: how to convert execution path into strategy value

### 6.2 `RollComponentProfile`

For bundle assets, each component should itself be explicitly represented.

Suggested fields:

- `component_key`
- `symbol`
- `contract_scope`
- `lifecycle_rule`
- `market_state_rule`
- `execution_rule`
- `substitute_rule`
- `metadata`

This allows:

- one-symbol single roll assets
- multi-symbol bundles
- same-symbol multi-contract bundles

under the same general framework.

### 6.3 `BundleRule`

`BundleRule` describes how multiple components form one investable asset.

Suggested responsibilities:

- determine component target weights
- apply min/max caps
- define rebalance frequency
- define smoothing logic
- support drift between rebalance dates if needed

This object is what makes `BLACK` interpretable as a normal bundle profile rather than a special hardcoded class.

### 6.4 `RollStrategyResult`

This is the formal output from the layer.

Minimum fields:

- `value_series`
- `contract_plan`
- `roll_schedule`
- `lookthrough_book`
- `roll_return`
- `metadata`

Recommended additional fields:

- `asset_mode`
- `component_values`
- `component_weights`
- `eligible_contracts`
- `lifecycle_state`
- `market_state_snapshot`
- `decision_trace`

## 7. Rule-Oriented Components

### 7.1 `LifecycleRule`

Purpose:

- define roll necessity from contract lifecycle

Typical outputs:

- `must_roll`
- `may_roll`
- `roll_window`
- `holding_validity`

Examples:

- fixed pre-expiry window
- last-holding-date trigger
- month-map rule

### 7.2 `MarketStateRule`

Purpose:

- define target preference from observed contract state

Possible inputs:

- open interest
- volume
- implied roll yield
- curve spreads
- liquidity metrics
- other cross-contract state variables

Possible outputs:

- ranked contracts
- target contract
- state diagnostics

### 7.3 `ContractSelector`

Purpose:

- combine lifecycle state and market-state output into target decision

Responsibilities:

- keep current contract if still valid
- switch when lifecycle forces change
- switch when market-state rule dominates and profile permits

### 7.4 `RollExecutor`

Purpose:

- turn target changes into time-distributed execution weights

Examples:

- single-day execution
- linear 3-day or 5-day execution
- adaptive execution schedule

### 7.5 `ValueComposer`

Purpose:

- build asset-level value chain

It should support:

- single-stream futures roll
- substitute-to-futures transition
- bundle aggregation from multiple rolled components

### 7.6 `LookThroughResolver`

Purpose:

- expose bottom-level tradable holdings

It should support at least:

- strategy -> component weights
- component -> contract weights
- final contract-level exposure by date

This is mandatory for execution readiness.

## 8. Proposed Output Views

Each `RollStrategyResult` should support two practical views.

### 8.1 Performance View

For upper-layer research and allocation:

- `value_series`
- returns
- component contribution

### 8.2 Look-Through View

For holdings and execution:

- component weights
- symbol weights
- contract-level exposure
- substitution state

Without this second view, the layer is not sufficient for real strategy deployment.

## 9. Minimum Implementable API

The first implementable interface should stay small.

Conceptually:

```python
result = roll_strategy.run(
    profile=profile,
    market_data=market_data,
    start=start,
    end=end,
)
```

Where `market_data` may expose:

- contract metadata
- daily prices
- open interest
- volume
- substitute series

And `result` returns both performance and look-through structures.

## 10. Mapping To Current GMAT3 Logic

Current GMAT3 modules can be interpreted as early versions of the following reusable components:

- `main_contract.py` -> lifecycle + market-state + selector logic
- `roll_return.py` -> part of value composition
- `sub_portfolio.py` -> value composer plus bundle rule handling
- `universe.py` -> profile metadata and bundle definitions

This suggests P2 should not start from scratch.

Instead, it should:

1. define the new component interfaces
2. map GMAT3 logic into them
3. verify that both single roll and bundle roll fit naturally

## 11. Concrete P2 Development Direction

Phase P2 should start with a minimum viable implementation that supports:

1. one single-symbol main-contract roll asset
2. one dynamic contract-selection roll asset
3. one bundle roll asset such as `BLACK`

If these three all fit the same abstraction, the design is likely good enough to proceed.

## 12. Decisions Fixed In v3

The following points should now be treated as stable unless later evidence strongly contradicts them:

1. Roll strategy should be profile-driven, not subclass-heavy.
2. Lifecycle and market-state decision systems are distinct.
3. Bundle roll is a first-class concept, not a special case.
4. Look-through is mandatory.
5. Asset-level `value_series` is the main output consumed by upper-layer allocation.

## 13. Open Questions After v3

Only a few major questions remain for implementation design:

1. Should `LifecycleRule` and `MarketStateRule` be dataclass configs, callable objects, or both?
2. What is the minimum formal schema for `lookthrough_book`?
3. How should component-level and contract-level exposures be stored efficiently?
4. Should bundle synchronization be handled inside `ValueComposer` or by a separate `BundleComposer`?
