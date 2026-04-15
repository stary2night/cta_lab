# Roll Strategy Layer Draft v2

## 1. Updated Core View

Based on the current research notes, a roll strategy should not be described mainly by asset category.

It should be described by the structure of its decision rules.

At a high level, roll strategies in `cta_lab` can be grouped into:

1. time-based roll
2. market-state-based roll
3. hybrid roll combining both

This means the core abstraction should not start from:

- domestic vs overseas
- equity vs bond vs commodity

It should start from:

- what drives roll decision making
- what drives target contract changes
- how execution is scheduled once a decision is made

## 2. Revised Roll Strategy Definition

An abstract roll strategy should be modeled as the combination of two decision systems:

### A. Lifecycle-Driven Decision Making

This rule family is related to the contract's own lifecycle.

Examples:

- first notice day
- last trading day
- exchange-specific holding limits
- predetermined roll windows
- month-based mapping rules
- fixed calendar roll schedules

This part answers:

`When does the strategy have to consider or trigger a roll because of the contract lifecycle?`

### B. Market-State-Driven Decision Making

This rule family is related to market variables observed across contracts.

Examples:

- open interest
- volume
- roll yield / implied roll yield
- spread shape
- liquidity conditions
- other cross-contract state variables

This part answers:

`Given the eligible contracts, which contract or contract set should be preferred under current market conditions?`

This rule should not be limited to the current contract only. It may compare:

- current contract vs next contract
- front curve vs farther curve
- a full candidate set of tradable contracts

## 3. Key Consequence For Design

The core of `RollStrategyBase` should be a composition of:

- lifecycle rules
- market-state rules
- execution rules

This means the most important profile fields are not just asset labels, but rule bundles.

## 4. Proposed Object Model

### 4.1 `RollStrategyProfile`

`RollStrategyProfile` should represent one roll asset definition.

It should include at least:

- `name`
- `asset_key`
- `currency`
- `components`
- `lifecycle_rule`
- `market_state_rule`
- `execution_rule`
- `value_rule`
- `substitute_rule`
- `lookthrough_rule`
- `metadata`

Interpretation:

- `lifecycle_rule` determines when rolling becomes necessary or allowed
- `market_state_rule` determines what contract should be targeted
- `execution_rule` determines how transition is distributed over time
- `value_rule` determines how the asset series is constructed

### 4.2 `RollStrategyBase`

`RollStrategyBase` should orchestrate the following sequence:

1. build candidate contract universe
2. evaluate lifecycle constraints
3. evaluate market-state signals on candidates
4. determine target contract plan
5. generate execution schedule
6. compose value series
7. resolve look-through exposures

This is more precise than the v1 version because it explicitly separates:

- lifecycle-driven roll necessity
- market-state-driven contract preference

### 4.3 `RollStrategyResult`

`RollStrategyResult` should expose both performance and penetrability.

Minimum fields:

- `value_series`
- `contract_plan`
- `roll_schedule`
- `lookthrough_book`
- `roll_return`
- `metadata`

Strongly recommended fields:

- `eligible_contracts`
- `lifecycle_state`
- `market_state_snapshot`
- `decision_trace`
- `component_values`

These fields will make later debugging and research much easier.

## 5. Revised Component Boundaries

### 5.1 `ContractUniverseResolver`

Produces the set of candidate contracts that may be considered.

Responsibilities:

- gather available contracts
- filter by maturity and availability
- filter by lifecycle constraints
- provide candidate set for downstream market-state evaluation

### 5.2 `LifecycleRule`

Determines whether a roll decision is required, allowed, or approaching.

Typical examples:

- fixed roll window before expiry
- first notice day constraint
- last holding day rule
- month-map constraint

Outputs may include:

- `must_roll`
- `may_roll`
- `roll_start_date`
- `roll_end_date`
- `current_contract_valid`

This object is central because many roll strategies are not purely market-state driven.

### 5.3 `MarketStateRule`

Scores or ranks candidate contracts using observed market conditions.

Typical inputs:

- open interest
- volume
- roll yield
- spread metrics
- cross-contract liquidity measures

Typical outputs:

- contract scores
- selected target contract
- ranked candidates
- supporting diagnostics

This component may compare more than two contracts and should support full-curve logic.

### 5.4 `ContractSelector`

Combines lifecycle state and market-state output into a target contract decision.

Responsibilities:

- preserve current contract if still valid under profile rules
- switch target when lifecycle forces a roll
- switch target when market-state preference dominates and rule allows it
- emit a stable target contract plan

This component is where hybrid rules are resolved.

### 5.5 `RollExecutor`

Converts target changes into a scheduled transition path.

Responsibilities:

- define execution window
- distribute old/new holdings through time
- support linear and advanced execution patterns

This remains separate from contract selection.

### 5.6 `ValueComposer`

Builds asset-level value series from execution path and returns.

Responsibilities:

- convert schedule to daily strategy return
- support substitute-index periods
- support composite assets if needed

### 5.7 `LookThroughResolver`

Expands the asset-level series into bottom-level tradable exposures.

Responsibilities:

- map strategy weights into contract-level exposure
- preserve component-level decomposition
- support date-specific inspection of true holdings

## 6. Where `BLACK` Fits

`BLACK` should not be treated as evidence that every special case needs a subclass.

Instead:

- it is still one roll asset definition
- it can still use lifecycle and market-state rules
- it additionally has a composite `value_rule`

So `BLACK` supports the profile-based view:

- special assets differ mostly by configuration and composition rules
- not necessarily by entirely different class hierarchies

## 7. Minimum API Direction

Draft v2 suggests the following conceptual API shape:

```python
result = roll_strategy.run(
    profile=profile,
    market_data=...,
    start=...,
    end=...,
)
```

Where `profile` internally points to:

- lifecycle rule
- market-state rule
- execution rule
- value rule

And `result` exposes:

- performance output
- decision output
- look-through output

## 8. Implication For Phase P2

Phase P2 should not start by directly generalizing `main_contract.py`.

It should start by defining a rule-oriented framework where:

- time-based roll rules can be represented
- market-state-based roll rules can be represented
- hybrid rules can be represented cleanly

Once that is stable, current GMAT3 logic can be mapped into:

- lifecycle rules
- market-state rules
- execution rules
- value composition rules

## 9. Open Questions For Draft v3

1. Should `LifecycleRule` and `MarketStateRule` be pure configuration objects or lightweight callable components?
2. How should hybrid conflict resolution be expressed in the profile?
3. Should `decision_trace` be a first-class object for notebook research and debugging?
4. What is the minimum look-through schema needed for execution readiness?
5. Which parts of current GMAT3 modules map directly into these new rule components?
