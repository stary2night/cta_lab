# Roll Strategy Layer Draft v1

## 1. Purpose

`Roll Strategy Layer` is intended to become a reusable component layer for asset-level strategy construction inside `cta_lab`.

Its goal is not to produce another continuous price series. Its goal is to produce a tradable asset definition built from futures contracts, together with:

- target contract plan
- roll execution schedule
- asset-level value series
- look-through exposure to bottom-level tradable contracts

This layer is designed to support:

- GMAT3-style main-contract roll assets
- dynamic contract-selection strategies
- execution-aware roll strategies
- substitute-index switching assets
- composite roll assets such as `BLACK`

## 2. Design Goals

The first version of the design should satisfy these constraints:

1. Different roll strategies should usually be different profiles, not different subclasses.
2. Contract selection and roll execution must be modeled separately.
3. Asset-level `value_series` is the primary output for upper-layer allocation.
4. Every result must remain look-through capable.
5. The layer should support both simple single-asset roll logic and composite assets.

## 3. Core Mental Model

A roll strategy can be viewed as a pipeline:

`market data -> contract universe -> target contract plan -> roll schedule -> value composition -> asset series + look-through`

This suggests a small set of stable component boundaries:

- `RollStrategyBase`
- `RollStrategyProfile`
- `ContractUniverseResolver`
- `ContractSelector`
- `RollExecutor`
- `ValueComposer`
- `LookThroughResolver`
- `RollStrategyResult`

## 4. Proposed Core Objects

### 4.1 `RollStrategyProfile`

`RollStrategyProfile` describes what kind of roll asset we want to build.

It should be configuration-oriented rather than implementation-heavy.

Suggested responsibilities:

- identify the asset or component universe
- define contract eligibility rules
- define target contract selection rules
- define roll timing and execution rules
- define substitute-switch rules when futures history is unavailable
- define value-composition rules
- define composite weighting rules when the asset is built from multiple components

Typical fields may include:

- `name`
- `asset_key`
- `asset_type`
- `currency`
- `components`
- `universe_rule`
- `selector_rule`
- `executor_rule`
- `value_rule`
- `substitute_rule`
- `lookthrough_rule`
- `metadata`

This object should be expressive enough that:

- `IF`, `TF`, `ES`, `LCO` are mostly different profiles
- `BLACK` is a composite profile
- dynamic roll indices are also profiles

### 4.2 `RollStrategyBase`

`RollStrategyBase` is the orchestrator for building one asset-level roll strategy result.

It should not own all detailed business rules itself. Instead, it should coordinate pluggable components according to a profile.

Suggested responsibilities:

- accept a `RollStrategyProfile`
- build the contract universe for each evaluation date
- generate a target contract plan
- convert target changes into a daily roll schedule
- compose the resulting asset-level value series
- build look-through exposures
- return a unified result object

Suggested high-level flow:

1. resolve eligible contracts
2. select target contract(s)
3. generate roll execution schedule
4. compose asset value series
5. resolve look-through exposures
6. package all outputs

### 4.3 `RollStrategyResult`

`RollStrategyResult` is the standard output object from the roll layer.

This should be richer than a single NAV series.

Suggested contents:

- `value_series`
- `contract_plan`
- `roll_schedule`
- `lookthrough_book`
- `roll_return`
- `component_values`
- `metadata`

Optional helpful fields:

- `active_contract`
- `old_contract`
- `new_contract`
- `execution_weights`
- `substitute_flag`
- `diagnostics`

The output should support two views:

- performance view: what the asset did
- look-through view: what the asset held

## 5. Proposed Component Boundaries

### 5.1 `ContractUniverseResolver`

This component decides what contracts are eligible candidates at a given date.

Responsibilities:

- enumerate candidate contracts
- apply maturity constraints
- apply liquidity constraints
- exclude contracts past holding limits
- support profile-specific candidate sets

This is especially important for dynamic roll strategies where the target is selected from a curve, not simply from one front contract.

### 5.2 `ContractSelector`

This component decides which contract or contract set should be targeted.

Responsibilities:

- select target contract from candidate contracts
- support rules such as:
  - open-interest max
  - fixed month mapping
  - implied roll yield ranking
  - parity-style keep-current-if-still-valid logic
- emit a target contract plan instead of a raw final series

This component answers:

`What should the strategy hold?`

### 5.3 `RollExecutor`

This component turns target changes into a daily execution schedule.

Responsibilities:

- decide when rolling starts
- define the execution window
- assign daily transition weights between old/new contracts
- support simple and advanced execution rules

Examples:

- 3-day linear roll
- 5-day index-style roll
- adaptive multi-day execution
- execution schedules optimized for transaction cost or execution risk

This component answers:

`How should the strategy move from the old target to the new target?`

### 5.4 `ValueComposer`

This component builds the asset-level `value_series`.

Responsibilities:

- map roll schedule into daily returns or value changes
- handle substitute-index periods before futures become tradable
- compose multi-component assets when needed
- support profile-specific composition logic

Examples:

- single-asset futures roll value chain
- substitute index then futures value chain
- composite basket such as `BLACK`

This component answers:

`How do we convert the contract plan and execution path into an investable asset series?`

### 5.5 `LookThroughResolver`

This component preserves penetrability from strategy layer to underlying tradable assets.

Responsibilities:

- expand asset-level holdings into contract-level exposures
- preserve component-level mappings for composite assets
- expose bottom-level tradable contracts and weights on any date

This component answers:

`What is the strategy actually holding underneath the asset-level series?`

## 6. Relationship With Other CTA Lab Layers

### With `data`

The roll layer should consume standardized futures and metadata access from `data`, but should not be reduced to a simple `load_continuous` extension.

`data` provides the raw contract information.
`roll` turns that information into an investable asset definition.

### With `signals`

Upper-layer cross-asset signals should consume `RollStrategyResult.value_series` or a matrix built from many such results.

Signals should not need to know the internal roll schedule, but advanced strategy diagnostics may still access it.

### With `portfolio`

Portfolio construction should consume the asset-level outputs from the roll layer as investable assets.

Look-through information should remain available for:

- exposure analysis
- execution mapping
- bottom-level holdings inspection

### With `strategy`

Complex strategies such as GMAT3 should be composed from:

- many roll strategy instances
- cross-asset signal and allocation logic
- final strategy aggregation

## 7. Scope of Draft v1

Draft v1 only fixes component boundaries and output expectations.

It does not yet define:

- exact dataclass schemas
- exact method signatures
- exact file splits
- registry/config loading conventions
- how much logic should move out of the current GMAT3 package first

Those should be handled in the next draft.

## 8. Near-Term Questions For Draft v2

The next design iteration should answer these questions:

1. What should be the minimum formal schema for `RollStrategyProfile`?
2. Should `RollStrategyResult` be a dataclass or a lightweight structured container?
3. How should composite assets expose component weights and look-through weights together?
4. Which parts of current GMAT3 code can be promoted directly into the roll component layer?
5. What is the minimum viable API that can support both GMAT3-style assets and dynamic roll-style assets?

## 9. Practical Direction

The practical goal of Phase P2 should be:

- first support GMAT3-style roll assets through the new abstractions
- then verify the same abstraction can also describe dynamic contract-selection strategies

If both fit naturally, the component design is probably on the right track.
