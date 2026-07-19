## ADDED Requirements

### Requirement: Documented local backtest configuration
The system SHALL document environment-variable, configuration-file, CLI, and Python-API examples for selecting `provider=local`, setting a relative data path, and choosing Parquet or DuckDB.

#### Scenario: User runs an existing strategy locally
- **WHEN** the user follows the documented local-provider configuration for an existing compatible strategy
- **THEN** the backtest uses only the selected local backend without requiring JQData authentication

### Requirement: Preflight validation
The local backtest workflow SHALL validate backend dependencies, schema versions, required datasets and shards, requested date coverage, security coverage, strategy-required provider capabilities, manifest identity, and configured memory/thread/temporary-space budgets before simulation begins.

#### Scenario: Strategy requires missing corporate actions
- **WHEN** preflight detects that the strategy requires `get_split_dividend()` but no valid corporate-action dataset exists
- **THEN** the backtest stops before simulation with an actionable preparation command

#### Scenario: Fully materialized generation lacks coverage
- **WHEN** the requested asset, frequency, or date interval is not completely covered by the selected DuckDB manifest
- **THEN** preflight stops before simulation and does not fall back to Parquet

### Requirement: Reproducible run metadata
Each local backtest SHALL record the selected backend, resolved non-secret data location, schema version, source fingerprints or manifest identity, shard set, resource settings, query-mode diagnostics, backend query counts, cache statistics, and native/fallback batch usage in its run metadata.

#### Scenario: User compares two reports
- **WHEN** two local backtest reports are inspected
- **THEN** their data backend and dataset identities can be compared without exposing credentials

### Requirement: Minimum offline smoke test
The project SHALL include an automated smoke strategy that reads local prices and reference data, places at least one order, and completes a report using a deterministic fixture or declared local sample.

#### Scenario: Local provider regression test runs
- **WHEN** the smoke test is executed with no network access
- **THEN** it completes successfully for every supported local backend and verifies equivalent key portfolio results

### Requirement: Backend switch without strategy edits
The workflow SHALL allow an eligible strategy to switch between Parquet and DuckDB through configuration only.

#### Scenario: User changes backend setting
- **WHEN** the same strategy and data snapshot are run once with Parquet and once with DuckDB
- **THEN** no strategy source changes are required and the documented deterministic outputs are equivalent

### Requirement: Batched engine hot paths
The backtest engine SHALL batch compatible reads for end-of-day position valuation, order execution-price resolution, current-data snapshots, and corporate-action evaluation. These internal optimizations MUST NOT change the public strategy API or event ordering.

#### Scenario: Portfolio contains many positions
- **WHEN** end-of-day valuation runs for multiple positions
- **THEN** the engine obtains all required closing prices from one native batch request per compatible asset/frequency group

#### Scenario: Multiple orders execute on one bar
- **WHEN** several orders require the same bar snapshot
- **THEN** execution-price resolution reuses the already loaded current snapshot rather than issuing one backend query per order

### Requirement: Multi-security backtest preloading
For a local DuckDB backtest, `BacktestDataSession` SHALL support preloading bounded multi-security Arrow blocks for the requested interval, fields, frequency, and manifest generation. It SHALL provide exact historical-window slices without repeated database queries when the requested data is resident.

#### Scenario: Daily strategy repeatedly requests count windows
- **WHEN** a strategy repeatedly asks for recent daily bars for securities in the active universe
- **THEN** the first compatible load populates a bounded block and subsequent windows are served from that block

#### Scenario: Minute data exceeds memory budget
- **WHEN** requested minute blocks would exceed the configured memory or free-memory guard
- **THEN** the session partitions loads by date/security, evicts blocks using a deterministic bounded policy, and preserves query correctness

### Requirement: Generation-isolated caches
Every current-bar, price-block, reference-factor, fundamentals, and corporate-action cache key MUST include the selected manifest or dataset identity so results from different immutable generations cannot mix.

#### Scenario: User switches manifest generation
- **WHEN** a new backtest selects a different manifest identity
- **THEN** it does not reuse cached values produced from the previous generation

### Requirement: Query-path rollback
The workflow SHALL provide a diagnostic setting that forces the scalar backend path and SHALL retain the Parquet backend as an explicit correctness baseline and rollback option. Neither fallback SHALL occur silently.

#### Scenario: Batch parity regression is suspected
- **WHEN** the user explicitly selects scalar query mode for the same DuckDB generation
- **THEN** the run uses the compatibility path, records that choice, and permits result comparison without strategy edits

### Requirement: Performance acceptance gates
The project SHALL benchmark cold and warm query latency, backend query count, peak memory, temporary disk, build time, and complete backtest runtime on declared representative workloads. Performance changes MUST pass semantic parity before their speedup is accepted.

#### Scenario: Twenty-security daily batch benchmark
- **WHEN** the one-year, twenty-security, two-field benchmark is run on the reference machine and snapshot
- **THEN** the native materialized batch path targets at least a fivefold speedup over the recorded scalar Parquet baseline or documents why the gate is not met

#### Scenario: Representative daily backtest benchmark
- **WHEN** a multi-security daily strategy is run against equivalent Parquet and fully materialized DuckDB snapshots
- **THEN** the optimized path targets at least a threefold end-to-end speedup while orders, fills, cash, positions, corporate actions, and key net-value points remain equivalent

#### Scenario: Correctness and speed conflict
- **WHEN** a faster path changes future-data isolation, adjustment, pause, unit, ordering, or portfolio results beyond declared tolerances
- **THEN** the performance gate fails regardless of runtime improvement
