## ADDED Requirements

### Requirement: DuckDB backend contract parity
The system SHALL provide a DuckDB implementation of `LocalDataBackend` that returns the same canonical pandas objects, units, ordering, point-in-time filtering, and errors as the Parquet backend for supported datasets.

#### Scenario: Equivalent price query
- **WHEN** identical source data and query parameters are used with Parquet and DuckDB backends
- **THEN** the public provider results are equivalent after normal dtype normalization

### Requirement: Fully materialized high-performance runtime
The system SHALL provide a high-performance local runtime in which every supported dataset required by the selected manifest is stored in canonical DuckDB physical tables. The runtime MUST NOT scan or silently fall back to source Parquet files when this profile is selected.

#### Scenario: Fully materialized backtest starts
- **WHEN** the user selects the DuckDB materialized profile and preflight succeeds
- **THEN** all strategy-time prices, reference data, fundamentals, and corporate actions are read from DuckDB physical tables

#### Scenario: A required materialized shard is missing
- **WHEN** the selected manifest does not provide a shard needed by the requested date, asset, or frequency
- **THEN** preflight or the query fails with rebuild guidance instead of reading Parquet

### Requirement: Parquet-to-DuckDB builder
The system SHALL provide a repeatable, non-interactive command that validates an existing Parquet data root and builds canonical DuckDB datasets without modifying the source Parquet files. Materialized mode SHALL be the production path; external views MAY be retained only for diagnostics, comparison, or migration.

#### Scenario: Materialized database build
- **WHEN** the user selects materialized mode and supplies a Parquet root and DuckDB output path
- **THEN** the builder consolidates source files into canonical physical tables, applies declared precedence and deduplication rules, and records their source fingerprints

#### Scenario: External-view database build
- **WHEN** the user selects external-view mode
- **THEN** the builder creates explicitly diagnostic DuckDB views and marks the resulting manifest as ineligible for the fully materialized runtime profile

### Requirement: Canonical consolidation rather than file mirroring
The builder MUST map source files to a bounded canonical table registry and MUST NOT create one table per security or one table per Parquet file. Overlapping history sources SHALL use deterministic precedence and canonical-key deduplication.

#### Scenario: Modern and legacy stock history overlap
- **WHEN** the long-history adjustment file overlaps the modern stock daily file
- **THEN** the canonical table keeps the declared modern row for the overlap and uses legacy rows only for the uncovered earlier interval

#### Scenario: Thousands of per-security minute files are imported
- **WHEN** the builder reads a frequency directory containing per-security Parquet files
- **THEN** it produces a bounded set of asset/frequency/time-partitioned canonical tables or database shards

### Requirement: Immutable sharded generations
The production build SHALL emit immutable, version-addressed DuckDB shards selected by an atomic manifest pointer. Sharding SHALL support at least metadata, daily data, fundamentals, corporate actions, and minute data partitioned by asset, frequency, and time range.

#### Scenario: A new generation is published on Windows
- **WHEN** all new shards and invariants validate successfully
- **THEN** the builder atomically publishes a new manifest pointer without replacing database files held open by an existing backtest

#### Scenario: An old backtest remains active
- **WHEN** a new manifest generation is published while an existing process has old shards open read-only
- **THEN** the existing process continues on its original generation and new processes use the new generation

### Requirement: Query-oriented physical layout
The builder SHALL order or cluster canonical tables according to declared query patterns so DuckDB zonemaps can prune data effectively. The manifest SHALL record the physical sort keys and partition coverage.

#### Scenario: Daily cross-section is queried
- **WHEN** the backend requests a trading-date range and a security set from daily bars
- **THEN** the physical layout permits date-range pruning before returning the requested securities

#### Scenario: Minute history is queried
- **WHEN** the backend requests minute bars for a security set and bounded interval
- **THEN** it opens only time shards overlapping the interval and applies security/time predicates within those shards

### Requirement: Atomic and diagnosable builds
The builder MUST create new shards in staging, validate schema and row-level invariants, and publish the new manifest only after every required shard succeeds. A failed build MUST leave the previous manifest and generation usable.

#### Scenario: Validation fails during refresh
- **WHEN** a required table or canonical column fails validation
- **THEN** the command exits non-zero, reports the failing dataset, preserves the active generation, and leaves incomplete staging files unreferenced

### Requirement: Versioned manifest
Every generation SHALL have a manifest with schema version, DuckDB storage compatibility, build mode, build timestamp, source root, source fingerprints, shard identities, physical sort keys, dataset row counts, covered date ranges, duplicate-key counts, and validation results.

#### Scenario: Provider opens a stale database
- **WHEN** the manifest schema is incompatible with the running provider
- **THEN** provider validation fails with migration or rebuild guidance before the backtest starts

### Requirement: Native batch query execution
`DuckDBDataBackend` SHALL override the storage-neutral batch contract and execute each compatible multi-security request with a bounded number of parameterized SQL statements. It SHALL reuse a read-only connection per backend instance and return canonical long-form Arrow or pandas results without per-row Python processing.

#### Scenario: Many securities share one daily request
- **WHEN** the provider requests the same date range, frequency, and fields for many stock securities
- **THEN** the backend performs one native batch scan of the relevant daily shard set rather than one scan per security

#### Scenario: Per-security count is requested
- **WHEN** a batch request specifies the latest N bars per security
- **THEN** the backend applies a calendar-derived lower bound or a partitioned SQL window while preserving exact per-security count semantics

### Requirement: Resource-aware build and runtime configuration
The builder and backend SHALL accept configurable thread, memory, database-root, temporary-directory, and temporary-space limits. Paths MUST be resolved with Windows-compatible path handling and MUST NOT hardcode a drive letter.

#### Scenario: Full build requires spill space
- **WHEN** a materialized build exceeds its configured memory budget
- **THEN** DuckDB spills only to the configured temporary directory and fails before publishing if the configured space limit cannot satisfy the build

#### Scenario: Multiple backtests run concurrently
- **WHEN** more than one read-only backtest process uses the same generation
- **THEN** each process obeys its configured thread and memory budget without requiring a writer connection

### Requirement: Controlled evaluation of external converters
An opaque or GUI-only converter MUST NOT be a required production dependency. Any candidate external converter SHALL be evaluated on an isolated sample for canonical schema, deduplication, sort order, manifest, validation, automation, and DuckDB-version compatibility before its output can be admitted.

#### Scenario: Bundled converter produces a sample database
- **WHEN** `/tool/格式转换器最新.exe` is evaluated
- **THEN** its output is used only after automated parity and invariant checks pass, and failure leaves the repository builder as the authoritative conversion path

### Requirement: Backward-compatible backend configuration
The existing relative `LOCAL_DATA_PATH=./data/parquet` configuration SHALL continue to select Parquet by default. Selecting DuckDB SHALL be explicit, SHALL accept a Windows-compatible relative database path, and SHALL not change strategy API calls.

#### Scenario: Windows user selects DuckDB
- **WHEN** the user sets the local backend to DuckDB and supplies a relative `.duckdb` path
- **THEN** the path resolves from the project root and the local provider opens the selected manifest or database generation
