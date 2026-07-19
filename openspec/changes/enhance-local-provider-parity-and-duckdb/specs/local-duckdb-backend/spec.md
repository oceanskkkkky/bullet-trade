## ADDED Requirements

### Requirement: DuckDB backend contract parity
The system SHALL provide a DuckDB implementation of `LocalDataBackend` that returns the same canonical pandas objects, units, ordering, point-in-time filtering, and errors as the Parquet backend for supported datasets.

#### Scenario: Equivalent price query
- **WHEN** identical source data and query parameters are used with Parquet and DuckDB backends
- **THEN** the public provider results are equivalent after normal dtype normalization

### Requirement: Parquet-to-DuckDB builder
The system SHALL provide a repeatable command that validates an existing Parquet data root and builds a DuckDB database without modifying the source Parquet files.

#### Scenario: Materialized database build
- **WHEN** the user selects materialized mode and supplies a Parquet root and DuckDB output path
- **THEN** the builder creates physical DuckDB tables for the supported datasets and records their source fingerprints

#### Scenario: External-view database build
- **WHEN** the user selects external-view mode
- **THEN** the builder creates DuckDB views over the Parquet datasets without duplicating their row data

### Requirement: Atomic and diagnosable builds
The builder MUST create or refresh a temporary database, validate its schema and row-level invariants, and replace the target only after validation succeeds. A failed build MUST leave the previous valid target usable.

#### Scenario: Validation fails during refresh
- **WHEN** a required table or canonical column fails validation
- **THEN** the command exits non-zero, reports the failing dataset, and preserves the existing DuckDB database

### Requirement: Versioned manifest
Every built database SHALL contain a manifest with schema version, build mode, build timestamp, source root, source fingerprints, dataset row counts, and covered date ranges.

#### Scenario: Provider opens a stale database
- **WHEN** the manifest schema is incompatible with the running provider
- **THEN** provider validation fails with migration or rebuild guidance before the backtest starts

### Requirement: Backward-compatible backend configuration
The existing relative `LOCAL_DATA_PATH=./data/parquet` configuration SHALL continue to select Parquet by default. Selecting DuckDB SHALL be explicit, SHALL accept a Windows-compatible relative database path, and SHALL not change strategy API calls.

#### Scenario: Windows user selects DuckDB
- **WHEN** the user sets the local backend to DuckDB and supplies a relative `.duckdb` path
- **THEN** the path resolves from the project root and the local provider opens that database

