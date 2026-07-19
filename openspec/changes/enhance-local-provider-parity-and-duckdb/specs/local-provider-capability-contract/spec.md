## ADDED Requirements

### Requirement: Stable local provider facade
The system SHALL keep `LocalDataProvider` as the storage-neutral public facade and SHALL preserve the existing JoinQuant-compatible method signatures and return shapes when a local storage backend is changed.

#### Scenario: Strategy changes storage backend
- **WHEN** a strategy changes the local backend from Parquet to DuckDB
- **THEN** the strategy can call the same provider methods without code changes

### Requirement: Explicit capability classification
The system SHALL publish each `DataProvider` method as `supported`, `limited`, or `unsupported`, including the applicable asset types, frequencies, fields, and limitations, and SHALL expose this information through non-sensitive diagnostics.

#### Scenario: User checks compatibility before a backtest
- **WHEN** the user requests local provider diagnostics or reads the generated capability reference
- **THEN** each public provider method has an explicit compatibility level and limitation description

### Requirement: Deterministic unsupported-operation errors
The system MUST raise a typed local-data query or capability error for an unsupported operation and MUST NOT return a plausible empty result merely because the operation has no implementation.

#### Scenario: Strategy requests tick data
- **WHEN** a strategy calls an unsupported tick or live-market method on `LocalDataProvider`
- **THEN** the provider raises an error that names the unsupported method and the missing local dataset or capability

### Requirement: Offline query isolation
The local provider and all local backends MUST NOT access JQData, Tushare, or another network source while answering a strategy query.

#### Scenario: Required local data is absent
- **WHEN** a local query requires a dataset that is missing
- **THEN** the query fails with an actionable local-data error and does not silently fetch remote data

### Requirement: Core interface parity
The system SHALL support local queries for prices, trade days, security catalogs, security information, index constituents and weights, the documented fundamentals subset, `is_st` extras, bars, and corporate actions when their declared datasets are present.

#### Scenario: Supported JoinQuant-style query
- **WHEN** a strategy calls a declared supported method with supported parameters and complete local data
- **THEN** the result follows the documented JoinQuant-compatible code, unit, ordering, and return-shape semantics

### Requirement: Backward-compatible batch backend contract
The system SHALL extend `LocalDataBackend` with storage-neutral batch reads for bars, current snapshots, reference factors, security information, and corporate actions. Every batch method MUST have a correct default implementation based on the existing scalar methods so that adding the extension does not break an existing backend subclass.

#### Scenario: Existing backend does not override batch reads
- **WHEN** a local backend implements only the pre-existing scalar methods
- **THEN** multi-security provider calls remain correct through the default batch fallback without requiring changes to that backend

#### Scenario: DuckDB advertises native batching
- **WHEN** the selected backend declares native batch capability
- **THEN** `LocalDataProvider` groups compatible requests and invokes a bounded number of batch operations instead of one operation per security

### Requirement: Scalar and batch semantic equivalence
For the same backend snapshot, the system MUST produce equivalent canonical rows, units, dtypes, ordering, pause handling, adjustment semantics, and public return shapes whether a query uses the scalar fallback or the native batch path.

#### Scenario: User forces the legacy query path
- **WHEN** a compatibility or diagnostic setting forces scalar fallback for a multi-security query
- **THEN** its public result is equivalent to the native batch result after documented dtype normalization

### Requirement: Batch capability observability
Provider diagnostics SHALL report whether each batch operation is `native` or `fallback` and SHALL expose query counts and rows read without exposing strategy secrets or credentials.

#### Scenario: User diagnoses an unexpectedly slow backtest
- **WHEN** the user inspects local provider and backtest-session diagnostics
- **THEN** the output shows whether native batching was used and how many backend queries were executed
