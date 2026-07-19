## ADDED Requirements

### Requirement: Documented local backtest configuration
The system SHALL document environment-variable, configuration-file, CLI, and Python-API examples for selecting `provider=local`, setting a relative data path, and choosing Parquet or DuckDB.

#### Scenario: User runs an existing strategy locally
- **WHEN** the user follows the documented local-provider configuration for an existing compatible strategy
- **THEN** the backtest uses only the selected local backend without requiring JQData authentication

### Requirement: Preflight validation
The local backtest workflow SHALL validate backend dependencies, schema versions, required datasets, requested date coverage, security coverage, and strategy-required provider capabilities before simulation begins.

#### Scenario: Strategy requires missing corporate actions
- **WHEN** preflight detects that the strategy requires `get_split_dividend()` but no valid corporate-action dataset exists
- **THEN** the backtest stops before simulation with an actionable preparation command

### Requirement: Reproducible run metadata
Each local backtest SHALL record the selected backend, resolved non-secret data location, schema version, source fingerprints or manifest identity, and capability diagnostics in its run metadata.

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
