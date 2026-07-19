## ADDED Requirements

### Requirement: Canonical corporate-action dataset
The system SHALL define a versioned corporate-action schema containing at least security code, event date, security type, scale factor, pre-tax cash bonus, per-base unit, source, source event identity, implementation status, and source update metadata. It SHALL retain announcement, record, payment, and listing dates when supplied.

#### Scenario: Stock cash-and-stock dividend is normalized
- **WHEN** an implemented stock dividend record contains cash, bonus-share, and capitalization ratios
- **THEN** the canonical event stores a per-ten-share pre-tax cash bonus and a scale factor derived from all share-distribution ratios

#### Scenario: Fund cash distribution is normalized
- **WHEN** an implemented ETF or fund dividend record contains a per-unit cash distribution
- **THEN** the canonical event stores `per_base` as one, the per-unit pre-tax cash bonus, and a scale factor of one unless a split ratio is present

### Requirement: Explicit Tushare ingestion
The system SHALL provide an explicit preparation command that reads stock corporate actions from Tushare `dividend` and fund corporate actions from `fund_div`, normalizes only usable implemented events, and writes them to the configured local dataset.

#### Scenario: User imports a date range
- **WHEN** the user runs the corporate-action preparation command with Tushare credentials and a date range
- **THEN** the command writes normalized local events, reports rejected or incomplete rows, and records the source watermark

### Requirement: Idempotent incremental updates
Corporate-action ingestion MUST deduplicate source events by a stable source identity and MUST update corrected records without producing duplicate economic events.

#### Scenario: The same Tushare window is imported twice
- **WHEN** an already imported date range is imported again
- **THEN** the canonical result contains one current version of each source event

### Requirement: Offline split-dividend query
`LocalDataProvider.get_split_dividend()` SHALL read only the canonical local corporate-action dataset and SHALL return events in ascending event-date order using the existing fields `security`, `date`, `security_type`, `scale_factor`, `bonus_pre_tax`, and `per_base`.

#### Scenario: Query spans multiple events
- **WHEN** the local dataset contains multiple eligible events for the requested security and date interval
- **THEN** the method returns every matching normalized event once in ascending date order

#### Scenario: Corporate-action dataset is missing
- **WHEN** `get_split_dividend()` is called but the backend has no corporate-action dataset
- **THEN** the provider raises an actionable capability error instead of returning an unconditional empty list

### Requirement: Point-in-time safe event selection
The importer SHALL retain announcement and implementation metadata needed to audit look-ahead risk, and the query layer SHALL exclude cancelled, pre-disclosure, or otherwise non-effective proposals from executable events.

#### Scenario: Dividend proposal is cancelled
- **WHEN** a source row is marked cancelled or not implemented
- **THEN** it is not returned as an executable split-dividend event

### Requirement: Batch corporate-action execution path
The local backend SHALL support one batch query for a set of securities and a date interval, while `LocalDataProvider.get_split_dividend()` SHALL retain its existing single-security public signature. The backtest engine SHALL reuse the batch result for all held securities in that trading-day evaluation.

#### Scenario: Portfolio holds many dividend-paying securities
- **WHEN** the engine evaluates corporate actions for multiple positions on one trading day
- **THEN** it issues one native batch backend query per compatible data partition rather than one query per position

### Requirement: Event-calendar cache consistency
The backtest data session SHALL cache normalized effective events by dataset identity, security, and event date, and MUST invalidate or isolate that cache when the selected manifest generation changes.

#### Scenario: Two backtests use different data generations
- **WHEN** two backtests run against different immutable manifest identities
- **THEN** neither run can observe corporate-action cache entries from the other generation
