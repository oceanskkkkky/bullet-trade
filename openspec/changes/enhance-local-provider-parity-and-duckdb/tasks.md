## 1. Stabilize the local provider contract

- [ ] 1.1 Add typed capability errors, immutable corporate-action request types, and storage-neutral capability metadata to `bullet_trade/data/local/base.py`.
- [ ] 1.2 Define a single canonical dataset/schema registry shared by Parquet validation, DuckDB construction, diagnostics, and tests.
- [ ] 1.3 Implement `LocalDataProvider.capabilities()` and extend diagnostics with supported/limited/unsupported status, dataset coverage, and schema identity.
- [ ] 1.4 Override inherited unsupported and subscription methods so they raise actionable typed errors instead of returning plausible empty values or no-ops.
- [ ] 1.5 Add contract tests covering every public `DataProvider` method and the documented Local/JQData compatibility classification.

## 2. Add local corporate actions

- [ ] 2.1 Define the versioned canonical `corporate_actions` schema, validation rules, event identity, implementation-status policy, and stock/fund normalization helpers.
- [ ] 2.2 Extend `ParquetDataBackend` with filtered corporate-action reads and clear missing-dataset/schema errors.
- [ ] 2.3 Implement a Tushare importer for `dividend` and `fund_div` that resolves security type from catalogs, batches/rate-limits requests, preserves audit dates, and reports rejected rows.
- [ ] 2.4 Implement idempotent staging and merge/write behavior with source watermarks, payload hashes, correction handling, and atomic Parquet replacement.
- [ ] 2.5 Replace `LocalDataProvider.get_split_dividend()`'s unconditional empty list with canonical offline querying and JoinQuant-compatible result normalization.
- [ ] 2.6 Add fixture tests for cash dividends, bonus/transfer shares, fund distributions, cancelled proposals, date boundaries, duplicate imports, corrections, and missing datasets.
- [ ] 2.7 Add an integration test that imports mocked Tushare responses and verifies the resulting local split-dividend events without network access during query.

## 3. Build the DuckDB storage backend

- [ ] 3.1 Add DuckDB as an optional local-data dependency and provide a precise installation error when the backend or builder is selected without it.
- [ ] 3.2 Implement the Parquet-to-DuckDB builder for external-view and materialized modes using the canonical schema registry and parameter-safe path handling.
- [ ] 3.3 Add `bt_manifest` generation with schema version, source fingerprints, row counts, date coverage, build mode, and build timestamp.
- [ ] 3.4 Implement temporary-database construction, validation, connection cleanup, and atomic target replacement with Windows-specific failure tests.
- [ ] 3.5 Implement `DuckDBDataBackend` reads for bars, calendars, securities, index components, fundamentals, and corporate actions with predicate/projection pushdown.
- [ ] 3.6 Extend the local provider factory and environment loader to accept `LOCAL_DATA_BACKEND=duckdb` and a project-relative `.duckdb` `LOCAL_DATA_PATH` while preserving Parquet defaults and aliases.
- [ ] 3.7 Add cross-backend contract tests that compare canonical dtypes, ordering, errors, and query results for the same fixture datasets.
- [ ] 3.8 Benchmark representative single-security bars, broad date scans, index joins, and point-in-time fundamentals against Parquet; document when views or materialized mode is preferable.

## 4. Integrate the local backtest workflow

- [ ] 4.1 Add a local-data preflight command/service that validates dependencies, manifest/schema versions, date/security coverage, and explicitly required provider capabilities.
- [ ] 4.2 Wire preflight into local backtest startup without changing startup behavior for other providers.
- [ ] 4.3 Record the backend, resolved non-secret path, schema/manifest identity, fingerprints, coverage, and capability diagnostics in backtest run metadata and reports.
- [ ] 4.4 Add a deterministic strategy fixture that reads prices/reference data, places an order, and completes an offline backtest report.
- [ ] 4.5 Run the smoke strategy against Parquet and DuckDB from the same snapshot and assert equivalent orders, positions, cash, and key net-value points.

## 5. Documentation and final verification

- [ ] 5.1 Update the local provider guide with the complete Local/JQData interface matrix and make current limitations distinct from newly implemented capabilities.
- [ ] 5.2 Document Tushare corporate-action prerequisites, field mappings, permissions/rate-limit failure handling, import examples, and the fact that `adj_factor` cannot replace event data.
- [ ] 5.3 Document Parquet-to-DuckDB commands, views/materialized trade-offs, disk implications, stale-manifest recovery, Windows relative paths, and rollback to Parquet.
- [ ] 5.4 Document end-to-end environment, Python API, preflight, CLI backtest, backend-switch, and troubleshooting examples; update sample environment files.
- [ ] 5.5 Run focused unit/integration tests, the dual-backend offline smoke test, formatting/type checks, and the project test suite; record any unrelated pre-existing failures.
- [ ] 5.6 Validate all OpenSpec artifacts and confirm every acceptance scenario maps to an automated test or an explicit manual verification step.
