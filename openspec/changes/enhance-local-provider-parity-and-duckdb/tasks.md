## 1. Freeze contracts and performance baselines

- [x] 1.1 Capture Parquet golden results for scalar prices, counts, adjustments, pause filling, catalogs, index data, fundamentals, and existing error behavior.
- [ ] 1.2 Capture deterministic end-to-end baseline reports with orders, fills, cash, positions, dividends, and key net-value points for representative daily and minute strategies.
- [x] 1.3 Add backend query-count, rows-read, cold/warm latency, peak-memory, and cache-stat instrumentation without changing public strategy APIs.
- [x] 1.4 Record the reference one-security and twenty-security Parquet benchmarks, fixture identity, machine resources, and benchmark procedure.
- [x] 1.5 Add typed capability, missing-shard, incompatible-generation, and unsupported-operation errors to the local-data contract.
- [x] 1.6 Define one versioned canonical dataset/schema registry shared by Parquet validation, DuckDB construction, diagnostics, and tests.
- [x] 1.7 Implement `LocalDataProvider.capabilities()` and diagnostics for supported/limited/unsupported operations, manifest identity, batch mode, and query statistics.

## 2. Add non-breaking batch backend contracts

- [x] 2.1 Define immutable homogeneous batch request/result models for bars, current snapshots, reference factors, security information, and corporate actions.
- [x] 2.2 Add non-abstract batch methods to `LocalDataBackend` with correct scalar-loop defaults so existing backend subclasses remain valid.
- [x] 2.3 Update `LocalDataProvider.get_price()` to group compatible multi-security requests and use the batch contract while preserving scalar and public return semantics.
- [x] 2.4 Vectorize provider-side code/unit conversion, adjustment, pause filling, field selection, ordering, and panel/long-shape assembly over canonical batch rows.
- [x] 2.5 Add an explicit `batch`/`scalar` diagnostic query-mode setting with no silent mode switching.
- [x] 2.6 Add scalar-versus-batch contract tests for mixed assets, empty securities, missing rows, date bounds, per-security count, and all supported return shapes.
- [x] 2.7 Preserve Parquet golden and project tests with the default batch fallback before implementing native DuckDB batching.

## 3. Add canonical corporate actions

- [x] 3.1 Define the versioned `corporate_actions` schema, event identity, implementation-status policy, and stock/fund normalization helpers.
- [x] 3.2 Extend `ParquetDataBackend` with scalar and fallback-batch corporate-action reads and actionable missing-dataset/schema errors.
- [x] 3.3 Implement a Tushare importer for `dividend` and `fund_div` that resolves security type from catalogs, batches/rate-limits requests, preserves audit dates, and reports rejected rows.
- [x] 3.4 Implement idempotent staging and merge/write behavior with source watermarks, payload hashes, correction handling, and atomic Parquet publication.
- [x] 3.5 Replace `LocalDataProvider.get_split_dividend()`'s unconditional empty list with canonical offline querying and JoinQuant-compatible normalization.
- [x] 3.6 Add fixture tests for cash dividends, bonus/transfer shares, fund distributions, cancelled proposals, date boundaries, duplicate imports, corrections, and missing datasets.
- [x] 3.7 Add a mocked-Tushare integration test and a generation-isolated batch event-calendar test with no network access during query.

## 4. Build the authoritative materialization pipeline

- [x] 4.1 Add a pinned compatible DuckDB optional dependency and precise installation/version errors for builder and backend selection.
- [x] 4.2 Define bounded canonical DuckDB schemas and shard rules for metadata, daily data, fundamentals, actions, and asset/frequency/time-partitioned minute data.
- [x] 4.3 Implement deterministic consolidation of per-security files and modern-versus-legacy stock daily precedence without one-table-per-file mirroring.
- [x] 4.4 Implement resource-aware CTAS/COPY materialization with configurable threads, memory, temporary directory, temporary-space limit, and `preserve_insertion_order` behavior.
- [x] 4.5 Apply and record query-oriented physical sort keys; add plan/profiling checks that demonstrate expected shard and zonemap pruning.
- [x] 4.6 Generate immutable version-addressed shards and a generation manifest containing schema/storage versions, fingerprints, coverage, sort keys, row counts, duplicate counts, and validation results.
- [x] 4.7 Implement staging validation and atomic active-manifest publication that never replaces database files held open by a Windows backtest process.
- [x] 4.8 Implement domain/time-shard rebuilds, safe reruns, stale staging detection, old-generation retention, and explicit generation cleanup guidance.
- [x] 4.9 Add disk/memory/temp-space preflight and failure tests that preserve the active generation when a build cannot complete.
- [x] 4.10 Record the static evaluation and explicit rejection of the unsigned `/tool/格式转换器最新.exe`; do not execute it or admit its output, and keep the repository-native builder as the only accepted production path.
- [x] 4.11 Add build tests for row counts, min/max dates, canonical key uniqueness, representative row hashes, null/type invariants, and source-file non-modification.

## 5. Implement the native DuckDB backend

- [x] 5.1 Implement manifest loading, schema/storage compatibility checks, date/asset/frequency shard selection, and long-lived read-only connection management.
- [x] 5.2 Implement scalar DuckDB reads for bars, calendars, securities, index components, fundamentals, and corporate actions with canonical dtype/error behavior.
- [x] 5.3 Implement native batched daily/minute bar queries using parameterized security sets, projection/filter pushdown, bounded date ranges, and exact per-security count semantics.
- [x] 5.4 Implement native batch current-snapshot, reference-factor, security-information, and corporate-action queries.
- [x] 5.5 Return Arrow readers/tables internally and defer pandas conversion until the Provider boundary; add tests for chunking and result lifetime.
- [x] 5.6 Add per-process thread, memory, temp-directory, and temp-space configuration plus diagnostic reporting of effective DuckDB settings.
- [x] 5.7 Extend the provider factory/environment loader for a relative manifest path, fully materialized profile, and explicit scalar/batch mode while preserving Parquet defaults and aliases.
- [x] 5.8 Add three-way Parquet scalar, DuckDB scalar, and DuckDB native-batch contract tests for dtypes, ordering, units, adjustments, pauses, errors, and query results.

## 6. Batch the backtest engine and data session

- [x] 6.1 Change end-of-day position valuation to fetch all compatible held-security closes in batch and retain identical portfolio update order/results.
- [x] 6.2 Reuse the current-bar snapshot for order execution-price resolution and batch any genuinely missing execution fields instead of querying once per order.
- [x] 6.3 Batch daily corporate-action retrieval for all positions and preserve eligibility, suspension-delay, tax, and event-deduplication semantics.
- [x] 6.4 Add internal `BacktestCurrentData.preload(securities)` and share its generation-keyed snapshot across strategy access, order processing, and valuation.
- [x] 6.5 Extend `BacktestDataSession` with multi-security Arrow blocks keyed by manifest identity, frequency, fields, adjustment/pause semantics, and time coverage.
- [x] 6.6 Implement daily-range prefetch and minute date/security block loading with deterministic LRU eviction, maximum-memory, and minimum-free-memory guards.
- [x] 6.7 Include current bars, price blocks, reference factors, fundamentals, and corporate actions in generation-isolated cache rules and statistics.
- [x] 6.8 Record batch/fallback usage, backend query count, cache hit/miss/eviction, loaded rows, and degradation reasons in backtest manifests and reports.
- [x] 6.9 Add regression tests for future-data isolation, event order, repeated `get_current_data()`, multi-order bars, dynamic universes, and cache-budget degradation.

## 7. Materialize production datasets and tune performance

- [x] 7.1 Build and validate a small daily/finance/actions pilot generation on a configurable data root; compare all golden queries and reports before scaling.
- [x] 7.2 Benchmark 4/8/12/16 threads, bounded memory settings, candidate data drives, and separate temp drives; select documented defaults for the reference machine.
- [ ] 7.3 Materialize and validate complete metadata, daily, fundamentals, and corporate-action shards, then run native batch and end-to-end daily benchmarks.
- [x] 7.4 Choose minute shard period and physical ordering from representative single-security, active-universe, and cross-sectional minute workloads.
- [ ] 7.5 Materialize all supported minute asset/frequency/time shards with resumable generation tracking and aggregate validation.
- [x] 7.6 Verify the twenty-security one-year query targets at least fivefold speedup over the recorded scalar Parquet baseline or document the blocking profile evidence.
- [x] 7.7 Verify the representative daily backtest targets at least threefold end-to-end speedup with equivalent orders, fills, cash, positions, actions, and net value.
- [x] 7.8 Run minute and concurrent-read benchmarks, enforce per-process resource budgets, and record any workloads that should use different shard/cache settings.

## 8. Documentation and final verification

- [x] 8.1 Update the local provider guide with the Local/JQData matrix, native/fallback batch modes, fully materialized runtime, and explicit no-Parquet-fallback behavior.
- [x] 8.2 Document Tushare corporate-action prerequisites, mappings, permission/rate-limit failures, import commands, and why adjustment factors cannot replace events.
- [x] 8.3 Document canonical shards, manifest generations, build/rebuild/cleanup commands, storage estimates, Windows file-lock behavior, and rollback choices.
- [x] 8.4 Document environment, Python API, preflight, resource tuning, Arrow cache settings, CLI backtest, benchmark reproduction, and troubleshooting examples.
- [x] 8.5 Update sample environment files without hardcoded drive letters and explain external data roots or managed directory junctions.
- [x] 8.6 Run focused unit/integration tests, three-way contract tests, offline smoke tests, performance gates, formatting/type checks, and the project suite; record unrelated pre-existing failures.
- [ ] 8.7 Validate all OpenSpec artifacts and confirm every acceptance scenario maps to an automated test or explicit benchmark/manual verification.
