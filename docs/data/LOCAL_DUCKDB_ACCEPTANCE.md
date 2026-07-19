# Local DuckDB acceptance map

This document maps every scenario in OpenSpec change `enhance-local-provider-parity-and-duckdb` to repeatable evidence. Automated evidence uses the named pytest test; measured evidence is recorded in `LOCAL_DUCKDB_BENCHMARK.md`. A scenario marked as a design decision is intentionally unavailable rather than an untested runtime path.

## Provider capability contract

| Scenario | Evidence |
|---|---|
| Strategy changes storage backend | `test_duckdb_three_way_public_price_parity`; the same strategy is also used by the Parquet/DuckDB end-to-end benchmark. |
| User checks compatibility before a backtest | `test_local_provider_capability_schema_and_query_diagnostics`. |
| Strategy requests tick data | `test_local_provider_live_and_tick_operations_fail_explicitly` verifies typed failures for tick and subscription operations. |
| Required local data is absent | `test_local_provider_reports_configuration_and_schema_errors` and `test_local_provider_missing_corporate_actions_is_actionable`. |
| Supported JoinQuant-style query | The local-provider read, adjustment, index, fundamentals, valuation and corporate-action tests in `tests/unit/test_local_data_provider.py`. |
| Existing backend does not override batch reads | `test_old_backend_subclass_gets_correct_batch_fallback`. |
| DuckDB advertises native batching | `test_duckdb_native_batch_count_and_query_bound` and diagnostics assertions. |
| User forces the legacy query path | `test_local_provider_scalar_and_batch_price_contracts_are_equal` and the parameterized three-way parity tests. |
| User diagnoses an unexpectedly slow backtest | `test_local_provider_capability_schema_and_query_diagnostics`; query counts, rows, timings and cache statistics are also captured in benchmark/run manifests. |

## Corporate actions

| Scenario | Evidence |
|---|---|
| Stock cash-and-stock dividend is normalized | `test_tushare_corporate_action_normalizers`. |
| Fund cash distribution is normalized | `test_tushare_corporate_action_normalizers`. |
| User imports a date range | `test_tushare_import_is_offline_queryable_idempotent_and_correctable` plus the production import recorded below. |
| The same Tushare window is imported twice | `test_tushare_import_is_offline_queryable_idempotent_and_correctable`. |
| Query spans multiple events | `test_local_provider_reads_canonical_corporate_actions`. |
| Corporate-action dataset is missing | `test_local_provider_missing_corporate_actions_is_actionable` and `test_local_backtest_preflight_requires_actions_and_accepts_explicit_opt_out`. |
| Dividend proposal is cancelled | `test_tushare_corporate_action_normalizers` and the rejected canonical fixture row. |
| Portfolio holds many dividend-paying securities | `test_engine_batch_event_calendar_cache_is_generation_isolated` and batched engine regression tests. |
| Two backtests use different data generations | `test_engine_batch_event_calendar_cache_is_generation_isolated`. |

## DuckDB backend and builder

| Scenario | Evidence |
|---|---|
| Equivalent price query | `test_duckdb_three_way_public_price_parity` and `test_duckdb_three_way_adjustment_and_dtype_parity`. |
| Fully materialized backtest starts | Daily and minute end-to-end runs use explicit immutable manifests; backend diagnostics assert `fully_materialized=true` and `parquet_fallback=false`. |
| A required materialized shard is missing | `test_duckdb_manifest_errors_are_typed_and_preflight_preserves_active` and `test_duckdb_backtest_preflight_rejects_missing_frequency_shard`. |
| Materialized database build | `test_duckdb_builder_manifest_precedence_and_safe_rerun` plus the production materializations recorded below. |
| External-view database build | Design decision: no external-view selector is exposed. The accepted runtime is materialized-only; diagnostic comparisons use the Parquet backend directly. |
| Modern and legacy stock history overlap | `test_duckdb_builder_manifest_precedence_and_safe_rerun`. |
| Thousands of per-security minute files are imported | The full 5,804-file stock 1-minute pilot and complete production build; `test_duckdb_minute_builds_year_shards_and_queries_only_overlap` covers bounded sharding. |
| A new generation is published on Windows | `test_duckdb_builder_manifest_precedence_and_safe_rerun`; real builds use atomic `current-manifest.json` publication. |
| An old backtest remains active | Immutable generation files and pointer-only publication are exercised by safe rerun/base-generation tests; Windows builds never replace an open database file. |
| Daily cross-section is queried | The recorded `EXPLAIN ANALYZE` plan demonstrates date/security predicate pushdown and zonemap pruning. |
| Minute history is queried | `test_duckdb_minute_builds_year_shards_and_queries_only_overlap`, quarterly-shard test and minute workload benchmark. |
| Validation fails during refresh | `test_duckdb_manifest_errors_are_typed_and_preflight_preserves_active`. |
| Provider opens a stale database | The incompatible schema case in `test_duckdb_manifest_errors_are_typed_and_preflight_preserves_active`. |
| Many securities share one daily request | `test_provider_batch_reuses_one_trade_calendar` and `test_duckdb_native_batch_count_and_query_bound`. |
| Per-security count is requested | `test_duckdb_native_batch_count_and_query_bound` and the cross-year count assertion in the minute-shard test. |
| Full build requires spill space | Builder preflight failure test plus production builds using an explicitly separate H-volume temp directory and bounded memory. |
| Multiple backtests run concurrently | `scripts/benchmark_local_duckdb_concurrency.py`; 1/2/4-process results and resource caps are recorded in the benchmark document. |
| Bundled converter produces a sample database | Design decision: the unsigned converter was statically reviewed and explicitly rejected without execution. No converter output is admissible; the repository builder remains authoritative. |
| Windows user selects DuckDB | `test_duckdb_builder_cli_configuration_is_relative`, relative-path Provider test, environment examples and real Windows builds. |

## Local backtest workflow

| Scenario | Evidence |
|---|---|
| User runs an existing strategy locally | `DATA_PROVIDER_LOCAL.md` and the end-to-end local strategy runs. No JQData/Tushare call occurs during strategy queries. |
| Strategy requires missing corporate actions | `test_local_backtest_preflight_requires_actions_and_accepts_explicit_opt_out`. |
| Fully materialized generation lacks coverage | `test_duckdb_backtest_preflight_rejects_missing_frequency_shard`; coverage is checked before the engine obtains its trading calendar. |
| User compares two reports | Backtest data-session manifests record backend, generation, resource and query/cache diagnostics; Parquet/DuckDB metrics are compared in the benchmark document. |
| Local provider regression test runs | The focused offline suite and deterministic daily/minute acceptance strategies. |
| User changes backend setting | `test_duckdb_three_way_public_price_parity` and the same-source end-to-end Parquet/DuckDB runs require no strategy edit. |
| Portfolio contains many positions | `test_backtest_update_positions_fetches_securities_in_one_batch`. |
| Multiple orders execute on one bar | `test_backtest_order_records_requested_and_fill_price` plus current-snapshot preload/reuse tests. |
| Daily strategy repeatedly requests count windows | `test_data_api_price_block_cache_reuses_single_security_count_window` and `test_multi_security_arrow_block_is_generation_isolated_and_reused`. |
| Minute data exceeds memory budget | `test_backtest_session_enforces_memory_budget`, deterministic LRU eviction and minute date-boundary tests. |
| User switches manifest generation | `test_multi_security_arrow_block_is_generation_isolated_and_reused`, Provider-independent cache test and event-calendar generation test. |
| Batch parity regression is suspected | Scalar/batch parity tests and the documented `LOCAL_DATA_QUERY_MODE=scalar` rollback. |
| Twenty-security daily batch benchmark | Query and Provider benchmarks in `LOCAL_DUCKDB_BENCHMARK.md`; parity is asserted before timing. |
| Representative daily backtest benchmark | Same-strategy Parquet/DuckDB result comparison and the recorded 4.072x engine speedup. |
| Correctness and speed conflict | All performance results are accepted only after exact frame or portfolio equality; three-way exact dtype/value tests enforce the rule. |

## Production verification record

The final production generation record is appended only after all selected shards, full Tushare actions, daily/minute end-to-end reports and aggregate manifest validation complete. OpenSpec task 8.7 remains incomplete until that record exists and the strict OpenSpec validator passes.
