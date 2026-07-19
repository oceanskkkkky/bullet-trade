# Local DuckDB correctness and performance baseline

## Reference snapshot and machine

- Source root: `./data/parquet`; the daily/metadata/finance pilot discovered 9,204 source files and 5,851,283,496 bytes.
- Pilot generation: `1e4e025097494dc3`, DuckDB 1.5.4, built without publishing the active pointer.
- Pilot output: about 12.98 GB (`daily` 12.46 GB, `finance` 0.52 GB, plus metadata/manifest).
- Reference machine: Intel Core i5-12600KF, 10 cores / 16 logical processors, about 31.8 GiB RAM.
- Runtime: Conda `bullet-trade-duckdb-py310`, Python 3.10.20, pandas 2.3.3, PyArrow 25.0.0.

The source fingerprint list and exact per-table coverage live in the generation manifest. Do not compare results from different fingerprints as though they were the same snapshot.

## Materialization result

The first 8-thread/12-GB run and a 4-thread/20-GB run using the old whole-table window query both exhausted their bounded memory without publishing a generation. After replacing the whole-table precedence window with modern-table plus legacy `ANTI JOIN`, and batching the 9,194 index Parquet files, this command completed in about 33 minutes:

```powershell
bullet-trade data build-duckdb --source ./data/parquet `
  --output ./data/duckdb-pilot --temp-directory ./data/duckdb-temp `
  --threads 4 --memory-limit 20GB --max-temp-size 100GB `
  --source-file-batch-size 128 --domains meta,daily,finance,actions --no-publish
```

Validated rows:

| Dataset | Rows | Duplicate canonical keys | Coverage |
|---|---:|---:|---|
| stock_daily | 18,002,712 | 0 | 1990-12-19 to 2026-07-17 |
| fund_daily | 2,957,614 | 0 | 2009-01-05 to 2026-07-17 |
| index_daily | 14,232,827 | 0 | 1991-07-15 to 2026-07-17 |
| income | 312,335 | 0 | through 2026-07-10 |
| balance | 302,501 | 0 | through 2026-07-10 |
| cash_flow | 299,489 | 0 | through 2026-07-10 |
| indicator | 334,922 | 0 | through 2026-07-10 |

No `corporate_actions.parquet` existed in this source snapshot, so the pilot correctly contains no actions shard. Import and validate actions before using dividend-aware production backtests.

## Correctness gate

Real-data comparisons used Parquet scalar, DuckDB scalar and DuckDB native batch and required strict ordering, dtype and value parity for:

- one-stock adjusted OHLC/volume/money (243 rows);
- twenty-stock unadjusted close/volume (4,860 rows);
- 15,257 catalog rows;
- 300 CSI 300 constituent weights;
- point-in-time income rows and nanosecond date dtypes.

The real comparison exposed and fixed two issues that one-row fixtures did not catch: NumPy arrays in multi-constituent index snapshots and DuckDB microsecond financial date dtypes. Run the automated contract gate with:

```powershell
conda run -n bullet-trade-duckdb-py310 python -m pytest `
  tests/unit/test_local_data_provider.py `
  tests/unit/test_backtest_data_session.py `
  tests/unit/test_backtest_engine_order_queries.py `
  tests/unit/test_exec_and_dividends_reference.py -q --no-cov
```

An expanded Windows validation on 2026-07-19 compared the Parquet root with immutable generation `01b20d221c4928d2` (DuckDB 1.5.4). Twenty-two public-interface cases passed exact value, row/column order, index and dtype equality: trade calendar and trade-day attribution, stock/fund/index catalogs and security information, three-asset daily bars, unadjusted/pre/post single-security bars, long and panel batch bars, `get_bars`, `is_st`, index members and weights, valuation, income and continuous valuation. Representative one-shot timings while the output drive was concurrently building minute shards were:

| Public call | Parquet | DuckDB | Result |
|---|---:|---:|---|
| three-stock `get_price`, long | 3.133 s | 0.397 s | exact, 7.89x |
| three-stock `get_price`, panel | 0.631 s | 0.086 s | exact, 7.35x |
| one-stock pre-adjusted `get_price` | 0.452 s | 0.140 s | exact, 3.23x |
| one-stock post-adjusted `get_price` | 1.706 s | 0.377 s | exact, 4.53x |
| two-stock `get_extras("is_st")` | 0.317 s | 0.095 s | exact, 3.33x |

Index snapshots and continuous valuation were exact but slower on DuckDB during this run. Because the same drive was under sustained full-minute write load, those measurements are diagnostic only and must be repeated after materialization before deciding whether an SQL or immutable-snapshot cache optimization is warranted.

## Warm query benchmark

Procedure: one untimed warm-up followed by five measured calls in one process, four DuckDB threads and an 8-GB runtime limit. The workload is calendar year 2025 on the same 20 liquid stocks, `fields=["close", "volume"]`, `fq=None`, `panel=False`.

| Backend | Median | Result |
|---|---:|---|
| Parquet scalar | 5.051 s | reference |
| DuckDB scalar | 0.570 s | 8.86x faster |
| DuckDB native batch | 0.273 s | **18.49x faster** |

The optimized hot call performs two backend queries: one batched bar query and one shared trade-calendar query. Five native-batch samples were 0.430, 0.342, 0.273, 0.245 and 0.264 seconds. This exceeds the 5x twenty-security query gate.

The comparable one-stock adjusted six-field measurement before the shared-calendar optimization was 0.365 seconds for Parquet scalar and 0.159 seconds for DuckDB batch mode. End-to-end backtest and minute/concurrent gates remain separate and must not be inferred from the query benchmark.

## End-to-end daily backtest

`tests/strategies/local_batch_buy_and_hold.py` is the deterministic multi-security reference. It submits twenty target-value orders on the first open and then exercises batched current snapshots and multi-position daily valuation. Both backends used the data session and 2-GB price-block cache over 2025-01-01 through 2025-02-28.

| Backend | Engine runtime | Final value |
|---|---:|---:|
| Parquet scalar/fallback | 26.969 s | 101,303.00 |
| DuckDB native batch | 6.624 s | 101,303.00 |

The complete metrics objects were identical and the end-to-end speedup was **4.072x**, exceeding the 3x gate. The earlier one-ETF buy-and-hold run improved only from 17.558 to 16.064 seconds because fixed engine/reporting costs dominate a single-position workload; use the multi-security result to evaluate batching, and keep both workload classes in performance reports.

After vectorizing the Provider-side conversion, adjustment, pause filling and result assembly path, a strict-equality 20-stock January query (360 returned rows, seven fields) had warm scalar samples of 0.589, 0.542 and 0.561 seconds versus batch samples of 0.184, 0.188 and 0.177 seconds. Median Provider-level speedup was **3.06x** while preserving exact values, dtypes, row ordering and compatibility indexes.

`EXPLAIN ANALYZE` for three stocks in 2025 reported 0.0163 seconds, 729 output rows and a table scan of 1,313,898 rows rather than all 18,002,712 rows. The plan included pushed date and security filters; the physical `trade_date, ts_code` ordering enables date zonemap pruning.

## Minute materialization pilot

The production-path pilot scanned all 5,804 stock 1-minute files (71,420,985,556 source bytes) but retained only 2025. Output and DuckDB temporary storage were placed on separate configured drives; the paths are machine choices, not code defaults. With 4 threads, an 8-GB limit, 64 source files per batch, strict validation and `--no-publish`, it completed in 1,148.9 seconds:

| Result | Value |
|---|---:|
| Rows | 318,300,163 |
| DuckDB bytes | 4,873,269,248 |
| Canonical-key duplicates | 0 |
| Coverage | 2025-01-02 09:30 to 2025-12-31 15:00 |
| Source fingerprints | 5,804 |

Twenty real securities over one trading day returned 3,856 rows on both backends. Every OHLC, volume, amount and adjustment-factor value matched exactly; after normalizing DuckDB Arrow timestamps to `datetime64[ns]`, strict pandas frame equality also passed. Parquet scalar fallback took 4.917 seconds and one DuckDB batch query took 0.394 seconds, a **12.46x** speedup.

### Thread selection

A 200-security, 3.62-GB copied sample was materialized four times with the same 12-GB memory limit, H-class temporary drive and I-class output drive. Each run produced and strictly validated 10,004,392 rows.

| Threads | Build seconds | Output bytes |
|---:|---:|---:|
| 4 | 22.964 | 150,220,800 |
| 8 | **19.682** | 150,220,800 |
| 12 | 21.426 | 151,007,232 |
| 16 | 22.706 | 150,745,088 |

Eight threads is the selected build default for this 10-core/16-thread machine. More threads increased scheduling/compression contention without improving elapsed time. A 16-GB mixed-build memory limit leaves safer headroom than the 8-GB full pilot while preserving memory for Python and Windows.

### Sort and time-shard selection

Two physical layouts were compared on the same 10,004,392 rows. `trade_time,ts_code` used 197,144,576 bytes, 31% more than `ts_code,trade_time` at 150,220,800 bytes. Warm medians were:

| Workload | `ts_code,trade_time` | `trade_time,ts_code` |
|---|---:|---:|
| One security, one year | **10.8 ms** | 31.9 ms |
| 20 securities, one month | 16.0 ms | **16.0 ms** |
| 200 securities, one day | 19.3 ms | **11.3 ms** |

The selected primary order remains `ts_code,trade_time`: it is materially smaller and faster for security history while medium universes are equal. The engine's batched current snapshots and Arrow blocks mitigate the large-cross-section case; a second full minute copy is not justified by the measured space/performance trade-off.

For `stock_1m`, a Q2 shard was also compared with the annual shard. Quarterly warm medians improved one-security month from 2.89 to 2.56 ms, 20-security month from 18.53 to 17.72 ms, and 200-security day from 19.02 to 12.79 ms. Four quarterly files have approximately the same aggregate space as one annual file, so production uses quarterly `stock_1m` shards. Smaller ETF/index 1m datasets and all 5m/15m/30m/60m datasets remain annual to bound manifest and connection counts.

## Concurrent read budget

`scripts/benchmark_local_duckdb_concurrency.py` runs isolated Windows processes against the same immutable generation and verifies every worker's row count and pandas hash. With 20 stocks, seven fields, five repeated January queries per process, two DuckDB threads and 2 GB per process, all workers returned the same 360-row digest:

| Processes | Median query latency | Latency increase vs one process |
|---:|---:|---:|
| 1 | 0.298 s | — |
| 2 | 0.320 s | 7.3% |
| 4 | 0.337 s | 13.1% |

Four concurrent readers are stable on the reference machine when capped at two threads and 2 GB each. Process startup dominates short benchmark wall time, so capacity decisions use the in-worker warm-query samples. For long backtests, keep the same per-process cap or lower it further when more than four engines run concurrently; do not multiply the normal single-process 8-thread/12-GB runtime budget by worker count.

## Operational notes

- Domain-by-domain production builds use `--base-manifest` to compose a new immutable generation. Same-volume inherited shards are hard-linked, so adding a minute dataset does not duplicate already accepted daily/finance/action databases; cross-volume migration copies once because Windows cannot hard-link across drives. `--inherit-only` exists for that migration step and performs no source scan.
- Build memory and runtime memory are separate budgets. The current minute matrix selected 8 threads/16 GB for mixed production builds; the read-only runtime used 4 threads/4–8 GB. The older daily pilot's 4-thread/20-GB figure describes the superseded precedence implementation and is retained for audit context.
- For concurrent backtests divide threads and memory per process. Do not assign every process the whole host budget.
- The pilot was intentionally built with `--no-publish`. Point a verification Provider directly at its generation `manifest.json`, or rebuild/publish only after acceptance.
- Failed staging directories are never active. Inspect process ownership and paths before explicitly deleting them; never delete a generation referenced by `current-manifest.json` or held by a Windows process.
- The unsigned GUI-only `/tool/格式转换器最新.exe` is not a production dependency. Static inspection found an embedded DuckDB runtime, but the acceptance decision explicitly prohibits executing it or admitting its output. Only the repository-native, tested builder is accepted.
