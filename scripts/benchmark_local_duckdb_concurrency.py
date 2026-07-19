#!/usr/bin/env python3
"""Benchmark deterministic concurrent reads from one DuckDB generation."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import pandas as pd

from bullet_trade.data.providers.local import LocalDataProvider


def _worker(payload: Tuple[Dict[str, Any], Sequence[str], Dict[str, Any], int]) -> Dict[str, Any]:
    config, securities, query, repeats = payload
    provider = LocalDataProvider(config)
    provider.get_price(list(securities), **query)
    samples: List[float] = []
    frame = pd.DataFrame()
    for _ in range(repeats):
        started = time.perf_counter()
        frame = provider.get_price(list(securities), **query)
        samples.append(time.perf_counter() - started)
    digest = int(pd.util.hash_pandas_object(frame, index=True).sum())
    return {
        "pid": os.getpid(),
        "rows": len(frame),
        "digest": digest,
        "samples_seconds": samples,
        "median_seconds": statistics.median(samples),
        "total_query_seconds": sum(samples),
    }


def _parse_workers(value: str) -> List[int]:
    workers = sorted({int(item.strip()) for item in value.split(",") if item.strip()})
    if not workers or workers[0] <= 0:
        raise argparse.ArgumentTypeError("workers must contain positive integers")
    return workers


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--temp-directory", type=Path)
    parser.add_argument("--workers", type=_parse_workers, default=[1, 2, 4])
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--securities", type=int, default=20)
    parser.add_argument("--threads-per-process", type=int, default=2)
    parser.add_argument("--memory-per-process", default="2GB")
    parser.add_argument("--start", default="2025-01-02")
    parser.add_argument("--end", default="2025-01-31")
    return parser


def main() -> int:
    args = create_parser().parse_args()
    if args.repeats <= 0 or args.securities <= 0:
        raise SystemExit("repeats and securities must be positive")
    manifest = args.manifest.expanduser().resolve()
    if not manifest.is_file():
        raise SystemExit("manifest does not exist: {}".format(manifest))
    config: Dict[str, Any] = {
        "backend": "duckdb",
        "path": str(manifest),
        "query_mode": "batch",
        "threads": args.threads_per_process,
        "memory_limit": args.memory_per_process,
    }
    if args.temp_directory is not None:
        config["temp_directory"] = str(args.temp_directory.expanduser().resolve())
    probe = LocalDataProvider(config)
    securities = list(probe.get_all_securities(["stock"]).index[: args.securities])
    if len(securities) != args.securities:
        raise SystemExit(
            "requested {} securities but generation exposes {}".format(
                args.securities, len(securities)
            )
        )
    query = {
        "start_date": args.start,
        "end_date": args.end,
        "fields": ["open", "high", "low", "close", "volume", "money", "paused"],
        "fq": None,
        "fill_paused": True,
        "panel": False,
    }
    cases: List[Dict[str, Any]] = []
    for worker_count in args.workers:
        payload = (config, securities, query, args.repeats)
        started = time.perf_counter()
        if worker_count == 1:
            results = [_worker(payload)]
        else:
            with ProcessPoolExecutor(
                max_workers=worker_count, mp_context=get_context("spawn")
            ) as executor:
                results = list(executor.map(_worker, [payload] * worker_count))
        wall_seconds = time.perf_counter() - started
        digests = {item["digest"] for item in results}
        rows = {item["rows"] for item in results}
        if len(digests) != 1 or len(rows) != 1:
            raise RuntimeError("concurrent workers returned inconsistent data")
        cases.append(
            {
                "workers": worker_count,
                "wall_seconds": wall_seconds,
                "queries": worker_count * args.repeats,
                "queries_per_second": worker_count * args.repeats / wall_seconds,
                "median_worker_query_seconds": statistics.median(
                    item["median_seconds"] for item in results
                ),
                "rows_per_query": next(iter(rows)),
                "digest": next(iter(digests)),
                "workers_detail": results,
            }
        )
    print(
        json.dumps(
            {
                "manifest": str(manifest),
                "securities": securities,
                "query": query,
                "threads_per_process": args.threads_per_process,
                "memory_per_process": args.memory_per_process,
                "cases": cases,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
