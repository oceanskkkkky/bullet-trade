"""Explicit local-data preparation commands."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, Set, Tuple

import pandas as pd

from bullet_trade.data.local import (
    AssetType,
    DuckDBBuildConfig,
    DuckDBMaterializer,
    ParquetDataBackend,
    TushareCorporateActionImporter,
    security_type_map_from_catalogs,
)
from bullet_trade.data.providers.local import LocalDataProvider


def _create_tushare_client(token: str) -> Any:
    try:
        import tushare as ts
    except ImportError as exc:
        raise RuntimeError("缺少 Tushare SDK；请执行 `pip install bullet-trade[tushare]`") from exc
    ts.set_token(token)
    return ts.pro_api(token)


def _parse_years(value: str) -> Tuple[int, ...]:
    years: Set[int] = set()
    for item in (part.strip() for part in str(value or "").split(",")):
        if not item:
            continue
        if "-" in item:
            start_text, end_text = item.split("-", 1)
            start, end = int(start_text), int(end_text)
            if end < start:
                raise ValueError("分钟年份范围无效：{}".format(item))
            years.update(range(start, end + 1))
        else:
            years.add(int(item))
    return tuple(sorted(years))


def run_data_command(args: Any) -> int:
    """Dispatch a local-data preparation subcommand."""

    if args.data_command == "build-duckdb":
        source = LocalDataProvider._resolve_data_root(args.source)
        output = LocalDataProvider._resolve_data_root(args.output)
        temp = (
            LocalDataProvider._resolve_data_root(args.temp_directory)
            if args.temp_directory
            else None
        )
        base_manifest = (
            LocalDataProvider._resolve_data_root(args.base_manifest)
            if args.base_manifest
            else None
        )
        builder = DuckDBMaterializer(
            DuckDBBuildConfig(
                source_root=source,
                output_root=output,
                base_manifest=base_manifest,
                temp_directory=temp,
                threads=args.threads,
                memory_limit=args.memory_limit,
                max_temp_directory_size=args.max_temp_size,
                source_file_batch_size=args.source_file_batch_size,
                domains=(
                    ()
                    if args.inherit_only
                    else tuple(
                        item.strip() for item in args.domains.split(",") if item.strip()
                    )
                ),
                datasets=tuple(item.strip() for item in args.datasets.split(",") if item.strip()),
                minute_years=_parse_years(args.minute_years),
                minute_quarterly_datasets=tuple(
                    item.strip()
                    for item in args.minute_quarterly_datasets.split(",")
                    if item.strip()
                ),
                resume=args.resume,
                retained_generations=args.retain_generations,
            )
        )
        if args.dry_run:
            print(json.dumps(builder.preflight().__dict__, ensure_ascii=False, indent=2))
            return 0
        manifest = builder.build(publish=not args.no_publish)
        if args.print_full_manifest:
            build_payload = manifest.as_dict()
        else:
            build_payload = {
                "generation_id": manifest.generation_id,
                "build_timestamp": manifest.build_timestamp,
                "duckdb_version": manifest.duckdb_version,
                "validation_passed": manifest.validation_passed,
                "source_fingerprint_count": len(manifest.source_fingerprints),
                "shard_count": len(manifest.shards),
                "datasets": sorted(
                    {dataset for shard in manifest.shards for dataset in shard.datasets}
                ),
                "shards": [
                    {
                        "shard_id": shard.shard_id,
                        "path": shard.path,
                        "row_counts": dict(shard.row_counts),
                        "coverage": dict(shard.coverage),
                        "validations": dict(shard.validations),
                    }
                    for shard in manifest.shards
                ],
                "manifest": str(output / "generations" / manifest.generation_id / "manifest.json"),
                "published": not args.no_publish,
            }
        print(json.dumps(build_payload, ensure_ascii=False, indent=2))
        return 0
    if args.data_command == "cleanup-duckdb":
        output = LocalDataProvider._resolve_data_root(args.output)
        builder = DuckDBMaterializer(
            DuckDBBuildConfig(
                source_root=LocalDataProvider._resolve_data_root("./data/parquet"),
                output_root=output,
                retained_generations=args.keep_generations,
            )
        )
        candidates = builder.retained_generation_candidates(args.keep_generations)
        if args.apply:
            generations = (output / "generations").resolve()
            for candidate in candidates:
                resolved = candidate.resolve()
                if resolved.parent != generations or resolved.name.startswith("."):
                    raise RuntimeError("拒绝清理非 generation 目录：{}".format(resolved))
                shutil.rmtree(resolved)
        print(
            json.dumps(
                {
                    "applied": bool(args.apply),
                    "candidates": [str(path) for path in candidates],
                    "guidance": "先关闭引用旧 generation 的 Windows 回测进程，再使用 --apply。",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if args.data_command != "import-corporate-actions":
        raise ValueError("未知 data 子命令：{}".format(args.data_command))
    token = args.token or os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("未配置 TUSHARE_TOKEN，也未传入 --token")
    root = LocalDataProvider._resolve_data_root(args.root)
    backend = ParquetDataBackend(root, strict=False)
    backend.validate()
    stocks = backend.read_securities((AssetType.STOCK,)).index.astype(str).tolist()
    funds = backend.read_securities((AssetType.FUND,)).index.astype(str).tolist()
    type_map = security_type_map_from_catalogs(stocks, funds)
    selected = args.securities.split(",") if args.securities else None
    output = Path(args.output).expanduser() if args.output else root / "corporate_actions.parquet"
    if not output.is_absolute():
        output = LocalDataProvider._resolve_data_root(output)
    importer = TushareCorporateActionImporter(
        _create_tushare_client(token),
        type_map,
        requests_per_minute=args.requests_per_minute,
    )
    checkpoint = Path(args.checkpoint).expanduser() if args.checkpoint else None
    if checkpoint is not None and not checkpoint.is_absolute():
        checkpoint = LocalDataProvider._resolve_data_root(checkpoint)
    report = importer.publish_resumable(
        output,
        selected,
        start=pd.Timestamp(args.start) if args.start else None,
        end=pd.Timestamp(args.end) if args.end else None,
        checkpoint_path=checkpoint,
        checkpoint_every=args.checkpoint_every,
        resume=args.resume,
    )
    report_payload: Dict[str, Any] = dict(report.__dict__)
    rejected_reasons = list(report.rejected_reasons)
    if args.print_all_rejections:
        report_payload["rejected_reasons"] = rejected_reasons
    else:
        report_payload["rejected_reason_examples"] = rejected_reasons[:20]
        report_payload["rejected_reasons_omitted"] = max(len(rejected_reasons) - 20, 0)
        report_payload.pop("rejected_reasons", None)
    print(json.dumps(report_payload, ensure_ascii=False, indent=2))
    return 0
