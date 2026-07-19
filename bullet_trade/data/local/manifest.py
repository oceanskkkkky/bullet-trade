"""Immutable DuckDB generation manifests and atomic active pointers."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple, cast

from .base import (
    LocalDataConfigurationError,
    LocalDataIncompatibleGenerationError,
    LocalDataMissingShardError,
)
from .schemas import CANONICAL_SCHEMA_VERSION, DUCKDB_STORAGE_VERSION

MANIFEST_FORMAT_VERSION = 1


@dataclass(frozen=True)
class ShardManifest:
    """One immutable physical DuckDB shard."""

    shard_id: str
    domain: str
    path: str
    datasets: Tuple[str, ...]
    sort_keys: Mapping[str, Tuple[str, ...]]
    row_counts: Mapping[str, int]
    duplicate_counts: Mapping[str, int]
    coverage: Mapping[str, Mapping[str, Optional[str]]]
    null_counts: Mapping[str, Mapping[str, int]]
    column_types: Mapping[str, Mapping[str, str]]
    representative_hashes: Mapping[str, str]
    validations: Mapping[str, bool]
    partition: Mapping[str, Optional[str]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ShardManifest":
        return cls(
            shard_id=str(value["shard_id"]),
            domain=str(value["domain"]),
            path=str(value["path"]),
            datasets=tuple(value.get("datasets", ())),
            sort_keys={str(key): tuple(items) for key, items in value.get("sort_keys", {}).items()},
            row_counts={str(key): int(item) for key, item in value.get("row_counts", {}).items()},
            duplicate_counts={
                str(key): int(item) for key, item in value.get("duplicate_counts", {}).items()
            },
            coverage=dict(value.get("coverage", {})),
            null_counts={
                str(dataset): {str(column): int(count) for column, count in counts.items()}
                for dataset, counts in value.get("null_counts", {}).items()
            },
            column_types={
                str(dataset): {str(column): str(dtype) for column, dtype in types.items()}
                for dataset, types in value.get("column_types", {}).items()
            },
            representative_hashes={
                str(dataset): str(digest)
                for dataset, digest in value.get("representative_hashes", {}).items()
            },
            validations={
                str(key): bool(item) for key, item in value.get("validations", {}).items()
            },
            partition={
                str(key): None if item is None else str(item)
                for key, item in value.get("partition", {}).items()
            },
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "domain": self.domain,
            "path": self.path,
            "datasets": list(self.datasets),
            "sort_keys": {key: list(value) for key, value in self.sort_keys.items()},
            "row_counts": dict(self.row_counts),
            "duplicate_counts": dict(self.duplicate_counts),
            "coverage": dict(self.coverage),
            "null_counts": {dataset: dict(counts) for dataset, counts in self.null_counts.items()},
            "column_types": {dataset: dict(types) for dataset, types in self.column_types.items()},
            "representative_hashes": dict(self.representative_hashes),
            "validations": dict(self.validations),
            "partition": dict(self.partition),
        }


@dataclass(frozen=True)
class GenerationManifest:
    """Pinned identity and complete shard set for a reproducible run."""

    generation_id: str
    build_timestamp: str
    source_root: str
    source_fingerprints: Tuple[Mapping[str, Any], ...]
    duckdb_version: str
    build_mode: str
    build_config: Mapping[str, Any]
    shards: Tuple[ShardManifest, ...]
    validation_passed: bool
    schema_version: str = CANONICAL_SCHEMA_VERSION
    storage_version: str = DUCKDB_STORAGE_VERSION
    manifest_version: int = MANIFEST_FORMAT_VERSION

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "GenerationManifest":
        return cls(
            generation_id=str(value["generation_id"]),
            build_timestamp=str(value["build_timestamp"]),
            source_root=str(value["source_root"]),
            source_fingerprints=tuple(value.get("source_fingerprints", ())),
            duckdb_version=str(value["duckdb_version"]),
            build_mode=str(value["build_mode"]),
            build_config=dict(value.get("build_config", {})),
            shards=tuple(ShardManifest.from_dict(item) for item in value.get("shards", ())),
            validation_passed=bool(value.get("validation_passed", False)),
            schema_version=str(value.get("schema_version", "")),
            storage_version=str(value.get("storage_version", "")),
            manifest_version=int(value.get("manifest_version", 0)),
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "manifest_version": self.manifest_version,
            "schema_version": self.schema_version,
            "storage_version": self.storage_version,
            "generation_id": self.generation_id,
            "build_timestamp": self.build_timestamp,
            "source_root": self.source_root,
            "source_fingerprints": list(self.source_fingerprints),
            "duckdb_version": self.duckdb_version,
            "build_mode": self.build_mode,
            "build_config": dict(self.build_config),
            "shards": [shard.as_dict() for shard in self.shards],
            "validation_passed": self.validation_passed,
        }

    def dataset_shard(self, dataset: str) -> ShardManifest:
        shards = self.dataset_shards(dataset)
        if shards:
            return shards[0]
        raise LocalDataMissingShardError(
            "DuckDB generation {} 缺少数据集 {!r}；请重建相应分片".format(
                self.generation_id, dataset
            )
        )

    def dataset_shards(self, dataset: str) -> Tuple[ShardManifest, ...]:
        """Return every immutable shard containing ``dataset`` in time order.

        ``dataset_shard`` remains available for backward compatibility with
        single-shard domains.  Time-partitioned callers must use this method so
        adding a new year does not silently hide older data.
        """

        matches = tuple(shard for shard in self.shards if dataset in shard.datasets)
        return tuple(
            sorted(
                matches,
                key=lambda shard: (
                    str(shard.partition.get("start") or ""),
                    str(shard.coverage.get(dataset, {}).get("min") or ""),
                    shard.shard_id,
                ),
            )
        )

    def select_dataset_shards(
        self,
        dataset: str,
        *,
        start: Optional[Any] = None,
        end: Optional[Any] = None,
    ) -> Tuple[ShardManifest, ...]:
        """Select all time shards intersecting a bounded query interval."""

        shards = self.dataset_shards(dataset)
        if not shards:
            self.dataset_shard(dataset)  # raises the stable typed error
        if start is None and end is None:
            return shards
        query_start = _coerce_datetime(start, minimum=True)
        query_end = _coerce_datetime(end, minimum=False)
        selected = []
        for shard in shards:
            partition_start = _coerce_datetime(shard.partition.get("start"), minimum=True)
            partition_end = _coerce_datetime(shard.partition.get("end"), minimum=False)
            coverage = shard.coverage.get(dataset, {})
            shard_start = partition_start or _coerce_datetime(coverage.get("min"), minimum=True)
            shard_end = partition_end or _coerce_datetime(coverage.get("max"), minimum=False)
            if query_end is not None and shard_start is not None and shard_start > query_end:
                continue
            if query_start is not None and shard_end is not None and shard_end < query_start:
                continue
            selected.append(shard)
        if not selected:
            raise LocalDataMissingShardError(
                "DuckDB generation {} 的数据集 {!r} 不覆盖区间 {}..{}；"
                "请构建相应时间分片或选择其他 generation".format(
                    self.generation_id, dataset, start, end
                )
            )
        return tuple(selected)


def _coerce_datetime(value: Optional[Any], *, minimum: bool) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    candidate = getattr(value, "to_pydatetime", None)
    if callable(candidate):
        result = cast(datetime, candidate())
        return result.replace(tzinfo=None)
    text = str(value).strip().replace("Z", "+00:00")
    try:
        result = datetime.fromisoformat(text)
    except ValueError:
        try:
            result = datetime.strptime(text[:10], "%Y-%m-%d")
        except ValueError:
            return None
    result = result.replace(tzinfo=None)
    if len(text) <= 10 and not minimum:
        return result.replace(hour=23, minute=59, second=59, microsecond=999999)
    return result


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        return cast(Mapping[str, Any], json.loads(path.read_text(encoding="utf-8")))
    except FileNotFoundError as exc:
        raise LocalDataConfigurationError("DuckDB manifest 不存在：{}".format(path)) from exc
    except (OSError, ValueError) as exc:
        raise LocalDataConfigurationError(
            "无法读取 DuckDB manifest：{} ({})".format(path, exc)
        ) from exc


def load_generation_manifest(path: Path) -> Tuple[GenerationManifest, Path]:
    """Resolve either a generation manifest or an atomic active pointer."""

    selected = Path(path).expanduser().resolve()
    value = _read_json(selected)
    if "manifest" in value and "generation_id" in value and "shards" not in value:
        target = Path(str(value["manifest"]))
        if not target.is_absolute():
            target = selected.parent / target
        selected = target.resolve()
        value = _read_json(selected)
    manifest = GenerationManifest.from_dict(value)
    if manifest.manifest_version != MANIFEST_FORMAT_VERSION:
        raise LocalDataIncompatibleGenerationError(
            "manifest format {} 与运行时 {} 不兼容；请升级或重建".format(
                manifest.manifest_version, MANIFEST_FORMAT_VERSION
            )
        )
    if manifest.schema_version != CANONICAL_SCHEMA_VERSION:
        raise LocalDataIncompatibleGenerationError(
            "generation schema {} 与运行时 {} 不兼容；请重建".format(
                manifest.schema_version, CANONICAL_SCHEMA_VERSION
            )
        )
    if manifest.storage_version != DUCKDB_STORAGE_VERSION:
        raise LocalDataIncompatibleGenerationError(
            "generation storage {} 与运行时 {} 不兼容；请重建".format(
                manifest.storage_version, DUCKDB_STORAGE_VERSION
            )
        )
    if manifest.build_mode != "materialized":
        raise LocalDataIncompatibleGenerationError(
            "generation build_mode={!r} 不是 fully materialized，不能用于高性能后端".format(
                manifest.build_mode
            )
        )
    if not manifest.validation_passed:
        raise LocalDataIncompatibleGenerationError("generation 未通过构建校验，拒绝打开")
    for shard in manifest.shards:
        shard_path = Path(shard.path)
        if not shard_path.is_absolute():
            shard_path = selected.parent / shard_path
        if not shard_path.is_file():
            raise LocalDataMissingShardError(
                "generation {} 缺少分片 {}：{}；请重建或切回旧 manifest".format(
                    manifest.generation_id, shard.shard_id, shard_path
                )
            )
    return manifest, selected


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Publish a small JSON file atomically on Windows and POSIX."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name(target.name + ".staging-{}".format(os.getpid()))
    try:
        staging.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(str(staging), str(target))
    finally:
        if staging.exists():
            staging.unlink()
