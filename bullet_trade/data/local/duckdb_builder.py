"""Authoritative Parquet-to-DuckDB materialization pipeline."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, cast

from .base import LocalDataConfigurationError, LocalDataSchemaError
from .manifest import GenerationManifest, ShardManifest, atomic_write_json, load_generation_manifest
from .schemas import CANONICAL_SCHEMA_VERSION, DATASET_REGISTRY, DUCKDB_STORAGE_VERSION


def ensure_duckdb() -> Any:
    """Import the pinned optional dependency with precise guidance."""

    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - installation dependent
        raise LocalDataConfigurationError(
            "DuckDB 后端未安装；请执行 `pip install bullet-trade[local]`。"
            "Python 3.8 使用 duckdb 1.3.2，Python 3.9 使用 1.4.4，"
            "Python 3.10+ 使用 1.5.4。"
        ) from exc
    return duckdb


@dataclass(frozen=True)
class DuckDBBuildConfig:
    """Bounded and reproducible build settings."""

    source_root: Path
    output_root: Path
    base_manifest: Optional[Path] = None
    temp_directory: Optional[Path] = None
    threads: int = 8
    memory_limit: str = "12GB"
    max_temp_directory_size: str = "200GB"
    preserve_insertion_order: bool = False
    domains: Tuple[str, ...] = ("meta", "daily", "finance", "actions")
    datasets: Tuple[str, ...] = ()
    minimum_free_bytes: int = 1 << 30
    estimated_output_ratio: float = 2.5
    estimated_temp_ratio: float = 2.5
    source_file_batch_size: int = 256
    minute_years: Tuple[int, ...] = ()
    minute_quarterly_datasets: Tuple[str, ...] = ()
    resume: bool = True
    retained_generations: int = 2

    def normalized(self) -> "DuckDBBuildConfig":
        return DuckDBBuildConfig(
            source_root=Path(self.source_root).expanduser().resolve(),
            output_root=Path(self.output_root).expanduser().resolve(),
            base_manifest=(
                Path(self.base_manifest).expanduser().resolve()
                if self.base_manifest is not None
                else None
            ),
            temp_directory=(
                Path(self.temp_directory).expanduser().resolve()
                if self.temp_directory is not None
                else None
            ),
            threads=max(int(self.threads), 1),
            memory_limit=str(self.memory_limit),
            max_temp_directory_size=str(self.max_temp_directory_size),
            preserve_insertion_order=bool(self.preserve_insertion_order),
            domains=tuple(dict.fromkeys(str(item) for item in self.domains)),
            datasets=tuple(dict.fromkeys(str(item) for item in self.datasets)),
            minimum_free_bytes=max(int(self.minimum_free_bytes), 0),
            estimated_output_ratio=max(float(self.estimated_output_ratio), 0.1),
            estimated_temp_ratio=max(float(self.estimated_temp_ratio), 0.1),
            source_file_batch_size=max(int(self.source_file_batch_size), 1),
            minute_years=tuple(sorted({int(year) for year in self.minute_years})),
            minute_quarterly_datasets=tuple(
                sorted({str(dataset) for dataset in self.minute_quarterly_datasets})
            ),
            resume=bool(self.resume),
            retained_generations=max(int(self.retained_generations), 1),
        )


@dataclass(frozen=True)
class BuildPreflight:
    """Read-only disk and source assessment."""

    source_files: int
    source_bytes: int
    output_free_bytes: int
    temp_free_bytes: int
    required_output_bytes: int
    required_temp_bytes: int
    passed: bool
    reasons: Tuple[str, ...] = field(default_factory=tuple)


def _sql_string(value: Path) -> str:
    return "'{}'".format(str(value).replace("\\", "/").replace("'", "''"))


def _read_parquet_expression(paths: Sequence[Path], *, union_by_name: bool = True) -> str:
    if not paths:
        raise LocalDataSchemaError("不能从空 Parquet 文件集合构建表")
    if len(paths) == 1:
        argument = _sql_string(paths[0])
    else:
        argument = "[{}]".format(",".join(_sql_string(path) for path in paths))
    return "read_parquet({}, union_by_name={})".format(
        argument, "true" if union_by_name else "false"
    )


def _quick_fingerprint(path: Path) -> Dict[str, Any]:
    stat = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        digest.update(stream.read(65536))
        if stat.st_size > 65536:
            stream.seek(max(stat.st_size - 65536, 0))
            digest.update(stream.read(65536))
    return {
        "path": str(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sample_sha256": digest.hexdigest(),
        "fingerprint_method": "size+mtime+first-last-64KiB",
    }


class DuckDBMaterializer:
    """Build immutable physical shards and publish an atomic pointer."""

    _DOMAIN_DATASETS = {
        "meta": ("stock_basic", "fund_basic", "index_basic"),
        "daily": ("stock_daily", "fund_daily", "index_daily"),
        "finance": ("income", "balance", "cash_flow", "indicator"),
        "actions": ("corporate_actions",),
        "minute": tuple(
            name for name, schema in DATASET_REGISTRY.items() if schema.domain == "minute"
        ),
    }

    def __init__(self, config: DuckDBBuildConfig) -> None:
        self.config = config.normalized()
        self.duckdb = ensure_duckdb()
        self._sources: Optional[Dict[str, Tuple[Path, ...]]] = None

    def _find_pattern(self, pattern: str) -> Tuple[Path, ...]:
        direct = tuple(
            sorted(path for path in self.config.source_root.glob(pattern) if path.is_file())
        )
        if direct:
            return direct
        matches: List[Path] = []
        if "/" not in pattern and "\\" not in pattern:
            for child in self.config.source_root.iterdir():
                if child.is_dir():
                    matches.extend(path for path in child.glob(pattern) if path.is_file())
        else:
            leaf = Path(pattern).name
            directory = Path(pattern).parent.name
            for candidate in self.config.source_root.rglob(directory):
                if candidate.is_dir():
                    matches.extend(path for path in candidate.glob(leaf) if path.is_file())
        return tuple(sorted(set(path.resolve() for path in matches)))

    def _datasets_for_domain(self, domain: str) -> Tuple[str, ...]:
        datasets = self._DOMAIN_DATASETS.get(domain, ())
        if not self.config.datasets:
            return datasets
        selected = set(self.config.datasets)
        return tuple(dataset for dataset in datasets if dataset in selected)

    def discover_sources(self) -> Dict[str, Tuple[Path, ...]]:
        if self._sources is not None:
            return self._sources
        if not self.config.source_root.is_dir():
            raise LocalDataConfigurationError(
                "Parquet source root 不存在：{}".format(self.config.source_root)
            )
        discovered: Dict[str, Tuple[Path, ...]] = {}
        unknown = sorted(set(self.config.datasets) - set(DATASET_REGISTRY))
        if unknown:
            raise LocalDataConfigurationError(
                "未知 canonical dataset：{}".format(", ".join(unknown))
            )
        outside_domains = sorted(
            dataset
            for dataset in self.config.datasets
            if DATASET_REGISTRY[dataset].domain not in self.config.domains
        )
        if outside_domains:
            raise LocalDataConfigurationError(
                "datasets 不属于已选择 domains：{}".format(", ".join(outside_domains))
            )
        invalid_quarterly = sorted(
            dataset
            for dataset in self.config.minute_quarterly_datasets
            if dataset not in DATASET_REGISTRY or DATASET_REGISTRY[dataset].domain != "minute"
        )
        if invalid_quarterly:
            raise LocalDataConfigurationError(
                "季度分片只支持 minute dataset：{}".format(", ".join(invalid_quarterly))
            )
        for domain in self.config.domains:
            for dataset in self._datasets_for_domain(domain):
                schema = DATASET_REGISTRY[dataset]
                paths: List[Path] = []
                for pattern in schema.source_patterns:
                    paths.extend(self._find_pattern(pattern))
                unique = tuple(sorted(set(paths)))
                if unique:
                    discovered[dataset] = unique
                elif dataset != "corporate_actions":
                    raise LocalDataSchemaError(
                        "构建数据集 {!r} 缺少源文件：{}".format(
                            dataset, ", ".join(schema.source_patterns)
                        )
                    )
        self._sources = discovered
        return discovered

    def preflight(self) -> BuildPreflight:
        sources = self.discover_sources()
        files = tuple(sorted({path for values in sources.values() for path in values}))
        source_bytes = sum(path.stat().st_size for path in files)
        output_parent = self.config.output_root
        output_parent.mkdir(parents=True, exist_ok=True)
        temp = self.config.temp_directory or (output_parent / ".tmp")
        temp.mkdir(parents=True, exist_ok=True)
        output_free = shutil.disk_usage(output_parent).free
        temp_free = shutil.disk_usage(temp).free
        required_output = int(source_bytes * self.config.estimated_output_ratio) + int(
            self.config.minimum_free_bytes
        )
        required_temp = int(source_bytes * self.config.estimated_temp_ratio) + int(
            self.config.minimum_free_bytes
        )
        reasons: List[str] = []
        if output_free < required_output:
            reasons.append(
                "输出盘可用 {} 字节，小于预计所需 {} 字节".format(output_free, required_output)
            )
        if temp_free < required_temp:
            reasons.append(
                "临时盘可用 {} 字节，小于预计所需 {} 字节".format(temp_free, required_temp)
            )
        return BuildPreflight(
            source_files=len(files),
            source_bytes=source_bytes,
            output_free_bytes=output_free,
            temp_free_bytes=temp_free,
            required_output_bytes=required_output,
            required_temp_bytes=required_temp,
            passed=not reasons,
            reasons=tuple(reasons),
        )

    def _build_identity(
        self,
        fingerprints: Sequence[Mapping[str, Any]],
        *,
        base_generation_id: Optional[str] = None,
    ) -> str:
        payload = {
            "schema_version": CANONICAL_SCHEMA_VERSION,
            "storage_version": DUCKDB_STORAGE_VERSION,
            "sources": list(fingerprints),
            "domains": self.config.domains,
            "datasets": self.config.datasets,
            "preserve_insertion_order": self.config.preserve_insertion_order,
            "minute_years": self.config.minute_years,
            "minute_quarterly_datasets": self.config.minute_quarterly_datasets,
            "base_generation_id": base_generation_id,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:16]

    def _configure_connection(self, connection: Any, temp: Path) -> None:
        connection.execute("SET threads = {}".format(self.config.threads))
        connection.execute(
            "SET memory_limit = '{}'".format(self.config.memory_limit.replace("'", ""))
        )
        connection.execute("SET temp_directory = {}".format(_sql_string(temp)))
        connection.execute(
            "SET max_temp_directory_size = '{}'".format(
                self.config.max_temp_directory_size.replace("'", "")
            )
        )
        connection.execute(
            "SET preserve_insertion_order = {}".format(
                "true" if self.config.preserve_insertion_order else "false"
            )
        )

    @staticmethod
    def _normalized_date_sql(column: str) -> str:
        return (
            "COALESCE(try_cast({0} AS TIMESTAMP), "
            "try_strptime(CAST({0} AS VARCHAR), '%Y%m%d'))".format(column)
        )

    def _create_dataset(self, connection: Any, dataset: str, paths: Sequence[Path]) -> None:
        schema = DATASET_REGISTRY[dataset]
        if dataset == "stock_daily":
            self._create_stock_daily(connection, paths)
            return
        if len(paths) > self.config.source_file_batch_size and dataset not in (
            "income",
            "balance",
            "cash_flow",
            "indicator",
            "corporate_actions",
        ):
            self._create_chunked_dataset(connection, dataset, paths)
            return
        relation = _read_parquet_expression(paths)
        if dataset in ("income", "balance", "cash_flow", "indicator"):
            select_sql = (
                "SELECT * EXCLUDE (ann_date, end_date), "
                "{ann_date} AS ann_date, {end_date} AS end_date "
                "FROM {relation} ORDER BY ts_code, ann_date, end_date"
            ).format(
                ann_date=self._normalized_date_sql("ann_date"),
                end_date=self._normalized_date_sql("end_date"),
                relation=relation,
            )
        elif dataset == "corporate_actions":
            select_sql = (
                "SELECT * FROM {relation} "
                "QUALIFY row_number() OVER (PARTITION BY event_id "
                "ORDER BY source_updated_at DESC NULLS LAST, payload_hash DESC) = 1 "
                "ORDER BY event_date, security"
            ).format(relation=relation)
        else:
            select_sql = "SELECT * FROM {}".format(relation)
            if schema.sort_columns:
                select_sql += " ORDER BY {}".format(", ".join(schema.sort_columns))
        connection.execute('CREATE TABLE "{}" AS {}'.format(dataset, select_sql))

    def _create_stock_daily(self, connection: Any, paths: Sequence[Path]) -> None:
        """Prefer modern rows and fill only missing canonical keys from legacy data."""

        modern = tuple(path for path in paths if path.name == "stock_daily.parquet")
        legacy = tuple(path for path in paths if path.name.startswith("daily_adj_"))
        if not modern:
            raise LocalDataSchemaError("stock_daily 构建缺少现代 stock_daily.parquet")
        modern_sql = (
            "SELECT * EXCLUDE (trade_date), {date_sql} AS trade_date FROM {relation}"
        ).format(
            date_sql=self._normalized_date_sql("trade_date"),
            relation=_read_parquet_expression(modern),
        )
        if not legacy:
            connection.execute(
                'CREATE TABLE "stock_daily" AS SELECT * FROM ({}) '
                "ORDER BY trade_date, ts_code".format(modern_sql)
            )
            return
        legacy_sql = (
            "SELECT * EXCLUDE (trade_date), {date_sql} AS trade_date FROM {relation}"
        ).format(
            date_sql=self._normalized_date_sql("trade_date"),
            relation=_read_parquet_expression(legacy),
        )
        connection.execute('CREATE TEMP TABLE "__stock_daily_modern" AS {}'.format(modern_sql))
        connection.execute(
            'CREATE TEMP TABLE "__stock_daily_legacy_missing" AS '
            "SELECT legacy.* FROM ({legacy}) AS legacy ANTI JOIN "
            '"__stock_daily_modern" AS modern USING (ts_code, trade_date)'.format(legacy=legacy_sql)
        )
        connection.execute(
            'CREATE TABLE "stock_daily" AS SELECT * FROM ('
            'SELECT * FROM "__stock_daily_modern" UNION ALL BY NAME '
            'SELECT * FROM "__stock_daily_legacy_missing") '
            "ORDER BY trade_date, ts_code"
        )
        connection.execute('DROP TABLE "__stock_daily_legacy_missing"')
        connection.execute('DROP TABLE "__stock_daily_modern"')

    def _create_chunked_dataset(self, connection: Any, dataset: str, paths: Sequence[Path]) -> None:
        """Bound metadata memory while consolidating per-security Parquet files."""

        schema = DATASET_REGISTRY[dataset]
        stage = "__{}_source".format(dataset)
        size = self.config.source_file_batch_size
        for offset in range(0, len(paths), size):
            relation = _read_parquet_expression(paths[offset : offset + size])
            source_select = self._chunk_source_select(connection, dataset, relation)
            if offset == 0:
                connection.execute('CREATE TABLE "{}" AS {}'.format(stage, source_select))
            else:
                connection.execute('INSERT INTO "{}" BY NAME {}'.format(stage, source_select))
        select_sql = 'SELECT * FROM "{}"'.format(stage)
        if schema.sort_columns:
            select_sql += " ORDER BY {}".format(", ".join(schema.sort_columns))
        connection.execute('CREATE TABLE "{}" AS {}'.format(dataset, select_sql))
        connection.execute('DROP TABLE "{}"'.format(stage))

    def _chunk_source_select(self, connection: Any, dataset: str, relation: str) -> str:
        """Normalize known all-null/list schema drift between index files."""

        schema = DATASET_REGISTRY[dataset]
        if schema.domain == "minute":
            described = connection.execute("DESCRIBE SELECT * FROM {}".format(relation)).fetchall()
            columns = {str(row[0]) for row in described}
            if "trade_time" not in columns:
                raise LocalDataSchemaError("分钟数据集 {} 的源文件缺少 trade_time".format(dataset))
            excluded = [column for column in ("trade_date", "trade_time") if column in columns]
            expressions = [
                "* EXCLUDE ({})".format(", ".join(excluded)) if excluded else "*",
                "{} AS trade_time".format(self._normalized_date_sql("trade_time")),
            ]
            if "trade_date" in columns:
                expressions.append(
                    "CAST({} AS DATE) AS trade_date".format(self._normalized_date_sql("trade_date"))
                )
            else:
                expressions.append(
                    "CAST({} AS DATE) AS trade_date".format(self._normalized_date_sql("trade_time"))
                )
            where = ""
            if self.config.minute_years:
                years = ",".join(str(year) for year in self.config.minute_years)
                where = " WHERE year({}) IN ({})".format(
                    self._normalized_date_sql("trade_time"), years
                )
            return "SELECT {} FROM {}{}".format(", ".join(expressions), relation, where)
        if dataset != "index_daily":
            return "SELECT * FROM {}".format(relation)
        described = connection.execute("DESCRIBE SELECT * FROM {}".format(relation)).fetchall()
        column_types = {str(row[0]): str(row[1]).upper() for row in described}
        nested_types = {"con_codes": "VARCHAR[]", "weights": "DOUBLE[]"}
        nested_excluded = [column for column in nested_types if column in column_types]
        expressions = [
            "* EXCLUDE ({})".format(", ".join(nested_excluded)) if nested_excluded else "*"
        ]
        for column, target_type in nested_types.items():
            source_type = column_types.get(column, "")
            if source_type.endswith("[]"):
                expressions.append('CAST("{}" AS {}) AS "{}"'.format(column, target_type, column))
            else:
                expressions.append('CAST(NULL AS {}) AS "{}"'.format(target_type, column))
        return "SELECT {} FROM {}".format(", ".join(expressions), relation)

    def _validate_dataset(self, connection: Any, dataset: str) -> Dict[str, Any]:
        schema = DATASET_REGISTRY[dataset]
        table = '"{}"'.format(dataset)
        table_info = connection.execute("PRAGMA table_info({})".format(table)).fetchall()
        column_types = {str(row[1]): str(row[2]) for row in table_info}
        columns = set(column_types)
        missing = sorted(set(schema.required_columns) - columns)
        if missing:
            raise LocalDataSchemaError(
                "DuckDB 表 {} 缺少字段：{}".format(dataset, ", ".join(missing))
            )
        row_count = int(connection.execute("SELECT count(*) FROM {}".format(table)).fetchone()[0])
        key_sql = ", ".join(schema.key_columns)
        duplicate_count = int(
            connection.execute(
                "SELECT COALESCE(sum(n - 1), 0) FROM "
                "(SELECT count(*) AS n FROM {} GROUP BY {} HAVING count(*) > 1)".format(
                    table, key_sql
                )
            ).fetchone()[0]
        )
        if duplicate_count:
            raise LocalDataSchemaError(
                "DuckDB 表 {} 存在 {} 条重复规范键".format(dataset, duplicate_count)
            )
        required = tuple(column for column in schema.required_columns if column in columns)
        null_values = connection.execute(
            "SELECT {} FROM {}".format(
                ", ".join(
                    'count(*) FILTER (WHERE "{}" IS NULL)'.format(column) for column in required
                ),
                table,
            )
        ).fetchone()
        null_counts = {column: int(null_values[index]) for index, column in enumerate(required)}
        key_nulls = sum(null_counts.get(column, 0) for column in schema.key_columns)
        invalid_types = [
            column for column in required if column_types.get(column, "").upper() in ("", "NULL")
        ]
        if key_nulls:
            raise LocalDataSchemaError(
                "DuckDB 表 {} 的规范键存在 {} 个空值".format(dataset, key_nulls)
            )
        if invalid_types:
            raise LocalDataSchemaError(
                "DuckDB 表 {} 的必需字段类型无效：{}".format(dataset, ", ".join(invalid_types))
            )
        coverage: Dict[str, Optional[str]] = {"min": None, "max": None}
        if schema.time_column:
            values = connection.execute(
                "SELECT min({0}), max({0}) FROM {1}".format(schema.time_column, table)
            ).fetchone()
            coverage = {
                "min": None if values[0] is None else str(values[0]),
                "max": None if values[1] is None else str(values[1]),
            }
        sample_columns = tuple(dict.fromkeys(schema.key_columns + schema.required_columns))
        sample_projection = ", ".join('"{}"'.format(column) for column in sample_columns)
        order_columns = schema.sort_columns or schema.key_columns
        order = ", ".join(order_columns)
        reverse_order = ", ".join("{} DESC".format(column) for column in order_columns)
        first_rows = connection.execute(
            "SELECT {} FROM {} ORDER BY {} LIMIT 4".format(sample_projection, table, order)
        ).fetchall()
        last_rows = connection.execute(
            "SELECT {} FROM {} ORDER BY {} LIMIT 4".format(sample_projection, table, reverse_order)
        ).fetchall()
        representative_hash = hashlib.sha256(
            json.dumps(
                [list(row) for row in first_rows + last_rows],
                sort_keys=True,
                default=str,
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()
        return {
            "row_count": row_count,
            "duplicate_count": duplicate_count,
            "coverage": coverage,
            "null_counts": null_counts,
            "column_types": column_types,
            "representative_hash": representative_hash,
            "valid": True,
        }

    def _build_shard(
        self,
        staging: Path,
        generation_id: str,
        domain: str,
        sources: Mapping[str, Tuple[Path, ...]],
    ) -> Optional[ShardManifest]:
        datasets = tuple(
            dataset for dataset in self._datasets_for_domain(domain) if dataset in sources
        )
        if not datasets:
            return None
        filename = "{}_{}.duckdb".format(domain, generation_id)
        database = staging / filename
        partial = database.with_name(database.name + ".partial")
        if partial.exists():
            partial.unlink()
        temp = self.config.temp_directory or (staging / "tmp")
        temp.mkdir(parents=True, exist_ok=True)
        connection = self.duckdb.connect(str(partial))
        try:
            self._configure_connection(connection, temp)
            results: Dict[str, Dict[str, Any]] = {}
            for dataset in datasets:
                self._create_dataset(connection, dataset, sources[dataset])
                results[dataset] = self._validate_dataset(connection, dataset)
            connection.execute("CHECKPOINT")
        finally:
            connection.close()
        os.replace(str(partial), str(database))
        return ShardManifest(
            shard_id=domain,
            domain=domain,
            path=filename,
            datasets=datasets,
            sort_keys={dataset: DATASET_REGISTRY[dataset].sort_columns for dataset in datasets},
            row_counts={dataset: results[dataset]["row_count"] for dataset in datasets},
            duplicate_counts={dataset: results[dataset]["duplicate_count"] for dataset in datasets},
            coverage={dataset: results[dataset]["coverage"] for dataset in datasets},
            null_counts={dataset: results[dataset]["null_counts"] for dataset in datasets},
            column_types={dataset: results[dataset]["column_types"] for dataset in datasets},
            representative_hashes={
                dataset: results[dataset]["representative_hash"] for dataset in datasets
            },
            validations={dataset: results[dataset]["valid"] for dataset in datasets},
        )

    def _minute_stage_directory(self, generation_id: str) -> Path:
        root = self.config.temp_directory or (self.config.output_root / ".tmp")
        return root / "bullet-trade-minute-build" / generation_id

    def _prepare_minute_stage(
        self,
        generation_id: str,
        dataset: str,
        paths: Sequence[Path],
    ) -> Path:
        """Scan each source file once into a resumable, unsorted routing table."""

        stage_root = self._minute_stage_directory(generation_id)
        stage_root.mkdir(parents=True, exist_ok=True)
        database = stage_root / "{}.duckdb".format(dataset)
        marker = stage_root / "{}.ready.json".format(dataset)
        if database.is_file() and marker.is_file():
            try:
                value = json.loads(marker.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                value = {}
            if (
                value.get("generation_id") == generation_id
                and value.get("dataset") == dataset
                and int(value.get("source_files", -1)) == len(paths)
            ):
                return database
        for candidate in (marker, database, database.with_name(database.name + ".wal")):
            if candidate.exists():
                candidate.unlink()
        connection = self.duckdb.connect(str(database))
        try:
            self._configure_connection(connection, stage_root)
            size = self.config.source_file_batch_size
            for offset in range(0, len(paths), size):
                relation = _read_parquet_expression(paths[offset : offset + size])
                source_select = self._chunk_source_select(connection, dataset, relation)
                if offset == 0:
                    connection.execute('CREATE TABLE "{}" AS {}'.format(dataset, source_select))
                else:
                    connection.execute('INSERT INTO "{}" BY NAME {}'.format(dataset, source_select))
            connection.execute("CHECKPOINT")
            row_count, minimum, maximum = connection.execute(
                'SELECT count(*), min(trade_time), max(trade_time) FROM "{}"'.format(dataset)
            ).fetchone()
        finally:
            connection.close()
        atomic_write_json(
            marker,
            {
                "generation_id": generation_id,
                "dataset": dataset,
                "source_files": len(paths),
                "row_count": int(row_count),
                "coverage": {"min": str(minimum), "max": str(maximum)},
            },
        )
        return database

    def _minute_years(self, stage_database: Path, dataset: str) -> Tuple[int, ...]:
        connection = self.duckdb.connect(str(stage_database), read_only=True)
        try:
            rows = connection.execute(
                'SELECT DISTINCT year(trade_time) FROM "{}" '
                "WHERE trade_time IS NOT NULL ORDER BY 1".format(dataset)
            ).fetchall()
        finally:
            connection.close()
        years = tuple(int(row[0]) for row in rows if row[0] is not None)
        if self.config.minute_years:
            allowed = set(self.config.minute_years)
            years = tuple(year for year in years if year in allowed)
        return years

    def _minute_partitions(
        self, stage_database: Path, dataset: str
    ) -> Tuple[Tuple[str, str, str, str, str], ...]:
        years = self._minute_years(stage_database, dataset)
        if dataset not in self.config.minute_quarterly_datasets:
            return tuple(
                (
                    str(year),
                    "year",
                    str(year),
                    "{:04d}-01-01 00:00:00".format(year),
                    "{:04d}-01-01 00:00:00".format(year + 1),
                )
                for year in years
            )
        connection = self.duckdb.connect(str(stage_database), read_only=True)
        try:
            rows = connection.execute(
                'SELECT DISTINCT year(trade_time), quarter(trade_time) FROM "{}" '
                "WHERE trade_time IS NOT NULL ORDER BY 1, 2".format(dataset)
            ).fetchall()
        finally:
            connection.close()
        allowed = set(years)
        partitions = []
        for raw_year, raw_quarter in rows:
            year, quarter = int(raw_year), int(raw_quarter)
            if year not in allowed:
                continue
            start_month = (quarter - 1) * 3 + 1
            end_year = year + 1 if quarter == 4 else year
            end_month = 1 if quarter == 4 else start_month + 3
            partitions.append(
                (
                    "{}q{}".format(year, quarter),
                    "quarter",
                    "{}Q{}".format(year, quarter),
                    "{:04d}-{:02d}-01 00:00:00".format(year, start_month),
                    "{:04d}-{:02d}-01 00:00:00".format(end_year, end_month),
                )
            )
        return tuple(partitions)

    def _build_minute_year_shard(
        self,
        staging: Path,
        generation_id: str,
        dataset: str,
        year: int,
        stage_database: Path,
    ) -> ShardManifest:
        return self._build_minute_partition_shard(
            staging,
            generation_id,
            dataset,
            label=str(year),
            kind="year",
            value=str(year),
            start="{:04d}-01-01 00:00:00".format(year),
            end="{:04d}-01-01 00:00:00".format(year + 1),
            stage_database=stage_database,
        )

    def _build_minute_partition_shard(
        self,
        staging: Path,
        generation_id: str,
        dataset: str,
        *,
        label: str,
        kind: str,
        value: str,
        start: str,
        end: str,
        stage_database: Path,
    ) -> ShardManifest:
        schema = DATASET_REGISTRY[dataset]
        shard_id = "minute:{}:{}:{}".format(schema.asset_type, dataset.rsplit("_", 1)[-1], value)
        filename = "{}_{}_{}.duckdb".format(dataset, label, generation_id)
        database = staging / filename
        partial = database.with_name(database.name + ".partial")
        for candidate in (partial, partial.with_name(partial.name + ".wal")):
            if candidate.exists():
                candidate.unlink()
        temp = self.config.temp_directory or (staging / "tmp")
        temp.mkdir(parents=True, exist_ok=True)
        connection = self.duckdb.connect(str(partial))
        try:
            self._configure_connection(connection, temp)
            connection.execute(
                "ATTACH {} AS minute_source (READ_ONLY)".format(_sql_string(stage_database))
            )
            connection.execute(
                'CREATE TABLE "{dataset}" AS SELECT * FROM minute_source."{dataset}" '
                "WHERE trade_time >= TIMESTAMP '{start}' AND trade_time < TIMESTAMP '{end}' "
                "ORDER BY {order}".format(
                    dataset=dataset,
                    start=start,
                    end=end,
                    order=", ".join(schema.sort_columns),
                )
            )
            result = self._validate_dataset(connection, dataset)
            connection.execute("CHECKPOINT")
        finally:
            connection.close()
        os.replace(str(partial), str(database))
        return ShardManifest(
            shard_id=shard_id,
            domain="minute",
            path=filename,
            datasets=(dataset,),
            sort_keys={dataset: schema.sort_columns},
            row_counts={dataset: result["row_count"]},
            duplicate_counts={dataset: result["duplicate_count"]},
            coverage={dataset: result["coverage"]},
            null_counts={dataset: result["null_counts"]},
            column_types={dataset: result["column_types"]},
            representative_hashes={dataset: result["representative_hash"]},
            validations={dataset: result["valid"]},
            partition={
                "kind": kind,
                "value": value,
                "start": start,
                "end": str(datetime.fromisoformat(end) - timedelta(microseconds=1)),
            },
        )

    def _cleanup_minute_stage(self, generation_id: str, dataset: str) -> None:
        stage_root = self._minute_stage_directory(generation_id)
        database = stage_root / "{}.duckdb".format(dataset)
        for candidate in (
            stage_root / "{}.ready.json".format(dataset),
            database,
            database.with_name(database.name + ".wal"),
        ):
            if candidate.exists():
                candidate.unlink()

    @staticmethod
    def _load_build_state(staging: Path, generation_id: str) -> Dict[str, Any]:
        path = staging / "build-state.json"
        if not path.is_file():
            return {"generation_id": generation_id, "completed_shards": {}}
        try:
            state = cast(Dict[str, Any], json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError) as exc:
            raise LocalDataConfigurationError("无法读取断点状态 {}：{}".format(path, exc)) from exc
        if state.get("generation_id") != generation_id:
            raise LocalDataConfigurationError("staging 的 generation identity 与当前源数据不一致")
        state.setdefault("completed_shards", {})
        return state

    @staticmethod
    def _save_build_state(staging: Path, state: Dict[str, Any]) -> None:
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        atomic_write_json(staging / "build-state.json", state)

    def _record_completed_shard(
        self, staging: Path, state: Dict[str, Any], shard: ShardManifest
    ) -> None:
        state["completed_shards"][shard.shard_id] = shard.as_dict()
        self._save_build_state(staging, state)

    @staticmethod
    def _completed_shard(
        staging: Path, state: Mapping[str, Any], shard_id: str
    ) -> Optional[ShardManifest]:
        value = state.get("completed_shards", {}).get(shard_id)
        if not value:
            return None
        shard = ShardManifest.from_dict(value)
        if not (staging / shard.path).is_file() or not all(shard.validations.values()):
            return None
        return shard

    def _build_minute_domain(
        self,
        staging: Path,
        generation_id: str,
        sources: Mapping[str, Tuple[Path, ...]],
        state: Dict[str, Any],
    ) -> List[ShardManifest]:
        shards: List[ShardManifest] = []
        state.setdefault("completed_datasets", {})
        for dataset in self._datasets_for_domain("minute"):
            paths = sources.get(dataset)
            if not paths:
                continue
            completed_ids = tuple(state["completed_datasets"].get(dataset, ()))
            completed = [
                self._completed_shard(staging, state, shard_id) for shard_id in completed_ids
            ]
            if completed_ids and all(shard is not None for shard in completed):
                shards.extend(shard for shard in completed if shard is not None)
                continue
            stage_database = self._prepare_minute_stage(generation_id, dataset, paths)
            dataset_shards: List[ShardManifest] = []
            for label, kind, value, start, end in self._minute_partitions(stage_database, dataset):
                schema = DATASET_REGISTRY[dataset]
                shard_id = "minute:{}:{}:{}".format(
                    schema.asset_type, dataset.rsplit("_", 1)[-1], value
                )
                shard = self._completed_shard(staging, state, shard_id)
                if shard is None:
                    if kind == "year":
                        shard = self._build_minute_year_shard(
                            staging, generation_id, dataset, int(value), stage_database
                        )
                    else:
                        shard = self._build_minute_partition_shard(
                            staging,
                            generation_id,
                            dataset,
                            label=label,
                            kind=kind,
                            value=value,
                            start=start,
                            end=end,
                            stage_database=stage_database,
                        )
                    self._record_completed_shard(staging, state, shard)
                shards.append(shard)
                dataset_shards.append(shard)
            state["completed_datasets"][dataset] = [shard.shard_id for shard in dataset_shards]
            self._save_build_state(staging, state)
            self._cleanup_minute_stage(generation_id, dataset)
        return shards

    @staticmethod
    def _merge_fingerprints(
        inherited: Sequence[Mapping[str, Any]],
        current: Sequence[Mapping[str, Any]],
    ) -> Tuple[Mapping[str, Any], ...]:
        merged: Dict[str, Mapping[str, Any]] = {}
        for fingerprint in tuple(inherited) + tuple(current):
            key = str(fingerprint.get("path") or json.dumps(fingerprint, sort_keys=True))
            merged[key] = fingerprint
        return tuple(merged[key] for key in sorted(merged))

    def _inherit_shards(
        self,
        staging: Path,
        base_manifest: GenerationManifest,
        base_manifest_path: Path,
    ) -> List[ShardManifest]:
        """Hard-link or copy unaffected immutable shards into a new generation."""

        selected_domains = set(self.config.domains)
        selected_minute = (
            set(self._datasets_for_domain("minute")) if "minute" in selected_domains else set()
        )
        inherited: List[ShardManifest] = []
        for shard in base_manifest.shards:
            if shard.domain != "minute" and shard.domain in selected_domains:
                continue
            if shard.domain == "minute" and selected_minute.intersection(shard.datasets):
                continue
            source = Path(shard.path)
            if not source.is_absolute():
                source = base_manifest_path.parent / source
            source = source.resolve()
            target = staging / source.name
            if not target.exists():
                try:
                    os.link(str(source), str(target))
                except OSError:
                    shutil.copy2(source, target)
            value = shard.as_dict()
            value["path"] = target.name
            inherited.append(ShardManifest.from_dict(value))
        return inherited

    def build(self, *, publish: bool = True) -> GenerationManifest:
        """Build, validate, rename, and optionally publish a generation."""

        preflight = self.preflight()
        if not preflight.passed:
            raise LocalDataConfigurationError(
                "DuckDB 构建预检失败：{}".format("；".join(preflight.reasons))
            )
        sources = self.discover_sources()
        files = tuple(sorted({path for values in sources.values() for path in values}))
        current_fingerprints = tuple(_quick_fingerprint(path) for path in files)
        base_manifest: Optional[GenerationManifest] = None
        base_manifest_path: Optional[Path] = None
        if self.config.base_manifest is not None:
            base_manifest, base_manifest_path = load_generation_manifest(
                self.config.base_manifest
            )
            runtime_major = str(self.duckdb.__version__).split(".", 1)[0]
            base_major = str(base_manifest.duckdb_version).split(".", 1)[0]
            if runtime_major != base_major:
                raise LocalDataConfigurationError(
                    "base generation DuckDB {} 与构建运行时 {} 不兼容".format(
                        base_manifest.duckdb_version, self.duckdb.__version__
                    )
                )
        fingerprints = self._merge_fingerprints(
            () if base_manifest is None else base_manifest.source_fingerprints,
            current_fingerprints,
        )
        generation_id = self._build_identity(
            fingerprints,
            base_generation_id=(
                None if base_manifest is None else base_manifest.generation_id
            ),
        )
        generations = self.config.output_root / "generations"
        final = generations / generation_id
        manifest_path = final / "manifest.json"
        if manifest_path.is_file():
            manifest, _ = load_generation_manifest(manifest_path)
            if publish:
                self._publish_pointer(manifest_path, manifest)
            return manifest
        generations.mkdir(parents=True, exist_ok=True)
        staging = generations / (".staging-{}".format(generation_id))
        if staging.exists() and not self.config.resume:
            raise LocalDataConfigurationError(
                "构建 staging 已存在：{}；启用 resume 或确认无活动构建后显式清理".format(staging)
            )
        staging.mkdir(parents=True, exist_ok=True)
        lock = self._acquire_build_lock(generations, generation_id)
        try:
            state = self._load_build_state(staging, generation_id)
            state.setdefault("started_at", datetime.now(timezone.utc).isoformat())
            self._save_build_state(staging, state)
            shards: List[ShardManifest] = (
                []
                if base_manifest is None or base_manifest_path is None
                else self._inherit_shards(staging, base_manifest, base_manifest_path)
            )
            for domain in self.config.domains:
                if domain not in self._DOMAIN_DATASETS:
                    raise LocalDataConfigurationError("未知物化 domain：{}".format(domain))
                if domain == "minute":
                    shards.extend(self._build_minute_domain(staging, generation_id, sources, state))
                    continue
                shard = self._completed_shard(staging, state, domain)
                if shard is None:
                    shard = self._build_shard(staging, generation_id, domain, sources)
                    if shard is not None:
                        self._record_completed_shard(staging, state, shard)
                if shard is not None:
                    shards.append(shard)
            manifest = GenerationManifest(
                generation_id=generation_id,
                build_timestamp=datetime.now(timezone.utc).isoformat(),
                source_root=str(self.config.source_root),
                source_fingerprints=fingerprints,
                duckdb_version=str(self.duckdb.__version__),
                build_mode="materialized",
                build_config={
                    "threads": self.config.threads,
                    "memory_limit": self.config.memory_limit,
                    "max_temp_directory_size": self.config.max_temp_directory_size,
                    "preserve_insertion_order": self.config.preserve_insertion_order,
                    "source_file_batch_size": self.config.source_file_batch_size,
                    "domains": list(self.config.domains),
                    "datasets": list(self.config.datasets),
                    "minute_years": list(self.config.minute_years),
                    "minute_quarterly_datasets": list(self.config.minute_quarterly_datasets),
                    "resume": self.config.resume,
                    "retained_generations": self.config.retained_generations,
                    "base_generation_id": (
                        None if base_manifest is None else base_manifest.generation_id
                    ),
                },
                shards=tuple(shards),
                validation_passed=all(all(shard.validations.values()) for shard in shards),
            )
            atomic_write_json(staging / "manifest.json", manifest.as_dict())
            staging.rename(final)
        except Exception:
            # Keep unreferenced staging for audit/recovery; active pointer is untouched.
            raise
        finally:
            self._release_build_lock(lock)
        final_manifest = final / "manifest.json"
        if publish:
            self._publish_pointer(final_manifest, manifest)
        return manifest

    @staticmethod
    def _pid_is_running(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False
        return True

    def _acquire_build_lock(self, generations: Path, generation_id: str) -> Path:
        lock = generations / (".build-{}.lock".format(generation_id))
        if lock.exists():
            try:
                value = json.loads(lock.read_text(encoding="utf-8"))
                owner = int(value.get("pid", -1))
            except (OSError, ValueError, TypeError):
                owner = -1
            if self._pid_is_running(owner):
                raise LocalDataConfigurationError(
                    "generation {} 正由 PID {} 构建；拒绝并发写入".format(generation_id, owner)
                )
            lock.unlink()
        try:
            descriptor = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise LocalDataConfigurationError(
                "generation {} 已有活动构建锁：{}".format(generation_id, lock)
            ) from exc
        try:
            payload = json.dumps(
                {
                    "pid": os.getpid(),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "generation_id": generation_id,
                }
            ).encode("utf-8")
            os.write(descriptor, payload)
        finally:
            os.close(descriptor)
        return lock

    @staticmethod
    def _release_build_lock(lock: Path) -> None:
        if lock.exists():
            lock.unlink()

    def _publish_pointer(self, manifest_path: Path, manifest: GenerationManifest) -> None:
        relative = manifest_path.relative_to(self.config.output_root)
        atomic_write_json(
            self.config.output_root / "current-manifest.json",
            {"generation_id": manifest.generation_id, "manifest": relative.as_posix()},
        )

    def stale_staging_directories(self) -> Tuple[Path, ...]:
        generations = self.config.output_root / "generations"
        if not generations.is_dir():
            return ()
        return tuple(sorted(path for path in generations.glob(".staging-*") if path.is_dir()))

    def retained_generation_candidates(self, keep: Optional[int] = None) -> Tuple[Path, ...]:
        """List old immutable generations eligible for an explicit cleanup.

        The active generation and newest retained generations are never returned.
        Listing is read-only so Windows users can close old backtests before removal.
        """

        generations = self.config.output_root / "generations"
        if not generations.is_dir():
            return ()
        retained = max(self.config.retained_generations if keep is None else int(keep), 1)
        active_id: Optional[str] = None
        pointer = self.config.output_root / "current-manifest.json"
        if pointer.is_file():
            try:
                active_id = str(json.loads(pointer.read_text(encoding="utf-8"))["generation_id"])
            except (OSError, ValueError, KeyError):
                active_id = None
        candidates = [
            path
            for path in generations.iterdir()
            if path.is_dir() and not path.name.startswith(".") and path.name != active_id
        ]
        candidates.sort(key=lambda path: path.stat().st_mtime_ns, reverse=True)
        return tuple(candidates[retained:])
