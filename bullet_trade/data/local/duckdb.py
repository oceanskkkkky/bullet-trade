"""Read-only native DuckDB backend for immutable materialized generations."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from .base import (
    AssetType,
    BarRequest,
    BatchBarRequest,
    CorporateActionRequest,
    CurrentBarRequest,
    LocalDataBackend,
    LocalDataConfigurationError,
    LocalDataQueryError,
    LocalDataSchemaError,
    ReferenceFactorRequest,
    SecurityInfoRequest,
)
from .duckdb_builder import ensure_duckdb
from .manifest import GenerationManifest, ShardManifest, load_generation_manifest


class DuckDBDataBackend(LocalDataBackend):
    """Native-batch backend with no strategy-time Parquet fallback."""

    name = "duckdb"

    def __init__(
        self,
        manifest_path: Path,
        *,
        threads: int = 8,
        memory_limit: str = "12GB",
        temp_directory: Optional[Path] = None,
        max_temp_directory_size: str = "100GB",
    ) -> None:
        self.manifest_path = Path(manifest_path).expanduser().resolve()
        self.threads = max(int(threads), 1)
        self.memory_limit = str(memory_limit)
        self.temp_directory = (
            Path(temp_directory).expanduser().resolve() if temp_directory is not None else None
        )
        self.max_temp_directory_size = str(max_temp_directory_size)
        self.duckdb = ensure_duckdb()
        self._manifest: Optional[GenerationManifest] = None
        self._resolved_manifest_path: Optional[Path] = None
        self._connections: Dict[str, Any] = {}
        self._columns: Dict[str, Tuple[str, ...]] = {}
        self._catalogs: Optional[Dict[AssetType, pd.DataFrame]] = None

    @property
    def manifest(self) -> GenerationManifest:
        if self._manifest is None:
            self.validate()
        assert self._manifest is not None
        return self._manifest

    def validate(self) -> None:
        manifest, resolved = load_generation_manifest(self.manifest_path)
        runtime_major = str(self.duckdb.__version__).split(".", 1)[0]
        build_major = str(manifest.duckdb_version).split(".", 1)[0]
        if runtime_major != build_major:
            raise LocalDataConfigurationError(
                "DuckDB runtime {} 无法安全打开由 {} 构建的 generation；"
                "请安装兼容版本或重建".format(self.duckdb.__version__, manifest.duckdb_version)
            )
        self._manifest = manifest
        self._resolved_manifest_path = resolved

    def _shard_path(self, shard: ShardManifest) -> Path:
        assert self._resolved_manifest_path is not None
        path = Path(shard.path)
        if not path.is_absolute():
            path = self._resolved_manifest_path.parent / path
        return path.resolve()

    def _connection_for_shard(self, shard: ShardManifest) -> Any:
        existing = self._connections.get(shard.shard_id)
        if existing is not None:
            return existing
        connection = self.duckdb.connect(str(self._shard_path(shard)), read_only=True)
        connection.execute("SET threads = {}".format(self.threads))
        connection.execute("SET memory_limit = '{}'".format(self.memory_limit.replace("'", "")))
        connection.execute(
            "SET max_temp_directory_size = '{}'".format(
                self.max_temp_directory_size.replace("'", "")
            )
        )
        if self.temp_directory is not None:
            self.temp_directory.mkdir(parents=True, exist_ok=True)
            escaped = str(self.temp_directory).replace("\\", "/").replace("'", "''")
            connection.execute("SET temp_directory = '{}'".format(escaped))
        self._connections[shard.shard_id] = connection
        return connection

    def _connection(self, dataset: str) -> Any:
        """Return the first shard connection for single-shard compatibility paths."""

        return self._connection_for_shard(self.manifest.dataset_shard(dataset))

    def close(self) -> None:
        for connection in self._connections.values():
            try:
                connection.close()
            except Exception:
                pass
        self._connections.clear()

    def __del__(self) -> None:  # pragma: no cover - interpreter cleanup timing
        self.close()

    def _table_columns(self, dataset: str) -> Tuple[str, ...]:
        cached = self._columns.get(dataset)
        if cached is not None:
            return cached
        connection = self._connection(dataset)
        rows = connection.execute('PRAGMA table_info("{}")'.format(dataset)).fetchall()
        columns = tuple(str(row[1]) for row in rows)
        if not columns:
            raise LocalDataSchemaError("DuckDB generation 缺少表：{}".format(dataset))
        self._columns[dataset] = columns
        return columns

    def _execute_frame(
        self,
        dataset: str,
        sql: str,
        parameters: Optional[Sequence[Any]] = None,
        *,
        signature: str,
    ) -> pd.DataFrame:
        arrow = self._execute_arrow(dataset, sql, parameters, signature=signature)
        return arrow.to_pandas()

    def _execute_arrow(
        self,
        dataset: str,
        sql: str,
        parameters: Optional[Sequence[Any]] = None,
        *,
        signature: str,
    ) -> Any:
        """Materialize an Arrow table whose lifetime is independent of the cursor."""

        shard = self.manifest.dataset_shard(dataset)
        return self._execute_arrow_on_shard(shard, dataset, sql, parameters, signature=signature)

    def _execute_arrow_on_shard(
        self,
        shard: ShardManifest,
        dataset: str,
        sql: str,
        parameters: Optional[Sequence[Any]] = None,
        *,
        signature: str,
    ) -> Any:
        started = perf_counter()
        try:
            relation = self._connection_for_shard(shard).execute(sql, list(parameters or ()))
            arrow = relation.fetch_arrow_table()
        except Exception as exc:
            raise LocalDataQueryError(
                "DuckDB 查询失败 dataset={} ({})".format(dataset, exc)
            ) from exc
        try:
            result_bytes = int(getattr(arrow, "nbytes", 0))
        except (AttributeError, TypeError, ValueError):
            result_bytes = 0
        self._record_query(
            "duckdb_sql",
            rows=int(getattr(arrow, "num_rows", 0)),
            elapsed_seconds=perf_counter() - started,
            result_bytes=result_bytes,
            signature="{}|shard={}".format(signature, shard.shard_id),
        )
        return arrow

    @staticmethod
    def _daily_dataset(asset_type: AssetType) -> str:
        return {
            AssetType.STOCK: "stock_daily",
            AssetType.FUND: "fund_daily",
            AssetType.INDEX: "index_daily",
        }[asset_type]

    def _bar_dataset(self, asset_type: AssetType, frequency: str) -> str:
        if frequency == "1d":
            return self._daily_dataset(asset_type)
        dataset = "{}_{}".format(asset_type.value, frequency)
        # A missing materialized minute shard must fail; never scan source Parquet.
        self.manifest.dataset_shards(dataset) or self.manifest.dataset_shard(dataset)
        return dataset

    def asset_type(self, security: str) -> AssetType:
        catalogs = self._load_catalogs()
        for asset in (AssetType.FUND, AssetType.INDEX, AssetType.STOCK):
            if security in catalogs[asset].index:
                return asset
        raise LocalDataQueryError("DuckDB 基础信息中不存在证券代码：{}".format(security))

    def read_bars(self, request: BarRequest) -> pd.DataFrame:
        result = self.read_bars_batch(
            BatchBarRequest(
                securities=(request.security,),
                asset_type=request.asset_type,
                frequency=request.frequency,
                start=request.start,
                end=request.end,
                columns=request.columns,
            )
        )
        return result.loc[result.get("code") == request.security].reset_index(drop=True)

    def read_bars_batch(self, request: BatchBarRequest) -> pd.DataFrame:
        return self._read_bars_batch_result(request, as_arrow=False)

    def read_bars_batch_arrow(self, request: BatchBarRequest) -> Any:
        """Return the native Arrow table for Provider/session consumers."""

        return self._read_bars_batch_result(request, as_arrow=True)

    def _read_bars_batch_result(self, request: BatchBarRequest, *, as_arrow: bool) -> Any:
        dataset = self._bar_dataset(request.asset_type, request.frequency)
        if not request.securities:
            if as_arrow:
                try:
                    import pyarrow as pa

                    return pa.Table.from_pandas(
                        pd.DataFrame(columns=["time", "code"] + list(request.columns)),
                        preserve_index=False,
                    )
                except ImportError:
                    pass
            return pd.DataFrame(columns=["time", "code"] + list(request.columns))
        available = set(self._table_columns(dataset))
        time_column = "trade_date" if request.frequency == "1d" else "trade_time"
        code_column = "ts_code"
        if time_column not in available or code_column not in available:
            raise LocalDataSchemaError(
                "DuckDB 表 {} 缺少 {} 或 {}".format(dataset, time_column, code_column)
            )
        selected = [column for column in request.columns if column in available]
        projections = [
            "CAST({} AS TIMESTAMP_NS) AS time".format(time_column),
            "CAST({} AS VARCHAR) AS code".format(code_column),
        ] + ['"{}"'.format(column) for column in selected]
        placeholders = ",".join("?" for _ in request.securities)
        predicates = ["{} IN ({})".format(code_column, placeholders)]
        parameters: List[Any] = list(request.securities)
        if request.start is not None:
            predicates.append("CAST({} AS TIMESTAMP) >= ?".format(time_column))
            parameters.append(request.start.to_pydatetime())
        if request.end is not None:
            predicates.append("CAST({} AS TIMESTAMP) <= ?".format(time_column))
            parameters.append(request.end.to_pydatetime())
        base = 'SELECT {projection} FROM "{dataset}" WHERE {where}'.format(
            projection=", ".join(projections), dataset=dataset, where=" AND ".join(predicates)
        )
        if request.count is not None:
            base = (
                "SELECT * EXCLUDE (_rn) FROM ("
                "SELECT *, row_number() OVER (PARTITION BY code ORDER BY time DESC) AS _rn "
                "FROM ({base})) WHERE _rn <= ?"
            ).format(base=base)
            parameters.append(int(request.count))
        sql = "{} ORDER BY time, code".format(base)
        signature = "bars|{}|{}|{}|{}|{}|count={}".format(
            dataset,
            request.frequency,
            request.columns,
            request.start,
            request.end,
            request.count,
        )
        shards = self.manifest.select_dataset_shards(dataset, start=request.start, end=request.end)
        tables = [
            self._execute_arrow_on_shard(shard, dataset, sql, parameters, signature=signature)
            for shard in shards
        ]
        arrow = self._combine_bar_tables(tables, request.count)
        if as_arrow:
            return arrow
        return arrow.to_pandas()

    @staticmethod
    def _combine_bar_tables(tables: Sequence[Any], count: Optional[int]) -> Any:
        import pyarrow as pa

        if not tables:
            return pa.table({"time": [], "code": []})
        combined = (
            tables[0]
            if len(tables) == 1
            else pa.concat_tables(list(tables), promote_options="default")
        )
        if int(getattr(combined, "num_rows", 0)) == 0:
            return combined
        if count is None:
            return combined.sort_by([("time", "ascending"), ("code", "ascending")])
        # Each annual shard already returns at most N rows per security.  The
        # bounded concatenation therefore contains at most N*years*securities;
        # applying the final tail here preserves exact cross-shard count semantics.
        frame = combined.to_pandas()
        frame = frame.sort_values(["time", "code"], kind="stable")
        frame = frame.groupby("code", sort=False, group_keys=False).tail(int(count))
        frame = frame.sort_values(["time", "code"], kind="stable").reset_index(drop=True)
        return pa.Table.from_pandas(frame, preserve_index=False)

    def read_current_bars(self, request: CurrentBarRequest) -> pd.DataFrame:
        return self.read_bars_batch(
            BatchBarRequest(
                securities=request.securities,
                asset_type=request.asset_type,
                frequency=request.frequency,
                end=request.at,
                columns=request.columns,
                count=1,
            )
        )

    def read_reference_factors_batch(self, request: ReferenceFactorRequest) -> pd.DataFrame:
        return self.read_bars_batch(
            BatchBarRequest(
                securities=request.securities,
                asset_type=request.asset_type,
                frequency=request.frequency,
                end=request.reference_date,
                columns=("adj_factor",),
                count=1,
            )
        )

    def read_trade_days(
        self, start: Optional[pd.Timestamp] = None, end: Optional[pd.Timestamp] = None
    ) -> List[datetime]:
        try:
            dataset = "index_daily"
            available = set(self._table_columns(dataset))
            predicates = ["ts_code = ?"] if "ts_code" in available else []
            parameters: List[Any] = ["000001.SH"] if predicates else []
        except Exception:
            dataset = "stock_daily"
            predicates = []
            parameters = []
        if start is not None:
            predicates.append("CAST(trade_date AS TIMESTAMP) >= ?")
            parameters.append(start.normalize().to_pydatetime())
        if end is not None:
            predicates.append("CAST(trade_date AS TIMESTAMP) <= ?")
            parameters.append(end.normalize().to_pydatetime())
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
        frame = self._execute_frame(
            dataset,
            "SELECT DISTINCT CAST(trade_date AS TIMESTAMP) AS trade_date "
            'FROM "{}"{} ORDER BY trade_date'.format(dataset, where),
            parameters,
            signature="calendar|{}|{}".format(start, end),
        )
        return [pd.Timestamp(value).to_pydatetime() for value in frame.get("trade_date", ())]

    def _read_catalog(self, dataset: str, asset: AssetType) -> pd.DataFrame:
        frame = self._execute_frame(
            dataset,
            'SELECT * FROM "{}" ORDER BY ts_code'.format(dataset),
            signature="catalog|{}".format(dataset),
        )
        if asset == AssetType.FUND:
            if "csname" in frame:
                frame["display_name"] = frame["csname"]
            elif "cname" in frame:
                frame["display_name"] = frame["cname"]
        else:
            frame["display_name"] = frame.get("name", frame.get("ts_code"))
        frame["name"] = frame.get("display_name")
        frame["type"] = asset.value
        frame["start_date"] = pd.to_datetime(
            frame.get("list_date", frame.get("base_date")), errors="coerce"
        ).astype("datetime64[ns]")
        if "delist_date" in frame:
            frame["end_date"] = pd.to_datetime(frame["delist_date"], errors="coerce").astype(
                "datetime64[ns]"
            )
        else:
            frame["end_date"] = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
        return frame.drop_duplicates("ts_code", keep="last").set_index("ts_code")

    def _load_catalogs(self) -> Dict[AssetType, pd.DataFrame]:
        if self._catalogs is None:
            self._catalogs = {
                AssetType.STOCK: self._read_catalog("stock_basic", AssetType.STOCK),
                AssetType.FUND: self._read_catalog("fund_basic", AssetType.FUND),
                AssetType.INDEX: self._read_catalog("index_basic", AssetType.INDEX),
            }
        return self._catalogs

    def read_securities(
        self, asset_types: Sequence[AssetType], date: Optional[pd.Timestamp] = None
    ) -> pd.DataFrame:
        catalogs = self._load_catalogs()
        frames = [catalogs[asset].copy() for asset in asset_types]
        if not frames:
            return pd.DataFrame(columns=("display_name", "name", "start_date", "end_date", "type"))
        frame = pd.concat(frames, axis=0)
        if date is not None:
            frame = frame[
                (frame["start_date"].fillna(pd.Timestamp.min) <= date)
                & (frame["end_date"].fillna(pd.Timestamp.max) >= date)
            ]
        columns = ("display_name", "name", "start_date", "end_date", "type")
        return frame[[column for column in columns if column in frame]].copy()

    def read_security_info_batch(self, request: SecurityInfoRequest) -> pd.DataFrame:
        frame = self.read_securities(
            (AssetType.STOCK, AssetType.FUND, AssetType.INDEX), request.date
        )
        return frame.loc[frame.index.intersection(request.securities)].copy()

    def read_index_components(
        self, index_code: str, date: Optional[pd.Timestamp] = None
    ) -> pd.DataFrame:
        predicates = ["ts_code = ?", "con_codes IS NOT NULL"]
        parameters: List[Any] = [index_code]
        if date is not None:
            predicates.append("CAST(trade_date AS TIMESTAMP) <= ?")
            parameters.append(date.normalize().to_pydatetime())
        frame = self._execute_frame(
            "index_daily",
            "SELECT con_codes, weights FROM index_daily WHERE {} "
            "ORDER BY trade_date DESC LIMIT 1".format(" AND ".join(predicates)),
            parameters,
            signature="index_components|{}|{}".format(index_code, date),
        )
        if frame.empty:
            return pd.DataFrame(columns=("code", "weight"))
        raw_codes = frame.iloc[0]["con_codes"]
        raw_weights = frame.iloc[0]["weights"]
        try:
            codes = [] if raw_codes is None else list(raw_codes)
        except TypeError:
            codes = []
        try:
            weights = [] if raw_weights is None else list(raw_weights)
        except TypeError:
            weights = []
        if len(weights) != len(codes):
            weights = [float("nan")] * len(codes)
        return pd.DataFrame({"code": codes, "weight": weights})

    @staticmethod
    def _normalize_financial_table(table: str) -> str:
        aliases = {
            "balancesheet": "balance",
            "balance_sheet": "balance",
            "cashflow": "cash_flow",
            "financial_indicator": "indicator",
            "fina_indicator": "indicator",
        }
        normalized = table.lower().strip()
        return aliases.get(normalized, normalized)

    def read_financial_table(
        self,
        table: str,
        columns: Sequence[str],
        *,
        as_of: pd.Timestamp,
        stat_date: Optional[pd.Timestamp] = None,
        securities: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        dataset = self._normalize_financial_table(table)
        if dataset == "valuation":
            dataset = "stock_daily"
            time_column = "trade_date"
        else:
            time_column = "ann_date"
        available = set(self._table_columns(dataset))
        missing = set(columns) - available
        if missing:
            raise LocalDataSchemaError(
                "DuckDB 表 {} 缺少请求字段：{}".format(dataset, ", ".join(sorted(missing)))
            )
        mandatory = {"ts_code", time_column}
        if dataset != "stock_daily":
            mandatory.add("end_date")
        selected = [
            item for item in dict.fromkeys(list(columns) + list(mandatory)) if item in available
        ]
        predicates = ["CAST({} AS TIMESTAMP) <= ?".format(time_column)]
        parameters: List[Any] = [as_of.to_pydatetime()]
        if stat_date is not None and "end_date" in available:
            predicates.append("CAST(end_date AS DATE) = ?")
            parameters.append(stat_date.date())
        if securities:
            placeholders = ",".join("?" for _ in securities)
            predicates.append("ts_code IN ({})".format(placeholders))
            parameters.extend(securities)
        frame = self._execute_frame(
            dataset,
            'SELECT {} FROM "{}" WHERE {}'.format(
                ", ".join('"{}"'.format(item) for item in selected),
                dataset,
                " AND ".join(predicates),
            ),
            parameters,
            signature="finance|{}|{}|{}".format(dataset, as_of, stat_date),
        )
        if frame.empty:
            return frame
        frame[time_column] = pd.to_datetime(frame[time_column], errors="coerce").astype(
            "datetime64[ns]"
        )
        sort = ["ts_code", time_column]
        if "end_date" in frame:
            frame["end_date"] = pd.to_datetime(frame["end_date"], errors="coerce").astype(
                "datetime64[ns]"
            )
            sort = ["ts_code", "end_date", time_column]
        frame = frame.sort_values(sort, kind="stable")
        subset = ["ts_code"] if stat_date is None else ["ts_code", "end_date"]
        return frame.drop_duplicates(subset, keep="last").reset_index(drop=True)

    def read_corporate_actions(
        self,
        security: str,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        return self.read_corporate_actions_batch(
            CorporateActionRequest((security,), start=start, end=end)
        )

    def read_corporate_actions_batch(self, request: CorporateActionRequest) -> pd.DataFrame:
        if not request.securities:
            return pd.DataFrame()
        placeholders = ",".join("?" for _ in request.securities)
        predicates = ["security IN ({})".format(placeholders)]
        parameters: List[Any] = list(request.securities)
        if not request.include_non_effective:
            predicates.append("lower(status) = 'effective'")
        if request.start is not None:
            predicates.append("CAST(event_date AS TIMESTAMP) >= ?")
            parameters.append(request.start.normalize().to_pydatetime())
        if request.end is not None:
            predicates.append("CAST(event_date AS TIMESTAMP) <= ?")
            parameters.append(request.end.normalize().to_pydatetime())
        return (
            self._execute_frame(
                "corporate_actions",
                "SELECT * FROM corporate_actions WHERE {} "
                "ORDER BY event_date, security, event_id".format(" AND ".join(predicates)),
                parameters,
                signature="actions|{}|{}".format(request.start, request.end),
            )
            .drop_duplicates("event_id", keep="last")
            .reset_index(drop=True)
        )

    def diagnostics(self) -> Dict[str, Any]:
        result: Dict[str, Any] = dict(super().diagnostics())
        manifest = self.manifest
        result.update(
            {
                "manifest_identity": manifest.generation_id,
                "manifest_path": str(self._resolved_manifest_path),
                "schema_version": manifest.schema_version,
                "storage_version": manifest.storage_version,
                "duckdb_runtime_version": str(self.duckdb.__version__),
                "duckdb_build_version": manifest.duckdb_version,
                "shards": [shard.shard_id for shard in manifest.shards],
                "threads": self.threads,
                "memory_limit": self.memory_limit,
                "temp_directory": None if self.temp_directory is None else str(self.temp_directory),
                "max_temp_directory_size": self.max_temp_directory_size,
                "fully_materialized": True,
                "parquet_fallback": False,
            }
        )
        return result
