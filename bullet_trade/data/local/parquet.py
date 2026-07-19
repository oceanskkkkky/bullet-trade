"""Parquet implementation of the local market-data backend."""

from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple, overload

import pandas as pd

from .base import (
    AssetType,
    BarRequest,
    LocalDataBackend,
    LocalDataCapabilityError,
    LocalDataConfigurationError,
    LocalDataQueryError,
    LocalDataSchemaError,
)
from .schemas import CANONICAL_SCHEMA_VERSION, DATASET_REGISTRY


class ParquetDataBackend(LocalDataBackend):
    """Read the repository's Tushare-style Parquet data lake.

    Large daily files are filtered at the Arrow scan boundary. Minute and index
    bars are read from per-security files. No source data is copied or mutated.
    """

    name = "parquet"
    _FREQUENCIES = {"1d", "1m", "5m", "15m", "30m", "60m"}
    _FINANCIAL_FILES = {
        "income": "income_cleaned.parquet",
        "balance": "balancesheet_cleaned.parquet",
        "cash_flow": "cashflow_cleaned.parquet",
        "indicator": "fina_indicator_cleaned.parquet",
        "forecast": "forecast_cleaned.parquet",
        "express": "express_cleaned.parquet",
    }

    def __init__(self, root: Path, *, strict: bool = True) -> None:
        self.root = Path(root).resolve()
        self.strict = bool(strict)
        self._paths: Dict[str, Path] = {}
        self._calendar: Optional[pd.DatetimeIndex] = None
        self._catalogs: Optional[Dict[AssetType, pd.DataFrame]] = None

    @staticmethod
    def ensure_dependency() -> None:
        try:
            import pyarrow  # noqa: F401
        except ImportError as exc:  # pragma: no cover - depends on installation
            raise LocalDataConfigurationError(
                "LocalDataProvider 的 Parquet 后端需要 pyarrow；"
                "请执行 `pip install bullet-trade[local]`"
            ) from exc

    @overload
    def _find_file(self, filename: str, *, required: Literal[True] = True) -> Path: ...

    @overload
    def _find_file(self, filename: str, *, required: Literal[False]) -> Optional[Path]: ...

    def _find_file(self, filename: str, *, required: bool = True) -> Optional[Path]:
        cache_key = "file:" + filename
        cached = self._paths.get(cache_key)
        if cached is not None:
            return cached

        direct = self.root / filename
        candidates: List[Path] = [direct]
        if self.root.exists():
            candidates.extend(child / filename for child in self.root.iterdir() if child.is_dir())
        path = next((candidate for candidate in candidates if candidate.is_file()), None)
        if path is None and required:
            raise LocalDataSchemaError(
                "本地 Parquet 数据集缺少文件 {!r}（数据根目录：{}）".format(filename, self.root)
            )
        if path is not None:
            self._paths[cache_key] = path
        return path

    @overload
    def _find_directory(self, dirname: str, *, required: Literal[True] = True) -> Path: ...

    @overload
    def _find_directory(self, dirname: str, *, required: Literal[False]) -> Optional[Path]: ...

    def _find_directory(self, dirname: str, *, required: bool = True) -> Optional[Path]:
        cache_key = "dir:" + dirname
        cached = self._paths.get(cache_key)
        if cached is not None:
            return cached

        direct = self.root / dirname
        candidates: List[Path] = [direct]
        if self.root.exists():
            candidates.extend(child / dirname for child in self.root.iterdir() if child.is_dir())
        path = next((candidate for candidate in candidates if candidate.is_dir()), None)
        if path is None and required:
            raise LocalDataSchemaError(
                "本地 Parquet 数据集缺少目录 {!r}（数据根目录：{}）".format(dirname, self.root)
            )
        if path is not None:
            self._paths[cache_key] = path
        return path

    def _find_matching_file(self, pattern: str) -> Optional[Path]:
        cache_key = "glob:" + pattern
        cached = self._paths.get(cache_key)
        if cached is not None:
            return cached
        candidates = sorted(self.root.glob(pattern))
        for child in self.root.iterdir():
            if child.is_dir():
                candidates.extend(sorted(child.glob(pattern)))
        path = next((candidate for candidate in candidates if candidate.is_file()), None)
        if path is not None:
            self._paths[cache_key] = path
        return path

    @staticmethod
    @lru_cache(maxsize=128)
    def _schema_columns(path_text: str) -> Tuple[str, ...]:
        try:
            import pyarrow.parquet as pq

            return tuple(pq.ParquetFile(path_text).schema_arrow.names)
        except Exception as exc:
            raise LocalDataSchemaError(
                "无法读取 Parquet schema：{} ({})".format(path_text, exc)
            ) from exc

    @staticmethod
    @lru_cache(maxsize=32)
    def _minimum_timestamp(path_text: str, column: str) -> pd.Timestamp:
        try:
            import pyarrow.parquet as pq

            parquet_file = pq.ParquetFile(path_text)
            values: List[Any] = []
            for group_index in range(parquet_file.num_row_groups):
                row_group = parquet_file.metadata.row_group(group_index)
                for column_index in range(row_group.num_columns):
                    metadata = row_group.column(column_index)
                    if metadata.path_in_schema != column:
                        continue
                    statistics = metadata.statistics
                    if statistics is not None and statistics.has_min_max:
                        values.append(statistics.min)
                    break
            parsed = pd.to_datetime(pd.Series(values), errors="coerce").dropna()
            if not parsed.empty:
                return pd.Timestamp(parsed.min()).normalize()
        except Exception as exc:
            raise LocalDataSchemaError(
                "无法读取 Parquet 日期边界：{}:{} ({})".format(path_text, column, exc)
            ) from exc
        raise LocalDataSchemaError(
            "Parquet 文件没有可用的日期统计：{}:{}".format(path_text, column)
        )

    def _require_columns(self, path: Path, required: Iterable[str]) -> None:
        existing = set(self._schema_columns(str(path)))
        missing = sorted(set(required) - existing)
        if missing:
            raise LocalDataSchemaError(
                "Parquet 文件 {} 缺少必需字段：{}".format(path, ", ".join(missing))
            )

    def validate(self) -> None:
        self.ensure_dependency()
        if not self.root.is_dir():
            raise LocalDataConfigurationError(
                "LOCAL_DATA_PATH 不存在或不是目录：{}。默认值为 `.\\data\\parquet`，"
                "相对于项目根目录解析。".format(self.root)
            )
        if not self.strict:
            return

        required_datasets = (
            "stock_basic",
            "stock_daily",
            "fund_basic",
            "fund_daily",
            "index_basic",
            "income",
            "balance",
            "cash_flow",
            "indicator",
        )
        for dataset_name in required_datasets:
            schema = DATASET_REGISTRY[dataset_name]
            filename = schema.source_patterns[0]
            path = self._find_file(filename)
            self._require_columns(path, schema.required_columns)

    def _read_parquet(
        self,
        path: Path,
        *,
        columns: Sequence[str],
        filters: Optional[List[Tuple[str, str, Any]]] = None,
    ) -> pd.DataFrame:
        available = set(self._schema_columns(str(path)))
        selected = list(dict.fromkeys(column for column in columns if column in available))
        if not selected:
            return pd.DataFrame()
        started = perf_counter()
        try:
            frame = pd.read_parquet(
                path,
                engine="pyarrow",
                columns=selected,
                filters=filters,
            )
            # The source lake was written by pandas and stores trade_date and
            # ts_code as physical columns referenced by pandas index metadata.
            # pandas consequently restores them as an Index/MultiIndex even
            # when Arrow selected them as columns. Canonical backends expose
            # explicit columns, independent of writer metadata.
            index_names = [
                name
                for name in frame.index.names
                if name is not None and name in selected and name not in frame.columns
            ]
            if index_names:
                frame = frame.reset_index(level=index_names)
            try:
                result_bytes = int(frame.memory_usage(index=True, deep=True).sum())
            except (AttributeError, TypeError, ValueError):
                result_bytes = 0
            filter_signature = repr(filters or ())
            self._record_query(
                "read_parquet",
                rows=len(frame),
                elapsed_seconds=perf_counter() - started,
                result_bytes=result_bytes,
                signature="{}|{}|{}".format(path, tuple(selected), filter_signature),
            )
            return frame
        except Exception as exc:
            raise LocalDataQueryError("读取 Parquet 失败：{} ({})".format(path, exc)) from exc

    def _load_catalogs(self) -> Dict[AssetType, pd.DataFrame]:
        if self._catalogs is not None:
            return self._catalogs

        stock_path = self._find_file("stock_basic_data.parquet")
        stock = self._read_parquet(
            stock_path,
            columns=("ts_code", "name", "list_date", "delist_date", "industry", "market"),
        )
        stock = stock.rename(columns={"name": "display_name"})
        stock["name"] = stock.get("display_name")
        stock["type"] = AssetType.STOCK.value

        fund_path = self._find_file("etf_basic_data.parquet")
        fund = self._read_parquet(
            fund_path,
            columns=("ts_code", "csname", "cname", "list_date", "delist_date", "etf_type"),
        )
        if "csname" in fund.columns:
            fund["display_name"] = fund["csname"]
        elif "cname" in fund.columns:
            fund["display_name"] = fund["cname"]
        else:
            fund["display_name"] = fund["ts_code"]
        fund["name"] = fund["display_name"]
        fund["type"] = AssetType.FUND.value

        index_path = self._find_file("index_basic.parquet")
        index = self._read_parquet(
            index_path,
            columns=("ts_code", "name", "list_date", "base_date", "category", "market"),
        )
        index = index.rename(columns={"name": "display_name"})
        index["name"] = index.get("display_name")
        index["type"] = AssetType.INDEX.value

        catalogs: Dict[AssetType, pd.DataFrame] = {}
        for asset, frame in (
            (AssetType.STOCK, stock),
            (AssetType.FUND, fund),
            (AssetType.INDEX, index),
        ):
            if "ts_code" not in frame.columns:
                raise LocalDataSchemaError("{} 基础信息缺少 ts_code".format(asset.value))
            frame = frame.drop_duplicates("ts_code", keep="last").set_index("ts_code")
            frame["start_date"] = pd.to_datetime(
                frame.get("list_date", frame.get("base_date")), errors="coerce"
            )
            if "delist_date" in frame.columns:
                frame["end_date"] = pd.to_datetime(frame["delist_date"], errors="coerce")
            else:
                frame["end_date"] = pd.NaT
            catalogs[asset] = frame
        self._catalogs = catalogs
        return catalogs

    def asset_type(self, security: str) -> AssetType:
        code = security.upper()
        catalogs = self._load_catalogs()
        for asset in (AssetType.FUND, AssetType.INDEX, AssetType.STOCK):
            if code in catalogs[asset].index:
                return asset
        raise LocalDataQueryError("本地基础信息中不存在证券代码：{}".format(security))

    @staticmethod
    def _frequency_dir(asset: AssetType, frequency: str) -> str:
        prefix = {
            AssetType.STOCK: "stock",
            AssetType.FUND: "etf",
            AssetType.INDEX: "index",
        }[asset]
        suffix = frequency[:-1]
        return "{}_{}min".format(prefix, suffix)

    def _bar_path(self, request: BarRequest) -> Path:
        if request.frequency not in self._FREQUENCIES:
            raise LocalDataQueryError(
                "不支持频率 {!r}；可用频率：{}".format(
                    request.frequency, ", ".join(sorted(self._FREQUENCIES))
                )
            )
        if request.frequency == "1d":
            if request.asset_type == AssetType.STOCK:
                return self._find_file("stock_daily.parquet")
            if request.asset_type == AssetType.FUND:
                return self._find_file("etf_daily.parquet")
            directory = self._find_directory("index_daily")
            path = directory / (request.security + ".parquet")
        else:
            directory = self._find_directory(
                self._frequency_dir(request.asset_type, request.frequency)
            )
            path = directory / (request.security + ".parquet")
        if not path.is_file():
            raise LocalDataQueryError(
                "本地数据缺少 {} {} 行情文件：{}".format(request.security, request.frequency, path)
            )
        return path

    def read_bars(self, request: BarRequest) -> pd.DataFrame:
        path = self._bar_path(request)
        is_minute = request.frequency != "1d"
        time_column = "trade_time" if is_minute else "trade_date"
        columns = list(request.columns) + ["ts_code", time_column, "trade_date"]
        filters: List[Tuple[str, str, Any]] = []
        if request.asset_type in (AssetType.STOCK, AssetType.FUND) and request.frequency == "1d":
            filters.append(("ts_code", "==", request.security))
        if request.start is not None:
            filters.append((time_column, ">=", request.start.to_pydatetime()))
        if request.end is not None:
            filters.append((time_column, "<=", request.end.to_pydatetime()))

        frame = self._read_parquet(path, columns=columns, filters=filters or None)
        if request.asset_type == AssetType.STOCK and request.frequency == "1d":
            legacy = self._read_legacy_stock_bars(request, columns, path)
            if not legacy.empty:
                frame = pd.concat((legacy, frame), ignore_index=True, sort=False)
        if frame.empty:
            return pd.DataFrame(columns=["time", "code"] + list(request.columns))
        if time_column not in frame.columns:
            raise LocalDataSchemaError("{} 缺少时间字段 {}".format(path, time_column))
        frame["time"] = pd.to_datetime(frame[time_column], errors="coerce")
        frame = frame.dropna(subset=["time"])
        if "ts_code" not in frame.columns:
            frame["ts_code"] = request.security
        frame["code"] = frame["ts_code"].fillna(request.security)
        frame = frame.sort_values("time").drop_duplicates("time", keep="last")
        drop_columns = [
            column for column in ("trade_time", "trade_date", "ts_code") if column in frame
        ]
        frame = frame.drop(columns=drop_columns)
        ordered = ["time", "code"] + [column for column in request.columns if column in frame]
        return frame[[column for column in ordered if column in frame.columns]].reset_index(
            drop=True
        )

    def _read_legacy_stock_bars(
        self,
        request: BarRequest,
        columns: Sequence[str],
        primary_path: Path,
    ) -> pd.DataFrame:
        """Read only the pre-primary gap from the long-history stock file."""

        legacy_path = self._find_matching_file("daily_adj_*.parquet")
        if legacy_path is None:
            return pd.DataFrame()
        primary_start = self._minimum_timestamp(str(primary_path), "trade_date")
        if request.start is not None and request.start.normalize() >= primary_start:
            return pd.DataFrame()
        legacy_end = primary_start - pd.Timedelta(days=1)
        if request.end is not None:
            legacy_end = min(legacy_end, request.end.normalize())
        if request.start is not None and request.start.normalize() > legacy_end:
            return pd.DataFrame()
        filters: List[Tuple[str, str, Any]] = [
            ("ts_code", "==", request.security),
            ("trade_date", "<=", legacy_end.strftime("%Y%m%d")),
        ]
        if request.start is not None:
            filters.append(("trade_date", ">=", request.start.strftime("%Y%m%d")))
        return self._read_parquet(legacy_path, columns=columns, filters=filters)

    def _calendar_path(self) -> Path:
        index_dir = self._find_directory("index_daily", required=False)
        if index_dir is not None:
            reference = index_dir / "000001.SH.parquet"
            if reference.is_file():
                return reference
            files = sorted(index_dir.glob("*.parquet"))
            if files:
                return files[0]
        return self._find_file("stock_daily.parquet")

    def _load_calendar(self) -> pd.DatetimeIndex:
        if self._calendar is None:
            path = self._calendar_path()
            frame = self._read_parquet(path, columns=("trade_date",))
            if "trade_date" not in frame:
                raise LocalDataSchemaError("交易日历参考文件缺少 trade_date：{}".format(path))
            values = pd.to_datetime(frame["trade_date"], errors="coerce").dropna().dt.normalize()
            self._calendar = pd.DatetimeIndex(values.unique()).sort_values()
        return self._calendar

    def read_trade_days(
        self,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
    ) -> List[datetime]:
        calendar = self._load_calendar()
        if start is not None:
            calendar = calendar[calendar >= start.normalize()]
        if end is not None:
            calendar = calendar[calendar <= end.normalize()]
        return [timestamp.to_pydatetime() for timestamp in calendar]

    def read_securities(
        self,
        asset_types: Sequence[AssetType],
        date: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        catalogs = self._load_catalogs()
        frames = [catalogs[asset].copy() for asset in asset_types]
        if not frames:
            return pd.DataFrame(columns=("display_name", "name", "start_date", "end_date", "type"))
        frame = pd.concat(frames, axis=0)
        if date is not None:
            start = frame["start_date"].fillna(pd.Timestamp.min)
            end = frame["end_date"].fillna(pd.Timestamp.max)
            frame = frame[(start <= date) & (end >= date)]
        columns = ("display_name", "name", "start_date", "end_date", "type")
        return frame[[column for column in columns if column in frame.columns]].copy()

    def read_index_components(
        self,
        index_code: str,
        date: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        path = self._find_directory("index_daily") / (index_code + ".parquet")
        if not path.is_file():
            raise LocalDataQueryError("本地指数文件不存在：{}".format(path))
        filters = None
        if date is not None:
            filters = [("trade_date", "<=", date.to_pydatetime())]
        frame = self._read_parquet(
            path,
            columns=("trade_date", "con_codes", "weights"),
            filters=filters,
        )
        if frame.empty or "con_codes" not in frame:
            return pd.DataFrame(columns=("code", "weight"))
        frame = frame[frame["con_codes"].notna()].sort_values("trade_date")
        if frame.empty:
            return pd.DataFrame(columns=("code", "weight"))
        row = frame.iloc[-1]
        raw_codes = row["con_codes"]
        raw_weights = row.get("weights")
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
        normalized = table.lower().strip()
        aliases = {
            "balancesheet": "balance",
            "balance_sheet": "balance",
            "cashflow": "cash_flow",
            "financial_indicator": "indicator",
            "fina_indicator": "indicator",
        }
        return aliases.get(normalized, normalized)

    def _financial_path(self, table: str) -> Path:
        normalized = self._normalize_financial_table(table)
        filename = self._FINANCIAL_FILES.get(normalized)
        if filename is None:
            raise LocalDataQueryError(
                "未知财务表 {!r}；可用表：{}".format(
                    table, ", ".join(sorted(self._FINANCIAL_FILES))
                )
            )
        return self._find_file(filename)

    def read_financial_table(
        self,
        table: str,
        columns: Sequence[str],
        *,
        as_of: pd.Timestamp,
        stat_date: Optional[pd.Timestamp] = None,
        securities: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        normalized = self._normalize_financial_table(table)
        if normalized == "valuation":
            return self._read_valuation(columns, as_of=as_of, securities=securities)

        path = self._financial_path(normalized)
        schema = set(self._schema_columns(str(path)))
        required = {"ts_code", "end_date", "ann_date"}
        missing = required - schema
        if missing:
            raise LocalDataSchemaError(
                "{} 缺少财务时点字段：{}".format(path, ", ".join(sorted(missing)))
            )
        selected = set(columns) | required | {"update_flag", "report_type"}
        missing_requested = set(columns) - schema
        if missing_requested:
            raise LocalDataSchemaError(
                "{} 缺少请求的财务字段：{}".format(path, ", ".join(sorted(missing_requested)))
            )
        # The financial lake stores Tushare dates as sortable YYYYMMDD strings.
        filters: List[Tuple[str, str, Any]] = [("ann_date", "<=", as_of.strftime("%Y%m%d"))]
        if stat_date is not None:
            filters.append(("end_date", "==", stat_date.strftime("%Y%m%d")))
        if securities:
            filters.append(("ts_code", "in", list(securities)))
        frame = self._read_parquet(path, columns=tuple(selected), filters=filters)
        if frame.empty:
            return frame
        frame["ann_date"] = pd.to_datetime(frame["ann_date"], errors="coerce")
        frame["end_date"] = pd.to_datetime(frame["end_date"], errors="coerce")
        frame = frame.dropna(subset=["ann_date", "end_date"])
        sort_columns = ["ts_code", "end_date"]
        if "report_type" in frame.columns:
            frame["_preferred_report"] = frame["report_type"].astype(str).isin(("1", "1.0"))
            sort_columns.append("_preferred_report")
        sort_columns.append("ann_date")
        if "update_flag" in frame.columns:
            sort_columns.append("update_flag")
        frame = frame.sort_values(sort_columns)
        frame = frame.drop_duplicates(("ts_code", "end_date"), keep="last")
        frame = frame.drop(columns=("_preferred_report",), errors="ignore")
        if stat_date is None:
            frame = frame.sort_values(["ts_code", "end_date", "ann_date"])
            frame = frame.drop_duplicates("ts_code", keep="last")
        return frame.reset_index(drop=True)

    def read_corporate_actions(
        self,
        security: str,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        """Read effective canonical events without any network fallback."""

        path = self._find_file("corporate_actions.parquet", required=False)
        if path is None:
            raise LocalDataCapabilityError(
                "本地数据根目录 {} 缺少 corporate_actions.parquet；"
                "请先运行显式 Tushare 公司行为导入命令".format(self.root)
            )
        schema = DATASET_REGISTRY["corporate_actions"]
        self._require_columns(path, schema.required_columns)
        columns = list(schema.required_columns) + list(schema.optional_columns)
        filters: List[Tuple[str, str, Any]] = [("security", "==", security)]
        if start is not None:
            filters.append(("event_date", ">=", start.normalize().to_pydatetime()))
        if end is not None:
            filters.append(("event_date", "<=", end.normalize().to_pydatetime()))
        frame = self._read_parquet(path, columns=columns, filters=filters)
        if frame.empty:
            return pd.DataFrame(columns=columns)
        frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")
        frame = frame.dropna(subset=["event_date"])
        frame = frame.loc[frame["status"].astype(str).str.lower() == "effective"]
        frame = frame.sort_values(["event_date", "security", "event_id"], kind="stable")
        return frame.drop_duplicates("event_id", keep="last").reset_index(drop=True)

    def _read_valuation(
        self,
        columns: Sequence[str],
        *,
        as_of: pd.Timestamp,
        securities: Optional[Sequence[str]],
    ) -> pd.DataFrame:
        path = self._find_file("stock_daily.parquet")
        schema = set(self._schema_columns(str(path)))
        missing_requested = set(columns) - schema
        if missing_requested:
            raise LocalDataSchemaError(
                "{} 缺少请求的估值字段：{}".format(path, ", ".join(sorted(missing_requested)))
            )
        selected = set(columns) | {"ts_code", "trade_date"}
        filters: List[Tuple[str, str, Any]] = [
            ("trade_date", "<=", as_of.to_pydatetime()),
            ("trade_date", ">=", (as_of - pd.Timedelta(days=45)).to_pydatetime()),
        ]
        if securities:
            filters.append(("ts_code", "in", list(securities)))
        frame = self._read_parquet(path, columns=tuple(selected), filters=filters)
        if frame.empty:
            return frame
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        frame = frame.sort_values(["ts_code", "trade_date"])
        return frame.drop_duplicates("ts_code", keep="last").reset_index(drop=True)

    def diagnostics(self) -> Dict[str, Any]:
        result = super().diagnostics()
        result.update(
            {
                "backend": self.name,
                "root": str(self.root),
                "strict": self.strict,
                "calendar_days": 0 if self._calendar is None else len(self._calendar),
                "schema_version": CANONICAL_SCHEMA_VERSION,
                "manifest_identity": None,
                "corporate_actions_available": self._find_file(
                    "corporate_actions.parquet", required=False
                )
                is not None,
                "cache_statistics": {
                    "schema": self._schema_columns.cache_info()._asdict(),
                    "date_boundary": self._minimum_timestamp.cache_info()._asdict(),
                    "catalog_loaded": self._catalogs is not None,
                    "calendar_loaded": self._calendar is not None,
                },
            }
        )
        return result
