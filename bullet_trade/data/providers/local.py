"""Provider facade for deterministic local market data."""

from __future__ import annotations

import math
from datetime import date as Date
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, NoReturn, Optional, Sequence, Tuple, Union, cast

import pandas as pd

from ..local import (
    AssetType,
    BarRequest,
    BatchBarRequest,
    CorporateActionRequest,
    LocalDataBackend,
    LocalDataConfigurationError,
    LocalDataUnsupportedOperationError,
    ReferenceFactorRequest,
)
from ..local.fundamentals import FundamentalsQueryAdapter
from ..local.parquet import ParquetDataBackend
from .base import DataProvider

DateLike = Optional[Union[str, datetime, Date]]


class LocalDataProvider(DataProvider):
    """JoinQuant-compatible facade over a strictly local backend.

    ``LocalDataProvider`` is the stable public integration point. Storage
    details live behind :class:`LocalDataBackend`, so adding DuckDB later does
    not require changes to strategy code, the data API, or this method surface.
    """

    name = "local"
    _TS_SUFFIX_TO_JQ = {"SH": "XSHG", "SZ": "XSHE", "BJ": "XBSE"}
    _JQ_SUFFIX_TO_TS = {value: key for key, value in _TS_SUFFIX_TO_JQ.items()}
    _DEFAULT_FIELDS = ("open", "close", "high", "low", "volume", "money")
    _FIELD_TO_SOURCE = {
        "volume": "vol",
        "money": "amount",
        "high_limit": "up_limit",
        "low_limit": "down_limit",
        "paused": "suspend_type",
    }

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        backend: Optional[LocalDataBackend] = None,
    ) -> None:
        self.config = dict(config or {})
        self._ready = False
        self._query_mode = str(self.config.get("query_mode") or "batch").strip().lower()
        if self._query_mode not in ("batch", "scalar"):
            raise LocalDataConfigurationError(
                "LOCAL_DATA_QUERY_MODE 必须为 'batch' 或 'scalar'，当前值：{!r}".format(
                    self._query_mode
                )
            )
        self._backend = backend or self._create_backend(self.config)
        self._fundamentals = FundamentalsQueryAdapter(
            self._backend,
            to_ts_code=self._to_ts_code,
            to_jq_code=self._to_jq_code,
        )

    @staticmethod
    def _as_bool(value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in ("1", "true", "yes", "on"):
            return True
        if normalized in ("0", "false", "no", "off"):
            return False
        raise LocalDataConfigurationError("无法解析布尔配置值：{!r}".format(value))

    @classmethod
    def _resolve_data_root(cls, value: Optional[Union[str, Path]]) -> Path:
        raw_path = Path(value or Path("data") / "parquet").expanduser()
        if not raw_path.is_absolute():
            current = Path.cwd().resolve()
            project_root: Optional[Path] = None
            for candidate in (current,) + tuple(current.parents):
                if (candidate / "pyproject.toml").is_file() or (candidate / ".git").exists():
                    project_root = candidate
                    break
            source_root = Path(__file__).resolve().parents[3]
            if project_root is None and (source_root / "pyproject.toml").is_file():
                project_root = source_root
            if project_root is None:
                project_root = current
            raw_path = project_root / raw_path
        return raw_path.resolve()

    @classmethod
    def _create_backend(cls, config: Dict[str, Any]) -> LocalDataBackend:
        backend_name = str(config.get("backend") or "parquet").strip().lower()
        root = cls._resolve_data_root(
            config.get("path") or config.get("data_path") or config.get("root")
        )
        strict = cls._as_bool(config.get("strict"), True)
        if backend_name == "parquet":
            return ParquetDataBackend(root, strict=strict)
        if backend_name == "duckdb":
            from ..local.duckdb import DuckDBDataBackend

            temp_value = config.get("temp_directory")
            temp_directory = cls._resolve_data_root(temp_value) if temp_value else None
            return DuckDBDataBackend(
                root,
                threads=int(config.get("threads") or 8),
                memory_limit=str(config.get("memory_limit") or "12GB"),
                temp_directory=temp_directory,
                max_temp_directory_size=str(config.get("max_temp_directory_size") or "100GB"),
            )
        raise LocalDataConfigurationError(
            "未知本地数据后端 {!r}；当前支持 parquet、duckdb".format(backend_name)
        )

    @property
    def backend(self) -> LocalDataBackend:
        """Expose the storage-neutral backend for diagnostics and extensions."""

        return self._backend

    @classmethod
    def _to_ts_code(cls, security: str) -> str:
        if not isinstance(security, str) or "." not in security:
            return security
        code, suffix = security.rsplit(".", 1)
        return "{}.{}".format(code, cls._JQ_SUFFIX_TO_TS.get(suffix.upper(), suffix.upper()))

    @classmethod
    def _to_jq_code(cls, security: str) -> str:
        if not isinstance(security, str) or "." not in security:
            return security
        code, suffix = security.rsplit(".", 1)
        return "{}.{}".format(code, cls._TS_SUFFIX_TO_JQ.get(suffix.upper(), suffix.upper()))

    @staticmethod
    def _timestamp(value: DateLike, *, normalize: bool = False) -> Optional[pd.Timestamp]:
        if value is None:
            return None
        try:
            timestamp = pd.Timestamp(value)
        except (TypeError, ValueError) as exc:
            raise LocalDataConfigurationError("无效日期参数：{!r}".format(value)) from exc
        return timestamp.normalize() if normalize else timestamp

    @staticmethod
    def _normalize_frequency(frequency: str) -> str:
        normalized = str(frequency).strip().lower()
        aliases = {
            "daily": "1d",
            "day": "1d",
            "d": "1d",
            "minute": "1m",
            "min": "1m",
            "1min": "1m",
            "5min": "5m",
            "15min": "15m",
            "30min": "30m",
            "60min": "60m",
        }
        return aliases.get(normalized, normalized)

    def auth(
        self,
        user: Optional[str] = None,
        pwd: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        _ = user, pwd, host, port
        if not self._ready:
            self._backend.validate()
            self._ready = True

    def _ensure_ready(self) -> None:
        if not self._ready:
            self.auth()

    @staticmethod
    def _source_columns(fields: Sequence[str], *, include_factor: bool) -> Tuple[str, ...]:
        columns: List[str] = []
        for field in fields:
            source = LocalDataProvider._FIELD_TO_SOURCE.get(field, field)
            if source not in columns:
                columns.append(source)
        for required in ("vol", "amount", "adj_factor", "suspend_type"):
            if required == "adj_factor" and not include_factor:
                continue
            if required not in columns:
                columns.append(required)
        return tuple(columns)

    def _count_start(
        self,
        frequency: str,
        end: Optional[pd.Timestamp],
        count: Optional[int],
    ) -> Optional[pd.Timestamp]:
        if not count or count <= 0:
            return None
        effective_end = end or pd.Timestamp.today()
        if frequency == "1d":
            days = self.get_trade_days(end_date=effective_end, count=count + 2)
        else:
            minutes = int(frequency[:-1])
            required_days = int(math.ceil(count * minutes / 240.0)) + 3
            days = self.get_trade_days(end_date=effective_end, count=required_days)
        return pd.Timestamp(days[0]) if days else None

    def _reference_factor(
        self,
        security: str,
        asset_type: AssetType,
        frequency: str,
        reference_date: pd.Timestamp,
        *,
        from_beginning: bool = False,
    ) -> Optional[float]:
        session = None
        cache_key = None
        try:
            from ..backtest_session import get_current_backtest_data_session

            session = get_current_backtest_data_session()
            cache_key = (
                "reference_factor",
                self._backend.diagnostics().get("manifest_identity")
                or self._backend.diagnostics().get("root"),
                security,
                asset_type.value,
                frequency,
                reference_date.isoformat(),
                bool(from_beginning),
            )
            cached = session.get_price_block(cache_key) if session is not None else None
            if cached is not None:
                return float(cached)
        except Exception:
            session = None
            cache_key = None
        start = None if from_beginning else reference_date.normalize() - pd.Timedelta(days=45)
        end = reference_date
        if frequency != "1d" and end == end.normalize():
            end = end.normalize() + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        request = BarRequest(
            security=security,
            asset_type=asset_type,
            frequency=frequency,
            start=start,
            end=end,
            columns=("adj_factor",),
        )
        factor_frame = self._backend.read_bars(request)
        if factor_frame.empty or "adj_factor" not in factor_frame:
            return None
        factors = pd.to_numeric(factor_frame["adj_factor"], errors="coerce").dropna()
        result = float(factors.iloc[-1]) if not factors.empty else None
        if result is not None and session is not None and cache_key is not None:
            session.set_price_block(cache_key, result, rows=1)
        return result

    def _apply_adjustment(
        self,
        frame: pd.DataFrame,
        *,
        security: str,
        asset_type: AssetType,
        frequency: str,
        fq: Optional[str],
        pre_factor_ref_date: DateLike,
    ) -> pd.DataFrame:
        if asset_type == AssetType.INDEX or fq not in ("pre", "post"):
            return frame
        if "adj_factor" not in frame:
            return frame
        factors = pd.to_numeric(frame["adj_factor"], errors="coerce").ffill().bfill()
        if factors.isna().all():
            return frame

        if fq == "pre":
            if pre_factor_ref_date is None:
                days = self.get_trade_days(count=1)
                reference_date = pd.Timestamp(days[-1]) if days else pd.Timestamp(frame.index.max())
            else:
                reference_date = pd.Timestamp(pre_factor_ref_date)
            reference = self._reference_factor(security, asset_type, frequency, reference_date)
            if reference is None:
                reference = float(factors.iloc[-1])
            ratio = factors / reference
        else:
            reference = self._reference_factor(
                security,
                asset_type,
                frequency,
                pd.Timestamp(frame.index.min()),
                from_beginning=True,
            )
            if reference is None:
                reference = float(factors.iloc[0])
            ratio = factors / reference

        adjusted = frame.copy()
        for column in ("open", "high", "low", "close"):
            if column in adjusted:
                adjusted[column] = pd.to_numeric(adjusted[column], errors="coerce") * ratio
        return adjusted

    def _fill_daily_pauses(
        self,
        frame: pd.DataFrame,
        *,
        start: Optional[pd.Timestamp],
        end: Optional[pd.Timestamp],
        calendar: Optional[Sequence[datetime]] = None,
    ) -> pd.DataFrame:
        if frame.empty:
            return frame
        start_candidates = [pd.Timestamp(frame.index.min()).normalize()]
        if start is not None:
            start_candidates.append(start)
        actual_start = max(start_candidates)
        actual_end = (
            end.normalize() if end is not None else pd.Timestamp(frame.index.max()).normalize()
        )
        calendar_index = pd.DatetimeIndex(
            self._backend.read_trade_days(start=actual_start, end=actual_end)
            if calendar is None
            else calendar
        ).normalize()
        calendar_index = calendar_index[
            (calendar_index >= actual_start) & (calendar_index <= actual_end)
        ]
        if calendar_index.empty:
            return frame
        original_index = pd.DatetimeIndex(frame.index).normalize()
        filled = frame.copy()
        filled.index = original_index
        filled = filled[~filled.index.duplicated(keep="last")]
        expanded_index = filled.index.union(calendar_index).sort_values()
        filled = filled.reindex(expanded_index)
        missing_bar = pd.Series(~filled.index.isin(original_index), index=filled.index)
        price_columns = (
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "high_limit",
            "low_limit",
            "adj_factor",
        )
        existing_price_columns = [column for column in price_columns if column in filled]
        if existing_price_columns:
            filled[existing_price_columns] = filled[existing_price_columns].ffill()
        if "is_st" in filled:
            filled["is_st"] = filled["is_st"].ffill().fillna(0)
        for column in ("volume", "money"):
            if column in filled:
                filled.loc[missing_bar, column] = 0.0
                filled[column] = filled[column].fillna(0.0)
        source_paused = pd.Series(False, index=filled.index)
        if "suspend_type" in filled:
            normalized_suspend = (
                filled["suspend_type"].fillna("N").astype(str).str.upper().str.strip()
            )
            source_paused = ~normalized_suspend.isin(("", "N", "R", "0", "FALSE", "NONE", "NAN"))
        filled["paused"] = (missing_bar | source_paused).astype(int)
        filled = filled.reindex(calendar_index)
        if "close" in filled:
            return filled.dropna(subset=["close"])
        return filled

    def _get_price_single(
        self,
        security: str,
        *,
        start_date: DateLike,
        end_date: DateLike,
        frequency: str,
        fields: Sequence[str],
        skip_paused: bool,
        fill_paused: bool,
        fq: Optional[str],
        count: Optional[int],
        pre_factor_ref_date: DateLike,
        raw_override: Optional[pd.DataFrame] = None,
        asset_type_override: Optional[AssetType] = None,
        expected_days: Optional[Sequence[datetime]] = None,
    ) -> pd.DataFrame:
        ts_code = self._to_ts_code(security)
        asset_type = asset_type_override or self._backend.asset_type(ts_code)
        daily = frequency == "1d"
        start = self._timestamp(start_date, normalize=daily)
        end = self._timestamp(end_date, normalize=False)
        if daily and end is not None:
            end = end.normalize()
        if start is None:
            start = self._count_start(frequency, end, count)
        request = BarRequest(
            security=ts_code,
            asset_type=asset_type,
            frequency=frequency,
            start=start,
            end=end,
            columns=self._source_columns(fields, include_factor=fq in ("pre", "post")),
        )
        raw = self._backend.read_bars(request) if raw_override is None else raw_override.copy()
        if daily and fill_paused and start is not None:
            request_days = (
                self._backend.read_trade_days(start=start, end=end)
                if expected_days is None
                else expected_days
            )
            first_expected = pd.Timestamp(request_days[0]) if request_days else None
            first_actual = None
            if not raw.empty and "time" in raw:
                first_actual = pd.Timestamp(pd.to_datetime(raw["time"]).min()).normalize()
            if first_expected is not None and (
                first_actual is None or first_actual > first_expected.normalize()
            ):
                seed_request = BarRequest(
                    security=ts_code,
                    asset_type=asset_type,
                    frequency=frequency,
                    end=first_expected - pd.Timedelta(microseconds=1),
                    columns=request.columns,
                )
                seed = self._backend.read_bars(seed_request).tail(1)
                if not seed.empty:
                    raw = pd.concat((seed, raw), ignore_index=True, sort=False)
        if raw.empty:
            return pd.DataFrame(columns=list(fields))
        frame = raw.rename(
            columns={
                "vol": "volume",
                "amount": "money",
                "up_limit": "high_limit",
                "down_limit": "low_limit",
            }
        )
        frame.index = pd.DatetimeIndex(pd.to_datetime(frame.pop("time")))
        frame.index.name = None
        frame = frame.drop(columns=("code",), errors="ignore")
        if daily:
            for column, multiplier in (("volume", 100.0), ("money", 1000.0)):
                if column in frame:
                    frame[column] = pd.to_numeric(frame[column], errors="coerce") * multiplier

        frame = self._apply_adjustment(
            frame,
            security=ts_code,
            asset_type=asset_type,
            frequency=frequency,
            fq=fq,
            pre_factor_ref_date=pre_factor_ref_date,
        )
        if daily and fill_paused:
            frame = self._fill_daily_pauses(frame, start=start, end=end, calendar=expected_days)
        elif "paused" not in frame:
            if "suspend_type" in frame:
                normalized_suspend = (
                    frame["suspend_type"].fillna("N").astype(str).str.upper().str.strip()
                )
                frame["paused"] = (
                    ~normalized_suspend.isin(("", "N", "R", "0", "FALSE", "NONE", "NAN"))
                ).astype(int)
            else:
                frame["paused"] = 0
        if skip_paused and "paused" in frame:
            frame = frame[frame["paused"] == 0]
        if count:
            frame = frame.tail(count)
        for field in fields:
            if field not in frame:
                frame[field] = 0.0
        return frame[list(fields)]

    def _read_batch_bars(self, request: BatchBarRequest) -> pd.DataFrame:
        arrow_reader = getattr(self._backend, "read_bars_batch_arrow", None)
        if callable(arrow_reader):
            arrow = arrow_reader(request)
            return arrow.to_pandas() if hasattr(arrow, "to_pandas") else pd.DataFrame(arrow)
        return self._backend.read_bars_batch(request)

    def _add_daily_batch_seeds(
        self,
        raw: pd.DataFrame,
        *,
        groups: Dict[AssetType, List[str]],
        frequency: str,
        columns: Tuple[str, ...],
        expected_days: Optional[Sequence[datetime]],
    ) -> pd.DataFrame:
        """Fetch one pre-window row for securities whose first day needs filling."""

        if not expected_days:
            return raw
        first_expected = pd.Timestamp(expected_days[0]).normalize()
        first_by_code: Dict[str, pd.Timestamp] = {}
        if not raw.empty and {"code", "time"}.issubset(raw.columns):
            times = pd.to_datetime(raw["time"], errors="coerce").dt.normalize()
            first_by_code = (
                pd.DataFrame({"code": raw["code"].astype(str), "time": times})
                .dropna(subset=["time"])
                .groupby("code", sort=False)["time"]
                .min()
                .to_dict()
            )
        seeds: List[pd.DataFrame] = []
        for asset_type, codes in groups.items():
            missing = [
                code
                for code in codes
                if code not in first_by_code or first_by_code[code] > first_expected
            ]
            if not missing:
                continue
            seed = self._read_batch_bars(
                BatchBarRequest(
                    securities=tuple(missing),
                    asset_type=asset_type,
                    frequency=frequency,
                    end=first_expected - pd.Timedelta(microseconds=1),
                    columns=columns,
                    count=1,
                )
            )
            if not seed.empty:
                seeds.append(seed)
        if not seeds:
            return raw
        return pd.concat((raw, *seeds), ignore_index=True, sort=False)

    def _apply_batch_adjustment(
        self,
        frame: pd.DataFrame,
        *,
        groups: Dict[AssetType, List[str]],
        frequency: str,
        fq: Optional[str],
        pre_factor_ref_date: DateLike,
    ) -> pd.DataFrame:
        """Apply adjustment ratios to a canonical long frame in vectorized form."""

        if frame.empty or fq not in ("pre", "post") or "adj_factor" not in frame:
            return frame
        adjusted = frame.copy()
        factor = pd.to_numeric(adjusted["adj_factor"], errors="coerce")
        factor = factor.groupby(adjusted["code"], sort=False).ffill()
        factor = factor.groupby(adjusted["code"], sort=False).bfill()
        reference_by_code: Dict[str, float] = {}
        if fq == "pre":
            if pre_factor_ref_date is None:
                days = self.get_trade_days(count=1)
                reference_date = (
                    pd.Timestamp(days[-1]) if days else pd.Timestamp(adjusted["time"].max())
                )
            else:
                reference_date = pd.Timestamp(pre_factor_ref_date)
            for asset_type, codes in groups.items():
                if asset_type == AssetType.INDEX:
                    continue
                refs = self._backend.read_reference_factors_batch(
                    ReferenceFactorRequest(
                        securities=tuple(codes),
                        asset_type=asset_type,
                        frequency=frequency,
                        reference_date=reference_date,
                    )
                )
                if refs.empty or not {"code", "adj_factor"}.issubset(refs.columns):
                    continue
                values = pd.to_numeric(refs["adj_factor"], errors="coerce")
                for code, value in zip(refs["code"].astype(str), values):
                    if not pd.isna(value):
                        reference_by_code[code] = float(value)
            fallback = (
                pd.DataFrame({"code": adjusted["code"], "factor": factor})
                .dropna(subset=["factor"])
                .groupby("code", sort=False)["factor"]
                .last()
            )
        else:
            fallback = (
                pd.DataFrame({"code": adjusted["code"], "factor": factor})
                .dropna(subset=["factor"])
                .groupby("code", sort=False)["factor"]
                .first()
            )
        for code, value in fallback.items():
            reference_by_code.setdefault(str(code), float(value))
        reference = adjusted["code"].map(reference_by_code)
        ratio = factor.div(reference).fillna(1.0)
        index_codes = {code for code in groups.get(AssetType.INDEX, ())}
        if index_codes:
            ratio.loc[adjusted["code"].isin(index_codes)] = 1.0
        price_columns = [
            column for column in ("open", "high", "low", "close") if column in adjusted
        ]
        if price_columns:
            adjusted[price_columns] = (
                adjusted[price_columns].apply(pd.to_numeric, errors="coerce").mul(ratio, axis=0)
            )
        return adjusted

    @staticmethod
    def _fill_daily_pauses_batch(
        frame: pd.DataFrame,
        *,
        codes: Sequence[str],
        start: Optional[pd.Timestamp],
        end: Optional[pd.Timestamp],
        calendar: Optional[Sequence[datetime]],
    ) -> pd.DataFrame:
        """Expand all securities over one shared calendar using grouped operations."""

        if frame.empty:
            return frame
        calendar_index = pd.DatetimeIndex(calendar or ()).normalize().unique().sort_values()
        if calendar_index.empty:
            return frame
        if start is not None:
            calendar_index = calendar_index[calendar_index >= start.normalize()]
        if end is not None:
            calendar_index = calendar_index[calendar_index <= end.normalize()]
        if calendar_index.empty:
            return frame
        prepared = frame.copy()
        prepared["time"] = pd.to_datetime(prepared["time"], errors="coerce").dt.normalize()
        prepared = prepared.dropna(subset=["time"])
        prepared = prepared.sort_values(["code", "time"], kind="stable").drop_duplicates(
            ["code", "time"], keep="last"
        )
        first_actual = prepared.groupby("code", sort=False)["time"].min()
        if start is not None:
            first_actual = first_actual.clip(lower=start.normalize())
        expanded_times = (
            calendar_index.append(pd.DatetimeIndex(prepared["time"])).unique().sort_values()
        )
        grid = pd.MultiIndex.from_product([tuple(codes), expanded_times], names=["code", "time"])
        prepared["_source_bar"] = True
        filled = prepared.set_index(["code", "time"]).reindex(grid)
        missing_bar = ~filled["_source_bar"].eq(True)
        price_columns = [
            column
            for column in (
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "high_limit",
                "low_limit",
                "adj_factor",
            )
            if column in filled
        ]
        if price_columns:
            filled[price_columns] = filled.groupby(level="code", sort=False)[price_columns].ffill()
        if "is_st" in filled:
            filled["is_st"] = filled.groupby(level="code", sort=False)["is_st"].ffill().fillna(0)
        for column in ("volume", "money"):
            if column in filled:
                filled.loc[missing_bar, column] = 0.0
                filled[column] = filled[column].fillna(0.0)
        source_paused = pd.Series(False, index=filled.index)
        if "suspend_type" in filled:
            normalized = filled["suspend_type"].fillna("N").astype(str).str.upper().str.strip()
            source_paused = ~normalized.isin(("", "N", "R", "0", "FALSE", "NONE", "NAN"))
        filled["paused"] = (missing_bar | source_paused).astype(int)
        filled = filled[filled.index.get_level_values("time").isin(calendar_index)].reset_index()
        actual_start = filled["code"].map(first_actual)
        filled = filled[actual_start.notna() & (filled["time"] >= actual_start)]
        if "close" in filled:
            filled = filled.dropna(subset=["close"])
        return filled.drop(columns=("_source_bar",), errors="ignore")

    def _get_price_batch(
        self,
        securities: Sequence[str],
        *,
        ts_codes: Dict[str, str],
        asset_types: Dict[str, AssetType],
        frequency: str,
        fields: Sequence[str],
        start: Optional[pd.Timestamp],
        end: Optional[pd.Timestamp],
        expected_days: Optional[Sequence[datetime]],
        skip_paused: bool,
        fill_paused: bool,
        fq: Optional[str],
        count: Optional[int],
        pre_factor_ref_date: DateLike,
        panel: bool,
    ) -> pd.DataFrame:
        groups: Dict[AssetType, List[str]] = {}
        for jq_code in securities:
            groups.setdefault(asset_types[jq_code], []).append(ts_codes[jq_code])
        source_columns = self._source_columns(fields, include_factor=fq in ("pre", "post"))
        raw_frames: List[pd.DataFrame] = []
        for asset_type, codes in groups.items():
            raw_frames.append(
                self._read_batch_bars(
                    BatchBarRequest(
                        securities=tuple(codes),
                        asset_type=asset_type,
                        frequency=frequency,
                        start=start,
                        end=end,
                        columns=source_columns,
                    )
                )
            )
        raw = pd.concat(raw_frames, ignore_index=True, sort=False) if raw_frames else pd.DataFrame()
        daily = frequency == "1d"
        if daily and fill_paused and start is not None:
            raw = self._add_daily_batch_seeds(
                raw,
                groups=groups,
                frequency=frequency,
                columns=source_columns,
                expected_days=expected_days,
            )
        if raw.empty:
            if panel:
                columns = pd.MultiIndex.from_product((securities, fields))
                return pd.DataFrame(columns=columns)
            return pd.DataFrame(columns=("time", "code") + tuple(fields))
        frame = raw.rename(
            columns={
                "vol": "volume",
                "amount": "money",
                "up_limit": "high_limit",
                "down_limit": "low_limit",
            }
        )
        frame["time"] = pd.to_datetime(frame["time"], errors="coerce")
        frame["code"] = frame["code"].astype(str)
        frame = frame.dropna(subset=["time"]).sort_values(["code", "time"], kind="stable")
        if daily:
            for column, multiplier in (("volume", 100.0), ("money", 1000.0)):
                if column in frame:
                    frame[column] = pd.to_numeric(frame[column], errors="coerce") * multiplier
        frame = self._apply_batch_adjustment(
            frame,
            groups=groups,
            frequency=frequency,
            fq=fq,
            pre_factor_ref_date=pre_factor_ref_date,
        )
        ordered_codes = [ts_codes[code] for code in securities]
        if daily and fill_paused:
            frame = self._fill_daily_pauses_batch(
                frame,
                codes=ordered_codes,
                start=start,
                end=end,
                calendar=expected_days,
            )
        elif "paused" not in frame:
            if "suspend_type" in frame:
                normalized = frame["suspend_type"].fillna("N").astype(str).str.upper().str.strip()
                frame["paused"] = (
                    ~normalized.isin(("", "N", "R", "0", "FALSE", "NONE", "NAN"))
                ).astype(int)
            else:
                frame["paused"] = 0
        if skip_paused and "paused" in frame:
            frame = frame[frame["paused"] == 0]
        if count:
            frame = frame.groupby("code", sort=False, group_keys=False).tail(int(count))
        for field in fields:
            if field not in frame:
                frame[field] = 0.0
        output_codes = {ts_codes[jq_code]: jq_code for jq_code in securities}
        result = frame[["time", "code"] + list(fields)].copy()
        result["code"] = result["code"].map(output_codes)
        result = result.dropna(subset=["code"])
        code_order = {code: position for position, code in enumerate(securities)}
        result["_code_order"] = result["code"].map(code_order)
        result = result.sort_values(["_code_order", "time"], kind="stable").reset_index(drop=True)
        result = result.sort_values(["time", "code"], kind="stable").drop(columns=["_code_order"])
        if not panel:
            return result
        wide = result.set_index(["time", "code"])[list(fields)].unstack("code")
        wide = wide.swaplevel(0, 1, axis=1)
        ordered_columns = pd.MultiIndex.from_product((securities, fields))
        wide = wide.reindex(columns=ordered_columns)
        wide.columns.names = [None, None]
        wide.index.name = None
        if daily and fill_paused and len(wide.index) >= 3:
            inferred_frequency = pd.infer_freq(wide.index)
            if inferred_frequency is not None:
                wide.index = pd.DatetimeIndex(wide.index, freq=inferred_frequency)
        return wide

    def get_price(
        self,
        security: Union[str, List[str]],
        start_date: DateLike = None,
        end_date: DateLike = None,
        frequency: str = "daily",
        fields: Optional[List[str]] = None,
        skip_paused: bool = False,
        fq: Optional[str] = "pre",
        count: Optional[int] = None,
        panel: bool = True,
        fill_paused: bool = True,
        pre_factor_ref_date: DateLike = None,
        prefer_engine: bool = False,
        force_no_engine: bool = False,
    ) -> pd.DataFrame:
        _ = prefer_engine, force_no_engine
        self._ensure_ready()
        if count is not None and count <= 0:
            raise ValueError("count 必须为正整数")
        normalized_frequency = self._normalize_frequency(frequency)
        requested_fields: Sequence[str] = tuple(fields or self._DEFAULT_FIELDS)
        sequence_input = isinstance(security, (list, tuple, set))
        securities = list(security) if sequence_input else [security]
        securities = [str(item) for item in securities]
        if not securities:
            return pd.DataFrame(columns=list(requested_fields))
        daily = normalized_frequency == "1d"
        shared_start = self._timestamp(start_date, normalize=daily)
        shared_end = self._timestamp(end_date, normalize=False)
        if daily and shared_end is not None:
            shared_end = shared_end.normalize()
        if shared_start is None:
            shared_start = self._count_start(normalized_frequency, shared_end, count)
        shared_days: Optional[Sequence[datetime]] = None
        if daily and fill_paused and shared_start is not None:
            shared_days = self._backend.read_trade_days(start=shared_start, end=shared_end)
        asset_types: Dict[str, AssetType] = {}
        ts_codes: Dict[str, str] = {}
        for jq_code in securities:
            ts_code = self._to_ts_code(jq_code)
            ts_codes[jq_code] = ts_code
            asset_types[jq_code] = self._backend.asset_type(ts_code)
        if self._query_mode == "batch" and len(securities) > 1:
            return self._get_price_batch(
                securities,
                ts_codes=ts_codes,
                asset_types=asset_types,
                frequency=normalized_frequency,
                fields=requested_fields,
                start=shared_start,
                end=shared_end,
                expected_days=shared_days,
                skip_paused=skip_paused,
                fill_paused=fill_paused,
                fq=fq,
                count=count,
                pre_factor_ref_date=pre_factor_ref_date,
                panel=panel,
            )
        frames: Dict[str, pd.DataFrame] = {}
        for item in securities:
            frames[item] = self._get_price_single(
                item,
                start_date=start_date,
                end_date=end_date,
                frequency=normalized_frequency,
                fields=requested_fields,
                skip_paused=skip_paused,
                fill_paused=fill_paused,
                fq=fq,
                count=count,
                pre_factor_ref_date=pre_factor_ref_date,
                asset_type_override=asset_types[item],
                expected_days=shared_days,
            )
        if len(frames) == 1 and not sequence_input:
            return next(iter(frames.values()))
        if panel:
            return pd.concat(frames, axis=1)
        rows: List[pd.DataFrame] = []
        for code, frame in frames.items():
            current = frame.copy().reset_index().rename(columns={"index": "time"})
            current.insert(1, "code", code)
            rows.append(current)
        if not rows:
            return pd.DataFrame(columns=("time", "code") + tuple(requested_fields))
        return pd.concat(rows, ignore_index=True).sort_values(["time", "code"])

    def get_trade_days(
        self,
        start_date: DateLike = None,
        end_date: DateLike = None,
        count: Optional[int] = None,
    ) -> List[datetime]:
        self._ensure_ready()
        start = self._timestamp(start_date, normalize=True)
        end = self._timestamp(end_date, normalize=True)
        days = self._backend.read_trade_days(start=start, end=end)
        if count is not None and count != -1:
            if count <= 0:
                raise ValueError("count 必须为正整数或 -1")
            days = days[-count:]
        return cast(List[datetime], days)

    def get_trade_day(
        self,
        security: Union[str, List[str]],
        query_dt: Union[str, datetime],
    ) -> Dict[str, Optional[Date]]:
        days = self.get_trade_days(end_date=query_dt, count=1)
        last_day = pd.Timestamp(days[-1]).date() if days else None
        securities = list(security) if isinstance(security, (list, tuple, set)) else [security]
        return {str(item): last_day for item in securities}

    @staticmethod
    def _asset_types(types: Union[str, List[str]]) -> Tuple[AssetType, ...]:
        values = [types] if isinstance(types, str) else list(types)
        result: List[AssetType] = []
        for value in values:
            normalized = str(value).lower()
            if normalized == "stock":
                asset = AssetType.STOCK
            elif normalized in ("fund", "etf", "lof"):
                asset = AssetType.FUND
            elif normalized == "index":
                asset = AssetType.INDEX
            else:
                continue
            if asset not in result:
                result.append(asset)
        return tuple(result)

    def get_all_securities(
        self,
        types: Union[str, List[str]] = "stock",
        date: DateLike = None,
    ) -> pd.DataFrame:
        self._ensure_ready()
        frame = self._backend.read_securities(
            self._asset_types(types), self._timestamp(date, normalize=True)
        )
        frame.index = pd.Index([self._to_jq_code(str(code)) for code in frame.index])
        return frame

    def get_security_info(self, security: str, date: DateLike = None) -> Dict[str, Any]:
        ts_code = self._to_ts_code(security)
        asset = self._backend.asset_type(ts_code)
        frame = self._backend.read_securities((asset,), self._timestamp(date, normalize=True))
        if ts_code not in frame.index:
            return {}
        result = dict(frame.loc[ts_code].to_dict())
        result["code"] = self._to_jq_code(ts_code)
        return result

    def get_index_stocks(self, index_symbol: str, date: DateLike = None) -> List[str]:
        self._ensure_ready()
        frame = self._backend.read_index_components(
            self._to_ts_code(index_symbol), self._timestamp(date, normalize=True)
        )
        return [self._to_jq_code(str(code)) for code in frame.get("code", [])]

    def get_index_weights(self, index_id: str, date: DateLike = None) -> pd.DataFrame:
        self._ensure_ready()
        frame = self._backend.read_index_components(
            self._to_ts_code(index_id), self._timestamp(date, normalize=True)
        )
        if frame.empty:
            return pd.DataFrame(columns=("weight",))
        frame = frame.copy()
        frame["code"] = frame["code"].map(self._to_jq_code)
        return frame.set_index("code")[["weight"]]

    def get_split_dividend(
        self,
        security: str,
        start_date: DateLike = None,
        end_date: DateLike = None,
    ) -> List[Dict[str, Any]]:
        self._ensure_ready()
        frame = self._backend.read_corporate_actions(
            self._to_ts_code(security),
            self._timestamp(start_date, normalize=True),
            self._timestamp(end_date, normalize=True),
        )
        if frame.empty:
            return []
        return self._normalize_corporate_action_frame(frame)

    def _normalize_corporate_action_frame(self, frame: pd.DataFrame) -> List[Dict[str, Any]]:
        """Normalize canonical backend rows to the stable public event shape."""

        events: List[Dict[str, Any]] = []
        for row in frame.itertuples(index=False):
            events.append(
                {
                    "security": self._to_jq_code(str(row.security)),
                    "date": pd.Timestamp(row.event_date).date(),
                    "security_type": str(row.security_type),
                    "scale_factor": float(row.scale_factor),
                    "bonus_pre_tax": float(row.bonus_pre_tax),
                    "per_base": float(row.per_base),
                }
            )
        return events

    def get_split_dividend_batch(
        self,
        securities: Sequence[str],
        start_date: DateLike = None,
        end_date: DateLike = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Internal engine hook; public single-security signature stays unchanged."""

        self._ensure_ready()
        requested = [str(item) for item in securities]
        result: Dict[str, List[Dict[str, Any]]] = {security: [] for security in requested}
        if not requested:
            return result
        ts_to_jq = {self._to_ts_code(security): security for security in requested}
        start = self._timestamp(start_date, normalize=True)
        end = self._timestamp(end_date, normalize=True)
        session = None
        cache_key = None
        frame = None
        try:
            from ..backtest_session import get_current_backtest_data_session

            session = get_current_backtest_data_session()
            diagnostics = self._backend.diagnostics()
            cache_key = (
                "corporate_actions",
                diagnostics.get("manifest_identity") or diagnostics.get("root"),
                tuple(ts_to_jq),
                None if start is None else start.isoformat(),
                None if end is None else end.isoformat(),
            )
            frame = session.get_price_block(cache_key) if session is not None else None
        except Exception:
            session = None
            cache_key = None
        if frame is None:
            frame = self._backend.read_corporate_actions_batch(
                CorporateActionRequest(
                    securities=tuple(ts_to_jq),
                    start=start,
                    end=end,
                )
            )
            if session is not None and cache_key is not None:
                session.set_price_block(cache_key, frame, rows=len(frame))
        if frame.empty:
            return result
        for ts_code, group in frame.groupby("security", sort=False):
            jq_code = ts_to_jq.get(str(ts_code), self._to_jq_code(str(ts_code)))
            result[jq_code] = self._normalize_corporate_action_frame(group)
        return result

    @staticmethod
    def _parse_stat_date(value: Optional[str]) -> Optional[pd.Timestamp]:
        if value is None:
            return None
        normalized = str(value).strip().lower()
        if len(normalized) == 6 and normalized[4] == "q" and normalized[-1] in "1234":
            endings = {"1": "0331", "2": "0630", "3": "0930", "4": "1231"}
            normalized = normalized[:4] + endings[normalized[-1]]
        elif len(normalized) == 4 and normalized.isdigit():
            normalized += "1231"
        try:
            return pd.Timestamp(normalized).normalize()
        except (TypeError, ValueError) as exc:
            raise ValueError("无法解析 statDate：{!r}".format(value)) from exc

    def get_fundamentals(
        self,
        query_object: Any,
        date: DateLike = None,
        statDate: Optional[str] = None,
    ) -> pd.DataFrame:
        self._ensure_ready()
        as_of = self._timestamp(date, normalize=True)
        if as_of is None:
            as_of = pd.Timestamp.today().normalize()
        stat_date = self._parse_stat_date(statDate)
        session = None
        cache_key = None
        try:
            from ..backtest_session import get_current_backtest_data_session

            session = get_current_backtest_data_session()
            diagnostics = self._backend.diagnostics()
            cache_key = (
                "fundamentals",
                diagnostics.get("manifest_identity") or diagnostics.get("root"),
                repr(query_object),
                as_of.isoformat(),
                None if stat_date is None else stat_date.isoformat(),
            )
            cached = session.get_price_block(cache_key) if session is not None else None
            if cached is not None:
                return cached.copy()
        except Exception:
            session = None
            cache_key = None
        result = self._fundamentals.execute(query_object, as_of=as_of, stat_date=stat_date)
        if session is not None and cache_key is not None:
            session.set_price_block(cache_key, result, rows=len(result))
        return result

    def get_fundamentals_continuously(
        self,
        query_object: Any,
        end_date: DateLike = None,
        count: int = 1,
        panel: bool = True,
    ) -> pd.DataFrame:
        _ = panel
        days = self.get_trade_days(end_date=end_date, count=count)
        frames: List[pd.DataFrame] = []
        for day in days:
            frame = self.get_fundamentals(query_object, date=day)
            if not frame.empty:
                frame = frame.copy()
                frame["day"] = pd.Timestamp(day).date()
                frames.append(frame)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    @staticmethod
    def _raise_unsupported_live_operation(operation: str) -> NoReturn:
        raise LocalDataUnsupportedOperationError(
            "LocalDataProvider 不支持 {}；本地 Provider 只提供可复现的离线历史数据".format(
                operation
            )
        )

    def get_ticks(
        self,
        security: str,
        end_dt: Union[str, datetime],
        start_dt: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None,
        fields: Optional[List[str]] = None,
        skip: bool = False,
        df: bool = False,
    ) -> Any:
        _ = security, end_dt, start_dt, count, fields, skip, df
        self._raise_unsupported_live_operation("get_ticks")

    def get_current_tick(
        self,
        security: str,
        dt: Optional[Union[str, datetime]] = None,
        df: bool = False,
    ) -> Optional[Any]:
        _ = security, dt, df
        self._raise_unsupported_live_operation("get_current_tick")

    def subscribe_ticks(self, symbols: List[str]) -> None:
        _ = symbols
        self._raise_unsupported_live_operation("subscribe_ticks")

    def subscribe_markets(self, markets: List[str]) -> None:
        _ = markets
        self._raise_unsupported_live_operation("subscribe_markets")

    def unsubscribe_ticks(self, symbols: Optional[List[str]] = None) -> None:
        _ = symbols
        self._raise_unsupported_live_operation("unsubscribe_ticks")

    def unsubscribe_markets(self, markets: Optional[List[str]] = None) -> None:
        _ = markets
        self._raise_unsupported_live_operation("unsubscribe_markets")

    def get_extras(
        self,
        info: str,
        security_list: List[str],
        start_date: DateLike = None,
        end_date: DateLike = None,
        df: bool = True,
        count: Optional[int] = None,
    ) -> Any:
        if info != "is_st":
            raise LocalDataUnsupportedOperationError(
                "LocalDataProvider.get_extras 当前仅支持 'is_st'，请求值：{!r}".format(info)
            )
        values: Dict[str, pd.Series] = {}
        for security in security_list:
            result = self.get_price(
                security,
                start_date=start_date,
                end_date=end_date,
                fields=["is_st"],
                fq=None,
                count=count,
                fill_paused=True,
            )
            values[security] = result.get("is_st", pd.Series(dtype=float)).astype(bool)
        wide = pd.DataFrame(values)
        return wide if df else {column: wide[column].tolist() for column in wide}

    def get_bars(
        self,
        security: Union[str, List[str]],
        count: int,
        unit: str = "1d",
        fields: Optional[List[str]] = None,
        include_now: bool = False,
        end_dt: DateLike = None,
        fq_ref_date: Union[int, datetime] = 1,
        df: bool = False,
    ) -> Any:
        _ = include_now
        pre_ref: DateLike = end_dt if fq_ref_date == 1 else None
        if isinstance(fq_ref_date, (datetime, Date)):
            pre_ref = fq_ref_date
        frame = self.get_price(
            security,
            end_date=end_dt,
            frequency=unit,
            fields=fields,
            count=count,
            panel=False if isinstance(security, (list, tuple)) else True,
            pre_factor_ref_date=pre_ref,
        )
        if df:
            return frame
        return frame.to_records(index=True)

    def preflight_backtest(
        self,
        *,
        start_date: DateLike,
        end_date: DateLike,
        frequency: str,
        securities: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        """Validate local runtime requirements before the first simulated bar."""

        self._ensure_ready()
        start = self._timestamp(start_date, normalize=True)
        end = self._timestamp(end_date, normalize=True)
        if start is None or end is None or end < start:
            raise LocalDataConfigurationError("本地回测预检需要有效且有序的 start_date/end_date")
        trade_days = self._backend.read_trade_days(start=start, end=end)
        if not trade_days:
            raise LocalDataConfigurationError(
                "本地数据在 {}..{} 没有交易日；请检查回测区间和数据覆盖".format(
                    start.date(), end.date()
                )
            )
        first_day = pd.Timestamp(trade_days[0]).normalize()
        last_day = pd.Timestamp(trade_days[-1]).normalize()
        normalized_frequency = self._normalize_frequency(frequency)
        requested = [str(item) for item in (securities or ())]
        resolved_assets: Dict[str, AssetType] = {}
        for security in requested:
            ts_code = self._to_ts_code(security)
            try:
                resolved_assets[ts_code] = self._backend.asset_type(ts_code)
            except Exception as exc:
                raise LocalDataConfigurationError(
                    "本地回测预检无法识别证券 {!r}；请补齐对应 basic 数据或修正代码后缀".format(
                        security
                    )
                ) from exc

        # When no universe is declared, retain the historical stock probe. If
        # securities are supplied, requirements must reflect their real assets.
        preflight_assets = set(resolved_assets.values()) or {AssetType.STOCK}
        required_datasets = set()
        bar_datasets = set()
        for asset_type in preflight_assets:
            basic_dataset = "{}_basic".format(asset_type.value)
            bar_dataset = (
                "{}_daily".format(asset_type.value)
                if normalized_frequency == "1d"
                else "{}_{}".format(asset_type.value, normalized_frequency)
            )
            required_datasets.update((basic_dataset, bar_dataset))
            bar_datasets.add(bar_dataset)

        require_actions = self._as_bool(self.config.get("require_corporate_actions"), True)
        require_actions = require_actions and any(
            asset_type in (AssetType.STOCK, AssetType.FUND) for asset_type in preflight_assets
        )
        if require_actions:
            required_datasets.add("corporate_actions")
        if self._as_bool(self.config.get("require_fundamentals"), False):
            required_datasets.update(("income", "balance", "cash_flow", "indicator"))

        manifest = getattr(self._backend, "manifest", None)
        security_coverage: Dict[str, Dict[str, str]] = {}
        if manifest is not None:
            for dataset in sorted(required_datasets):
                manifest.dataset_shards(dataset) or manifest.dataset_shard(dataset)
            for dataset in sorted(bar_datasets):
                shards = manifest.select_dataset_shards(dataset, start=first_day, end=last_day)
                minima = [
                    pd.Timestamp(shard.coverage[dataset]["min"]).normalize()
                    for shard in shards
                    if shard.coverage.get(dataset, {}).get("min")
                ]
                maxima = [
                    pd.Timestamp(shard.coverage[dataset]["max"]).normalize()
                    for shard in shards
                    if shard.coverage.get(dataset, {}).get("max")
                ]
                if not minima or not maxima or min(minima) > first_day or max(maxima) < last_day:
                    raise LocalDataConfigurationError(
                        "DuckDB 数据集 {!r} 覆盖不足：需要 {}..{}；请重建相应时间分片".format(
                            dataset, first_day.date(), last_day.date()
                        )
                    )
        else:
            for ts_code, asset_type in resolved_assets.items():
                catalog = self._backend.read_securities((asset_type,))
                if ts_code not in catalog.index:
                    raise LocalDataConfigurationError(
                        "本地回测预检在 {}_basic 中找不到证券 {}".format(
                            asset_type.value, self._to_jq_code(ts_code)
                        )
                    )
                metadata = catalog.loc[ts_code]
                listed_at = pd.to_datetime(metadata.get("start_date"), errors="coerce")
                delisted_at = pd.to_datetime(metadata.get("end_date"), errors="coerce")
                expected_start = (
                    max(first_day, listed_at.normalize()) if pd.notna(listed_at) else first_day
                )
                expected_end = (
                    min(last_day, delisted_at.normalize()) if pd.notna(delisted_at) else last_day
                )
                if expected_start > expected_end:
                    raise LocalDataConfigurationError(
                        "证券 {} 在回测区间 {}..{} 内未上市或已退市".format(
                            self._to_jq_code(ts_code), first_day.date(), last_day.date()
                        )
                    )
                try:
                    bars = self._backend.read_bars(
                        BarRequest(
                            security=ts_code,
                            asset_type=asset_type,
                            frequency=normalized_frequency,
                            start=expected_start,
                            end=expected_end + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1),
                            columns=("close",),
                        )
                    )
                except Exception as exc:
                    dataset = (
                        "{}_daily".format(asset_type.value)
                        if normalized_frequency == "1d"
                        else "{}_{}".format(asset_type.value, normalized_frequency)
                    )
                    raise LocalDataConfigurationError(
                        "本地回测预检无法读取数据集 {} 中证券 {} 的行情".format(
                            dataset, self._to_jq_code(ts_code)
                        )
                    ) from exc
                if bars.empty or "time" not in bars:
                    dataset = (
                        "{}_daily".format(asset_type.value)
                        if normalized_frequency == "1d"
                        else "{}_{}".format(asset_type.value, normalized_frequency)
                    )
                    raise LocalDataConfigurationError(
                        "本地数据集 {} 在 {}..{} 内没有证券 {} 的行情".format(
                            dataset,
                            expected_start.date(),
                            expected_end.date(),
                            self._to_jq_code(ts_code),
                        )
                    )
                bar_times = pd.to_datetime(bars["time"], errors="coerce").dropna()
                security_coverage[self._to_jq_code(ts_code)] = {
                    "asset_type": asset_type.value,
                    "min": str(bar_times.min()),
                    "max": str(bar_times.max()),
                }

        if require_actions and manifest is None:
            probe_security = next(
                (
                    code
                    for code, asset_type in resolved_assets.items()
                    if asset_type in (AssetType.STOCK, AssetType.FUND)
                ),
                None,
            )
            if probe_security is None:
                catalog = self._backend.read_securities((AssetType.STOCK,), first_day)
            else:
                catalog = None
            if catalog is not None and catalog.empty:
                raise LocalDataConfigurationError("本地回测预检无法找到股票基础信息")
            if catalog is not None:
                probe_security = str(catalog.index[0])
            assert probe_security is not None
            try:
                self._backend.read_corporate_actions(probe_security, first_day, last_day)
            except Exception as exc:
                raise LocalDataConfigurationError(
                    "本地回测需要 corporate_actions；请先运行 "
                    "`bullet-trade data import-corporate-actions --root ./data/parquet`"
                ) from exc
        return {
            "provider": self.name,
            "backend": self._backend.name,
            "frequency": normalized_frequency,
            "first_trade_day": str(first_day.date()),
            "last_trade_day": str(last_day.date()),
            "required_datasets": sorted(required_datasets),
            "security_coverage": security_coverage,
            "manifest_identity": self._backend.diagnostics().get("manifest_identity"),
        }

    def capabilities(self) -> Dict[str, Dict[str, Any]]:
        """Return the stable Local/JQData compatibility classification."""

        corporate_mode = self._backend.batch_capabilities().get("corporate_actions", "fallback")
        return {
            "get_price": {
                "status": "supported",
                "assets": ["stock", "fund", "index"],
                "frequencies": ["1d", "1m", "5m", "15m", "30m", "60m"],
                "batch": self._backend.batch_capabilities().get("bars", "fallback"),
            },
            "get_trade_days": {"status": "supported"},
            "get_all_securities": {"status": "supported"},
            "get_security_info": {
                "status": "supported",
                "batch": self._backend.batch_capabilities().get("security_info", "fallback"),
            },
            "get_index_stocks": {"status": "supported"},
            "get_index_weights": {"status": "supported"},
            "get_fundamentals": {
                "status": "limited",
                "limitations": "valuation 与已注册的 Tushare 财务表字段子集",
            },
            "get_extras": {
                "status": "limited",
                "limitations": "仅支持 is_st",
            },
            "get_bars": {"status": "supported"},
            "get_split_dividend": {
                "status": "limited",
                "limitations": "需要显式准备 corporate_actions 数据集",
                "batch": corporate_mode,
            },
            "tick/live/subscription": {
                "status": "unsupported",
                "limitations": "本地 Provider 只提供离线历史数据",
            },
        }

    def diagnostics(self) -> Dict[str, Any]:
        self._ensure_ready()
        result = dict(self._backend.diagnostics())
        result["provider"] = self.name
        result["query_mode"] = self._query_mode
        result["capabilities"] = self.capabilities()
        return result
