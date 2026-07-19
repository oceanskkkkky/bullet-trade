"""Stable contracts shared by local data backends.

The provider-facing API deliberately depends on these canonical models instead
of a storage engine.  A DuckDB backend can therefore be added without changing
strategy code or :class:`LocalDataProvider`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd


class LocalDataError(RuntimeError):
    """Base exception for deterministic local-data failures."""


class LocalDataConfigurationError(LocalDataError):
    """The local data root or backend configuration is invalid."""


class LocalDataSchemaError(LocalDataError):
    """A local dataset is missing required columns or files."""


class LocalDataQueryError(LocalDataError):
    """A query cannot be represented by the local backend."""


class LocalDataCapabilityError(LocalDataQueryError):
    """A requested operation is not implemented by the selected backend."""


class LocalDataMissingShardError(LocalDataSchemaError):
    """A materialized generation lacks a shard required by the query."""


class LocalDataIncompatibleGenerationError(LocalDataSchemaError):
    """A manifest generation is incompatible with the running code."""


class LocalDataUnsupportedOperationError(LocalDataCapabilityError):
    """A provider operation is deliberately unsupported by local data."""


class AssetType(str, Enum):
    """Canonical asset types supported by local backends."""

    STOCK = "stock"
    FUND = "fund"
    INDEX = "index"


@dataclass(frozen=True)
class BarRequest:
    """Storage-neutral request for one security's bars.

    Security codes use Tushare suffixes (``.SH``/``.SZ``) inside backends.
    Backends return a canonical frame containing ``time`` and ``code`` plus
    requested source columns. Unit and JoinQuant-code normalization belongs to
    the provider facade.
    """

    security: str
    asset_type: AssetType
    frequency: str
    start: Optional[pd.Timestamp] = None
    end: Optional[pd.Timestamp] = None
    columns: Tuple[str, ...] = ()


@dataclass(frozen=True)
class BatchBarRequest:
    """Homogeneous multi-security bar request.

    ``count`` is applied independently to every security.  Backends return a
    canonical long frame ordered by ``time, code``.
    """

    securities: Tuple[str, ...]
    asset_type: AssetType
    frequency: str
    start: Optional[pd.Timestamp] = None
    end: Optional[pd.Timestamp] = None
    columns: Tuple[str, ...] = ()
    count: Optional[int] = None


@dataclass(frozen=True)
class CurrentBarRequest:
    """Homogeneous current-snapshot request at or before ``at``."""

    securities: Tuple[str, ...]
    asset_type: AssetType
    frequency: str
    at: pd.Timestamp
    columns: Tuple[str, ...] = ()


@dataclass(frozen=True)
class ReferenceFactorRequest:
    """Reference adjustment-factor request for multiple securities."""

    securities: Tuple[str, ...]
    asset_type: AssetType
    frequency: str
    reference_date: pd.Timestamp


@dataclass(frozen=True)
class SecurityInfoRequest:
    """Point-in-time security-information request."""

    securities: Tuple[str, ...]
    date: Optional[pd.Timestamp] = None


@dataclass(frozen=True)
class CorporateActionRequest:
    """Canonical effective corporate-action request."""

    securities: Tuple[str, ...]
    start: Optional[pd.Timestamp] = None
    end: Optional[pd.Timestamp] = None
    include_non_effective: bool = False


@dataclass(frozen=True)
class BatchQueryResult:
    """Immutable metadata accompanying a canonical batch frame.

    Backends still expose pandas frames for compatibility; this result model is
    used by diagnostics and future Arrow transport without changing requests.
    """

    operation: str
    mode: str
    query_count: int
    row_count: int
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class _MutableQueryStats:
    """Internal cumulative query counters; never exposed directly."""

    query_count: int = 0
    rows_read: int = 0
    elapsed_seconds: float = 0.0
    max_elapsed_seconds: float = 0.0
    peak_result_bytes: int = 0
    cold_queries: int = 0
    warm_queries: int = 0
    signatures: Dict[str, int] = field(default_factory=dict)


class LocalDataBackend(ABC):
    """Storage-neutral local-data backend.

    Methods return canonical pandas objects. Implementations must never fall
    back to a network data source: reproducibility is part of this contract.
    """

    name: str = "base"

    def _query_stats_state(self) -> _MutableQueryStats:
        state = getattr(self, "_local_query_stats", None)
        if state is None:
            state = _MutableQueryStats()
            setattr(self, "_local_query_stats", state)
        return state

    def _record_query(
        self,
        operation: str,
        *,
        rows: int,
        elapsed_seconds: float,
        result_bytes: int = 0,
        signature: Optional[str] = None,
    ) -> None:
        """Record one storage query without changing public APIs."""

        state = self._query_stats_state()
        state.query_count += 1
        state.rows_read += max(int(rows), 0)
        state.elapsed_seconds += max(float(elapsed_seconds), 0.0)
        state.max_elapsed_seconds = max(state.max_elapsed_seconds, float(elapsed_seconds))
        state.peak_result_bytes = max(state.peak_result_bytes, max(int(result_bytes), 0))
        key = signature or operation
        seen = state.signatures.get(key, 0)
        if seen:
            state.warm_queries += 1
        else:
            state.cold_queries += 1
        state.signatures[key] = seen + 1

    def query_statistics(self) -> Dict[str, Any]:
        """Return a detached, non-sensitive statistics snapshot."""

        state = self._query_stats_state()
        return {
            "query_count": state.query_count,
            "rows_read": state.rows_read,
            "elapsed_seconds": state.elapsed_seconds,
            "max_elapsed_seconds": state.max_elapsed_seconds,
            "peak_result_bytes": state.peak_result_bytes,
            "cold_queries": state.cold_queries,
            "warm_queries": state.warm_queries,
        }

    def reset_query_statistics(self) -> None:
        """Reset counters for an isolated benchmark or contract test."""

        setattr(self, "_local_query_stats", _MutableQueryStats())

    @abstractmethod
    def validate(self) -> None:
        """Validate required datasets and dependencies, without full scans."""

    @abstractmethod
    def asset_type(self, security: str) -> AssetType:
        """Resolve a Tushare-style security code to its local asset type."""

    @abstractmethod
    def read_bars(self, request: BarRequest) -> pd.DataFrame:
        """Read raw bars in ascending time order."""

    @abstractmethod
    def read_trade_days(
        self,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
    ) -> List[datetime]:
        """Read the local exchange calendar."""

    @abstractmethod
    def read_securities(
        self,
        asset_types: Sequence[AssetType],
        date: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        """Read canonical security metadata indexed by Tushare code."""

    @abstractmethod
    def read_index_components(
        self,
        index_code: str,
        date: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        """Return columns ``code`` and ``weight`` for an index snapshot."""

    @abstractmethod
    def read_financial_table(
        self,
        table: str,
        columns: Sequence[str],
        *,
        as_of: pd.Timestamp,
        stat_date: Optional[pd.Timestamp] = None,
        securities: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        """Read a point-in-time financial or valuation table."""

    def read_bars_batch(self, request: BatchBarRequest) -> pd.DataFrame:
        """Read homogeneous bars, falling back to correct scalar calls."""

        frames: List[pd.DataFrame] = []
        for security in request.securities:
            frame = self.read_bars(
                BarRequest(
                    security=security,
                    asset_type=request.asset_type,
                    frequency=request.frequency,
                    start=request.start,
                    end=request.end,
                    columns=request.columns,
                )
            )
            if request.count is not None and not frame.empty:
                frame = frame.tail(int(request.count))
            frames.append(frame)
        if not frames:
            return pd.DataFrame(columns=["time", "code"] + list(request.columns))
        result = pd.concat(frames, ignore_index=True, sort=False)
        if result.empty:
            return pd.DataFrame(columns=["time", "code"] + list(request.columns))
        return result.sort_values(["time", "code"], kind="stable").reset_index(drop=True)

    def read_current_bars(self, request: CurrentBarRequest) -> pd.DataFrame:
        """Return the latest bar per security using the batch bar contract."""

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
        """Return latest adjustment factors at the reference timestamp."""

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

    def read_security_info_batch(self, request: SecurityInfoRequest) -> pd.DataFrame:
        """Return catalog rows through a correct scalar-compatible fallback."""

        if not request.securities:
            return pd.DataFrame()
        assets: List[AssetType] = []
        for security in request.securities:
            asset = self.asset_type(security)
            if asset not in assets:
                assets.append(asset)
        frame = self.read_securities(tuple(assets), request.date)
        wanted = set(request.securities)
        return frame.loc[frame.index.intersection(wanted)].copy()

    def read_corporate_actions(
        self,
        security: str,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        """Read effective events for one security.

        Kept non-abstract so existing third-party backends remain instantiable.
        """

        _ = start, end
        raise LocalDataCapabilityError(
            "后端 {!r} 未提供 corporate_actions 数据集，无法查询 {}；"
            "请先导入公司行为数据".format(self.name, security)
        )

    def read_corporate_actions_batch(self, request: CorporateActionRequest) -> pd.DataFrame:
        """Read events for several securities using scalar fallback."""

        frames: List[pd.DataFrame] = []
        for security in request.securities:
            frame = self.read_corporate_actions(security, request.start, request.end)
            if not frame.empty:
                frames.append(frame)
        if not frames:
            return pd.DataFrame()
        return (
            pd.concat(frames, ignore_index=True, sort=False)
            .sort_values(["event_date", "security"], kind="stable")
            .reset_index(drop=True)
        )

    def batch_capabilities(self) -> Dict[str, str]:
        """Report whether each batch method is native or scalar fallback."""

        operations = {
            "bars": "read_bars_batch",
            "current_bars": "read_current_bars",
            "reference_factors": "read_reference_factors_batch",
            "security_info": "read_security_info_batch",
            "corporate_actions": "read_corporate_actions_batch",
        }
        result: Dict[str, str] = {}
        for operation, method_name in operations.items():
            implementation = getattr(type(self), method_name)
            default = getattr(LocalDataBackend, method_name)
            result[operation] = "fallback" if implementation is default else "native"
        return result

    def diagnostics(self) -> Dict[str, Any]:
        """Return non-sensitive backend diagnostics for support and tests."""

        return {
            "backend": self.name,
            "batch_capabilities": self.batch_capabilities(),
            "query_statistics": self.query_statistics(),
        }
