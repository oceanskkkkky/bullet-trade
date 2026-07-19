"""Canonical corporate-action normalization and explicit Tushare ingestion."""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from .base import AssetType, LocalDataConfigurationError
from .manifest import atomic_write_json
from .schemas import CANONICAL_SCHEMA_VERSION, DATASET_REGISTRY

EFFECTIVE_STATUS = "effective"
REJECTED_STATUS = "rejected"
_CANCELLED_MARKERS = ("取消", "终止", "不实施", "未通过", "cancel", "terminate")
_IMPLEMENTED_MARKERS = ("实施", "完成", "implemented", "complete")


@dataclass(frozen=True)
class ImportReport:
    """Machine-readable result of one idempotent import run."""

    accepted: int
    rejected: int
    inserted: int
    updated: int
    total: int
    source_watermark: Optional[str]
    output_path: str
    rejected_reasons: Tuple[str, ...]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _date_value(value: Any) -> Optional[pd.Timestamp]:
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).normalize()


def _first_date(*values: Any) -> Optional[pd.Timestamp]:
    for value in values:
        parsed = _date_value(value)
        if parsed is not None:
            return parsed
    return None


def _canonical_payload(row: Mapping[str, Any]) -> str:
    payload = {
        str(key): None if pd.isna(value) else str(value)
        for key, value in sorted(row.items(), key=lambda item: str(item[0]))
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _payload_hash(row: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_payload(row).encode("utf-8")).hexdigest()


def _implementation_status(value: Any) -> str:
    text = str(value or "").strip().lower()
    if any(marker in text for marker in _CANCELLED_MARKERS):
        return REJECTED_STATUS
    if not text or any(marker in text for marker in _IMPLEMENTED_MARKERS):
        return EFFECTIVE_STATUS
    return REJECTED_STATUS


def _event_identity(source: str, security: str, row: Mapping[str, Any]) -> str:
    """Return a correction-stable identity for one economic action.

    Tushare may publish the same implemented plan more than once with a new
    announcement date or status text.  Those publication attributes must not
    create a second payable event.  ``end_date`` distinguishes independent
    reporting-period plans that happen to share an ex-date.
    """

    def date_identity(*values: Any) -> str:
        value = _first_date(*values)
        return "" if value is None else value.strftime("%Y%m%d")

    identity_fields = (
        date_identity(row.get("end_date")),
        date_identity(row.get("ex_date"), row.get("ann_date")),
        date_identity(row.get("record_date")),
        date_identity(row.get("pay_date")),
        date_identity(row.get("div_listdate")),
        date_identity(row.get("imp_ann_date"), row.get("imp_anndate")),
    )
    digest = hashlib.sha256(
        "|".join("" if value is None else str(value) for value in identity_fields).encode("utf-8")
    ).hexdigest()[:24]
    return "{}:{}:{}".format(source, security, digest)


def canonicalize_tushare_event_ids(frame: pd.DataFrame) -> pd.DataFrame:
    """Migrate normalized Tushare rows to the current stable identity scheme.

    Older normalized files did not retain ``period_end_date`` and cannot be
    migrated without another source fetch.  Frames that do contain the column
    are safe to migrate entirely, including rows whose period value is null.
    """

    if frame.empty or "period_end_date" not in frame.columns:
        return frame.copy()
    result = frame.copy()
    identities: List[Any] = []
    for row in result.to_dict(orient="records"):
        source = str(row.get("source") or "")
        security = str(row.get("security") or "")
        if source not in {"tushare.dividend", "tushare.fund_div"} or not security:
            identities.append(row.get("event_id"))
            continue
        identity = _event_identity(
            source,
            security,
            {
                "end_date": row.get("period_end_date"),
                "ex_date": row.get("event_date"),
                "record_date": row.get("record_date"),
                "pay_date": row.get("payment_date"),
                "div_listdate": row.get("listing_date"),
                "imp_ann_date": row.get("implementation_date"),
            },
        )
        identities.append(identity)
    result["event_id"] = identities
    if "source_event_id" in result.columns:
        tushare = result["source"].isin(("tushare.dividend", "tushare.fund_div"))
        result.loc[tushare, "source_event_id"] = result.loc[tushare, "event_id"]
    return result


def normalize_tushare_stock_dividend(
    row: Mapping[str, Any], *, security: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Normalize one Tushare ``dividend`` row to the canonical stock schema."""

    status = _implementation_status(row.get("div_proc"))
    event_date = _date_value(row.get("ex_date"))
    if status != EFFECTIVE_STATUS:
        return None, "stock dividend is not implemented"
    if event_date is None:
        return None, "stock dividend has no effective ex_date"
    stock_paid = _safe_float(row.get("stk_bo_rate"))
    transfer = _safe_float(row.get("stk_co_rate"))
    if stock_paid == 0.0 and transfer == 0.0:
        stock_paid = _safe_float(row.get("stk_div"))
    cash_per_share = _safe_float(row.get("cash_div_tax"))
    if cash_per_share == 0.0:
        cash_per_share = _safe_float(row.get("cash_div"))
    source_event_id = _event_identity("tushare.dividend", security, row)
    return {
        "event_id": source_event_id,
        "security": security,
        "security_type": AssetType.STOCK.value,
        "event_date": event_date,
        "scale_factor": 1.0 + stock_paid + transfer,
        "bonus_pre_tax": cash_per_share * 10.0,
        "per_base": 10.0,
        "period_end_date": _date_value(row.get("end_date")),
        "ann_date": _date_value(row.get("ann_date")),
        "record_date": _date_value(row.get("record_date")),
        "payment_date": _date_value(row.get("pay_date")),
        "listing_date": _date_value(row.get("div_listdate")),
        "implementation_date": _date_value(row.get("imp_ann_date")),
        "status": EFFECTIVE_STATUS,
        "source": "tushare.dividend",
        "source_event_id": source_event_id,
        "source_updated_at": _first_date(row.get("update_time"), row.get("ann_date"), event_date),
        "payload_hash": _payload_hash(row),
        "schema_version": CANONICAL_SCHEMA_VERSION,
    }, None


def normalize_tushare_fund_dividend(
    row: Mapping[str, Any], *, security: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Normalize one Tushare ``fund_div`` row to the canonical fund schema."""

    status = _implementation_status(row.get("div_proc"))
    event_date = _date_value(row.get("ex_date") or row.get("ann_date"))
    if status != EFFECTIVE_STATUS:
        return None, "fund dividend is not implemented"
    if event_date is None:
        return None, "fund dividend has no effective date"
    source_event_id = _event_identity("tushare.fund_div", security, row)
    return {
        "event_id": source_event_id,
        "security": security,
        "security_type": AssetType.FUND.value,
        "event_date": event_date,
        "scale_factor": _safe_float(row.get("split_ratio"), 1.0) or 1.0,
        "bonus_pre_tax": _safe_float(row.get("div_cash")),
        "per_base": 1.0,
        "period_end_date": _date_value(row.get("end_date")),
        "ann_date": _date_value(row.get("ann_date")),
        "record_date": _date_value(row.get("record_date")),
        "payment_date": _date_value(row.get("pay_date")),
        "listing_date": _date_value(row.get("div_listdate")),
        "implementation_date": _date_value(row.get("imp_anndate")),
        "status": EFFECTIVE_STATUS,
        "source": "tushare.fund_div",
        "source_event_id": source_event_id,
        "source_updated_at": _first_date(row.get("update_time"), row.get("ann_date"), event_date),
        "payload_hash": _payload_hash(row),
        "schema_version": CANONICAL_SCHEMA_VERSION,
    }, None


class TushareCorporateActionImporter:
    """Explicit, network-at-prepare-time importer with offline publication."""

    def __init__(
        self,
        client: Any,
        security_types: Mapping[str, AssetType],
        *,
        requests_per_minute: int = 120,
        sleep: Callable[[float], None] = time.sleep,
        network_max_attempts: int = 5,
        retry_backoff_seconds: float = 2.0,
        max_retry_delay_seconds: float = 30.0,
    ) -> None:
        if client is None:
            raise LocalDataConfigurationError("Tushare 公司行为导入需要显式 client")
        self.client = client
        self.security_types = dict(security_types)
        self.requests_per_minute = max(int(requests_per_minute), 1)
        self._sleep = sleep
        self.network_max_attempts = max(int(network_max_attempts), 1)
        self.retry_backoff_seconds = max(float(retry_backoff_seconds), 0.0)
        self.max_retry_delay_seconds = max(float(max_retry_delay_seconds), 0.0)

    @staticmethod
    def _is_transient_network_error(exc: Exception) -> bool:
        """Recognize transport failures without making requests a hard dependency."""

        transient_names = {
            "ConnectionError",
            "ConnectionResetError",
            "ConnectTimeout",
            "ProtocolError",
            "ReadTimeout",
            "RemoteDisconnected",
            "Timeout",
            "TimeoutError",
        }
        transient_winerrors = {10051, 10053, 10054, 10060, 10061, 10065}
        current: Optional[BaseException] = exc
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if isinstance(current, (ConnectionError, TimeoutError)):
                return True
            if type(current).__name__ in transient_names:
                return True
            if getattr(current, "winerror", None) in transient_winerrors:
                return True
            current = current.__cause__ or current.__context__
        return False

    def _fetch(self, security: str, asset_type: AssetType) -> pd.DataFrame:
        if asset_type == AssetType.FUND:
            method = getattr(self.client, "fund_div", None)
            endpoint = "fund_div"
        elif asset_type == AssetType.STOCK:
            method = getattr(self.client, "dividend", None)
            endpoint = "dividend"
        else:
            return pd.DataFrame()
        if not callable(method):
            raise LocalDataConfigurationError("Tushare client 未提供 {} 接口".format(endpoint))
        for attempt in range(1, self.network_max_attempts + 1):
            try:
                raw = method(ts_code=security)
                return pd.DataFrame() if raw is None else pd.DataFrame(raw)
            except Exception as exc:
                if attempt >= self.network_max_attempts or not self._is_transient_network_error(
                    exc
                ):
                    raise
                delay = min(
                    self.retry_backoff_seconds * (2 ** (attempt - 1)),
                    self.max_retry_delay_seconds,
                )
                if delay > 0:
                    self._sleep(delay)
        raise AssertionError("unreachable")

    def collect(
        self,
        securities: Optional[Sequence[str]] = None,
        *,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
    ) -> Tuple[pd.DataFrame, List[str]]:
        """Fetch and normalize requested securities, returning rejected reasons."""

        selected = list(securities or self.security_types.keys())
        events: List[Dict[str, Any]] = []
        rejected: List[str] = []
        interval = 60.0 / float(self.requests_per_minute)
        for index, security in enumerate(selected):
            asset_type = self.security_types.get(security)
            if asset_type not in (AssetType.STOCK, AssetType.FUND):
                rejected.append("{}: missing or unsupported catalog asset type".format(security))
                continue
            frame = self._fetch(security, asset_type)
            normalizer = (
                normalize_tushare_fund_dividend
                if asset_type == AssetType.FUND
                else normalize_tushare_stock_dividend
            )
            for row_index, row in frame.iterrows():
                event, reason = normalizer(row.to_dict(), security=security)
                if event is None:
                    rejected.append("{}[{}]: {}".format(security, row_index, reason))
                    continue
                event_date = pd.Timestamp(event["event_date"])
                if start is not None and event_date < pd.Timestamp(start).normalize():
                    continue
                if end is not None and event_date > pd.Timestamp(end).normalize():
                    continue
                events.append(event)
            if interval > 0 and index + 1 < len(selected):
                self._sleep(interval)
        columns = list(DATASET_REGISTRY["corporate_actions"].required_columns) + [
            "period_end_date",
            "ann_date",
            "record_date",
            "payment_date",
            "listing_date",
            "implementation_date",
            "schema_version",
        ]
        result = pd.DataFrame(events)
        if result.empty:
            return pd.DataFrame(columns=columns), rejected
        return self._deduplicate_events(result), rejected

    def publish(
        self,
        output_path: Path,
        securities: Optional[Sequence[str]] = None,
        *,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
    ) -> ImportReport:
        """Idempotently merge and atomically publish a canonical Parquet file."""

        path = Path(output_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        incoming, rejected = self.collect(securities, start=start, end=end)
        return self._merge_and_publish(path, incoming, rejected)

    def publish_resumable(
        self,
        output_path: Path,
        securities: Optional[Sequence[str]] = None,
        *,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
        checkpoint_path: Optional[Path] = None,
        checkpoint_every: int = 25,
        resume: bool = True,
    ) -> ImportReport:
        """Publish with bounded network-loss recovery at security boundaries."""

        path = Path(output_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        checkpoint = (
            Path(checkpoint_path).expanduser().resolve()
            if checkpoint_path is not None
            else path.with_name(path.name + ".import-state.json")
        )
        events_path = checkpoint.with_name(checkpoint.name + ".events.parquet")
        selected = tuple(str(item) for item in (securities or self.security_types.keys()))
        identity_payload = {
            "output_path": str(path),
            "securities": selected,
            "start": None if start is None else str(pd.Timestamp(start).normalize()),
            "end": None if end is None else str(pd.Timestamp(end).normalize()),
        }
        identity = hashlib.sha256(
            json.dumps(identity_payload, sort_keys=True).encode("utf-8")
        ).hexdigest()
        state: Dict[str, Any] = {
            "identity": identity,
            "completed": [],
            "rejected": [],
        }
        incoming = pd.DataFrame()
        if resume and checkpoint.is_file():
            try:
                loaded = json.loads(checkpoint.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise LocalDataConfigurationError(
                    "无法读取公司行为导入 checkpoint {}：{}".format(checkpoint, exc)
                ) from exc
            if loaded.get("identity") != identity:
                raise LocalDataConfigurationError(
                    "公司行为导入 checkpoint 与当前证券集合/日期/输出不一致；"
                    "请使用其他 checkpoint 或关闭 resume"
                )
            state = dict(loaded)
            if events_path.is_file():
                incoming = canonicalize_tushare_event_ids(pd.read_parquet(events_path))
        elif not resume:
            for candidate in (checkpoint, events_path):
                if candidate.exists():
                    candidate.unlink()
        completed = set(str(item) for item in state.get("completed", ()))
        pending = [security for security in selected if security not in completed]
        size = max(int(checkpoint_every), 1)
        for offset in range(0, len(pending), size):
            batch = pending[offset : offset + size]
            batch_events, batch_rejected = self.collect(batch, start=start, end=end)
            frames = [frame for frame in (incoming, batch_events) if not frame.empty]
            incoming = (
                pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
            )
            incoming = self._deduplicate_events(canonicalize_tushare_event_ids(incoming))
            state["completed"] = list(state.get("completed", ())) + list(batch)
            state["rejected"] = list(state.get("rejected", ())) + list(batch_rejected)
            state["updated_at"] = pd.Timestamp.utcnow().isoformat()
            self._atomic_write_frame(events_path, incoming)
            atomic_write_json(checkpoint, state)
        report = self._merge_and_publish(
            path, incoming, [str(item) for item in state.get("rejected", ())]
        )
        for candidate in (checkpoint, events_path):
            if candidate.exists():
                candidate.unlink()
        return report

    @staticmethod
    def _atomic_write_frame(path: Path, frame: pd.DataFrame) -> None:
        staging = path.with_name(path.name + ".staging-{}".format(os.getpid()))
        try:
            frame.to_parquet(staging, engine="pyarrow", index=False)
            os.replace(str(staging), str(path))
        finally:
            if staging.exists():
                staging.unlink()

    @staticmethod
    def _deduplicate_events(frame: pd.DataFrame) -> pd.DataFrame:
        """Select the newest deterministic publication for each event ID."""

        if frame.empty:
            return frame.reset_index(drop=True)
        result = frame.copy()
        for column in ("source_updated_at", "ann_date"):
            if column not in result.columns:
                result[column] = pd.NaT
            result[column] = pd.to_datetime(result[column], errors="coerce")
        if "payload_hash" not in result.columns:
            result["payload_hash"] = ""
        result = result.sort_values(
            [
                "event_date",
                "security",
                "event_id",
                "source_updated_at",
                "ann_date",
                "payload_hash",
            ],
            kind="stable",
            na_position="first",
        )
        return result.drop_duplicates("event_id", keep="last").reset_index(drop=True)

    def _merge_and_publish(
        self, path: Path, incoming: pd.DataFrame, rejected: Sequence[str]
    ) -> ImportReport:
        existing = pd.read_parquet(path) if path.is_file() else pd.DataFrame()
        existing = canonicalize_tushare_event_ids(existing)
        incoming = canonicalize_tushare_event_ids(incoming)
        old_hashes = (
            dict(zip(existing.get("event_id", ()), existing.get("payload_hash", ())))
            if not existing.empty
            else {}
        )
        inserted = sum(event_id not in old_hashes for event_id in incoming.get("event_id", ()))
        updated = sum(
            event_id in old_hashes and old_hashes[event_id] != payload_hash
            for event_id, payload_hash in zip(
                incoming.get("event_id", ()), incoming.get("payload_hash", ())
            )
        )
        frames = [frame for frame in (existing, incoming) if not frame.empty]
        merged = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
        merged = self._deduplicate_events(merged)
        self._atomic_write_frame(path, merged)
        watermark = None
        if not incoming.empty:
            values = pd.to_datetime(incoming["source_updated_at"], errors="coerce").dropna()
            if not values.empty:
                watermark = pd.Timestamp(values.max()).isoformat()
        return ImportReport(
            accepted=len(incoming),
            rejected=len(rejected),
            inserted=inserted,
            updated=updated,
            total=len(merged),
            source_watermark=watermark,
            output_path=str(path),
            rejected_reasons=tuple(rejected),
        )


def security_type_map_from_catalogs(
    stock_codes: Iterable[str], fund_codes: Iterable[str]
) -> Dict[str, AssetType]:
    """Build an explicit catalog-derived type map without code-prefix guesses."""

    result = {str(code): AssetType.STOCK for code in stock_codes}
    result.update({str(code): AssetType.FUND for code in fund_codes})
    return result
