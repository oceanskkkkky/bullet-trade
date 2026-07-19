"""Versioned canonical dataset registry for every local storage backend.

The registry is deliberately declarative: validation, DuckDB construction,
backend diagnostics, and contract tests all consume the same definitions.
Adding an optional column is backward compatible; changing a canonical key or
required column requires a schema-version increment.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

CANONICAL_SCHEMA_VERSION = "1.0.0"
DUCKDB_STORAGE_VERSION = "1"


@dataclass(frozen=True)
class DatasetSchema:
    """Canonical dataset declaration independent of storage format."""

    name: str
    domain: str
    asset_type: Optional[str]
    source_patterns: Tuple[str, ...]
    required_columns: Tuple[str, ...]
    optional_columns: Tuple[str, ...]
    key_columns: Tuple[str, ...]
    time_column: Optional[str]
    sort_columns: Tuple[str, ...]
    shard_rule: str


DATASET_REGISTRY: Dict[str, DatasetSchema] = {
    "stock_basic": DatasetSchema(
        "stock_basic",
        "meta",
        "stock",
        ("stock_basic_data.parquet",),
        ("ts_code", "name", "list_date"),
        ("delist_date", "industry", "market"),
        ("ts_code",),
        None,
        ("ts_code",),
        "meta",
    ),
    "fund_basic": DatasetSchema(
        "fund_basic",
        "meta",
        "fund",
        ("etf_basic_data.parquet",),
        ("ts_code", "list_date"),
        ("csname", "cname", "delist_date", "etf_type"),
        ("ts_code",),
        None,
        ("ts_code",),
        "meta",
    ),
    "index_basic": DatasetSchema(
        "index_basic",
        "meta",
        "index",
        ("index_basic.parquet",),
        ("ts_code", "name"),
        ("list_date", "base_date", "category", "market"),
        ("ts_code",),
        None,
        ("ts_code",),
        "meta",
    ),
    "stock_daily": DatasetSchema(
        "stock_daily",
        "daily",
        "stock",
        ("stock_daily.parquet", "daily_adj_*.parquet"),
        ("ts_code", "trade_date", "open", "high", "low", "close"),
        ("vol", "amount", "adj_factor", "suspend_type", "is_st"),
        ("ts_code", "trade_date"),
        "trade_date",
        ("trade_date", "ts_code"),
        "daily",
    ),
    "fund_daily": DatasetSchema(
        "fund_daily",
        "daily",
        "fund",
        ("etf_daily.parquet",),
        ("ts_code", "trade_date", "open", "high", "low", "close"),
        ("vol", "amount", "adj_factor"),
        ("ts_code", "trade_date"),
        "trade_date",
        ("trade_date", "ts_code"),
        "daily",
    ),
    "index_daily": DatasetSchema(
        "index_daily",
        "daily",
        "index",
        ("index_daily/*.parquet",),
        ("ts_code", "trade_date", "open", "high", "low", "close"),
        ("vol", "amount", "con_codes", "weights"),
        ("ts_code", "trade_date"),
        "trade_date",
        ("trade_date", "ts_code"),
        "daily",
    ),
    "income": DatasetSchema(
        "income",
        "finance",
        None,
        ("income_cleaned.parquet",),
        ("ts_code", "ann_date", "end_date"),
        (),
        ("ts_code", "end_date", "ann_date"),
        "ann_date",
        ("ts_code", "ann_date", "end_date"),
        "finance",
    ),
    "balance": DatasetSchema(
        "balance",
        "finance",
        None,
        ("balancesheet_cleaned.parquet",),
        ("ts_code", "ann_date", "end_date"),
        (),
        ("ts_code", "end_date", "ann_date"),
        "ann_date",
        ("ts_code", "ann_date", "end_date"),
        "finance",
    ),
    "cash_flow": DatasetSchema(
        "cash_flow",
        "finance",
        None,
        ("cashflow_cleaned.parquet",),
        ("ts_code", "ann_date", "end_date"),
        (),
        ("ts_code", "end_date", "ann_date"),
        "ann_date",
        ("ts_code", "ann_date", "end_date"),
        "finance",
    ),
    "indicator": DatasetSchema(
        "indicator",
        "finance",
        None,
        ("fina_indicator_cleaned.parquet",),
        ("ts_code", "ann_date", "end_date"),
        (),
        ("ts_code", "end_date", "ann_date"),
        "ann_date",
        ("ts_code", "ann_date", "end_date"),
        "finance",
    ),
    "corporate_actions": DatasetSchema(
        "corporate_actions",
        "actions",
        None,
        ("corporate_actions.parquet",),
        (
            "event_id",
            "security",
            "security_type",
            "event_date",
            "scale_factor",
            "bonus_pre_tax",
            "per_base",
            "status",
            "source",
            "source_event_id",
            "source_updated_at",
            "payload_hash",
        ),
        ("ann_date", "record_date", "payment_date", "listing_date"),
        ("event_id",),
        "event_date",
        ("event_date", "security"),
        "actions",
    ),
}

MINUTE_FREQUENCIES = ("1m", "5m", "15m", "30m", "60m")
for _asset, _source_prefix in (("stock", "stock"), ("fund", "etf"), ("index", "index")):
    for _frequency in MINUTE_FREQUENCIES:
        _minutes = _frequency[:-1]
        _name = "{}_{}".format(_asset, _frequency)
        DATASET_REGISTRY[_name] = DatasetSchema(
            _name,
            "minute",
            _asset,
            ("{}_{}min/*.parquet".format(_source_prefix, _minutes),),
            ("ts_code", "trade_time", "open", "high", "low", "close"),
            ("trade_date", "vol", "amount", "adj_factor"),
            ("ts_code", "trade_time"),
            "trade_time",
            ("ts_code", "trade_time"),
            "minute:{}:{}:year".format(_asset, _frequency),
        )


def schema_registry_snapshot() -> Dict[str, object]:
    """Return stable, JSON-serializable registry metadata."""

    return {
        "schema_version": CANONICAL_SCHEMA_VERSION,
        "storage_version": DUCKDB_STORAGE_VERSION,
        "datasets": {
            name: {
                "domain": item.domain,
                "asset_type": item.asset_type,
                "required_columns": list(item.required_columns),
                "key_columns": list(item.key_columns),
                "time_column": item.time_column,
                "sort_columns": list(item.sort_columns),
                "shard_rule": item.shard_rule,
            }
            for name, item in DATASET_REGISTRY.items()
        },
    }
