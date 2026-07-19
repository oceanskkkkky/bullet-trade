"""Local market-data backend contracts and implementations."""

from .base import (
    AssetType,
    BarRequest,
    BatchBarRequest,
    BatchQueryResult,
    CorporateActionRequest,
    CurrentBarRequest,
    LocalDataBackend,
    LocalDataCapabilityError,
    LocalDataConfigurationError,
    LocalDataError,
    LocalDataIncompatibleGenerationError,
    LocalDataMissingShardError,
    LocalDataQueryError,
    LocalDataSchemaError,
    LocalDataUnsupportedOperationError,
    ReferenceFactorRequest,
    SecurityInfoRequest,
)
from .corporate_actions import (
    ImportReport,
    TushareCorporateActionImporter,
    canonicalize_tushare_event_ids,
    normalize_tushare_fund_dividend,
    normalize_tushare_stock_dividend,
    security_type_map_from_catalogs,
)
from .duckdb import DuckDBDataBackend
from .duckdb_builder import DuckDBBuildConfig, DuckDBMaterializer, ensure_duckdb
from .manifest import GenerationManifest, ShardManifest, load_generation_manifest
from .parquet import ParquetDataBackend
from .schemas import CANONICAL_SCHEMA_VERSION, DATASET_REGISTRY, schema_registry_snapshot

__all__ = [
    "AssetType",
    "BatchBarRequest",
    "BatchQueryResult",
    "BarRequest",
    "CorporateActionRequest",
    "CurrentBarRequest",
    "LocalDataBackend",
    "LocalDataCapabilityError",
    "LocalDataConfigurationError",
    "LocalDataError",
    "LocalDataIncompatibleGenerationError",
    "LocalDataMissingShardError",
    "LocalDataQueryError",
    "LocalDataSchemaError",
    "LocalDataUnsupportedOperationError",
    "ParquetDataBackend",
    "ImportReport",
    "TushareCorporateActionImporter",
    "canonicalize_tushare_event_ids",
    "normalize_tushare_fund_dividend",
    "normalize_tushare_stock_dividend",
    "security_type_map_from_catalogs",
    "ReferenceFactorRequest",
    "SecurityInfoRequest",
    "CANONICAL_SCHEMA_VERSION",
    "DATASET_REGISTRY",
    "schema_registry_snapshot",
    "DuckDBBuildConfig",
    "DuckDBMaterializer",
    "DuckDBDataBackend",
    "ensure_duckdb",
    "GenerationManifest",
    "ShardManifest",
    "load_generation_manifest",
]
