import json
import inspect
from pathlib import Path

import pandas as pd
import pytest

from bullet_trade.cli.main import create_parser
from bullet_trade.core.engine import BacktestEngine
from bullet_trade.data import api as data_api
from bullet_trade.data.api import _create_provider, _normalize_provider_name
from bullet_trade.data.local import (
    CANONICAL_SCHEMA_VERSION,
    AssetType,
    BarRequest,
    BatchBarRequest,
    DuckDBBuildConfig,
    DuckDBDataBackend,
    DuckDBMaterializer,
    LocalDataBackend,
    LocalDataCapabilityError,
    LocalDataConfigurationError,
    LocalDataIncompatibleGenerationError,
    LocalDataMissingShardError,
    LocalDataSchemaError,
    LocalDataUnsupportedOperationError,
    TushareCorporateActionImporter,
    canonicalize_tushare_event_ids,
    normalize_tushare_fund_dividend,
    normalize_tushare_stock_dividend,
    schema_registry_snapshot,
)
from bullet_trade.data.providers.local import LocalDataProvider


def _write(frame: pd.DataFrame, path: Path, *, index=None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if index:
        frame = frame.set_index(index)
    frame.to_parquet(path, engine="pyarrow")


@pytest.fixture()
def local_root(tmp_path: Path) -> Path:
    root = tmp_path / "parquet"
    stock = root / "stock"
    fund = root / "fund"
    index = root / "index"
    finance = root / "finance"

    _write(
        pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "list_date": "19910403",
                    "delist_date": None,
                    "industry": "银行",
                    "market": "主板",
                }
            ]
        ),
        stock / "stock_basic_data.parquet",
    )
    _write(
        pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2025-01-02"),
                    "ts_code": "000001.SZ",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                    "vol": 100.0,
                    "amount": 2.0,
                    "adj_factor": 1.0,
                    "suspend_type": "N",
                    "is_st": 0,
                    "total_mv": 1_000_000.0,
                    "circ_mv": 800_000.0,
                    "pe": 5.0,
                },
                {
                    "trade_date": pd.Timestamp("2025-01-06"),
                    "ts_code": "000001.SZ",
                    "open": 10.5,
                    "high": 12.0,
                    "low": 10.0,
                    "close": 11.0,
                    "vol": 200.0,
                    "amount": 3.0,
                    "adj_factor": 2.0,
                    "suspend_type": "N",
                    "is_st": 0,
                    "total_mv": 1_100_000.0,
                    "circ_mv": 900_000.0,
                    "pe": 5.5,
                },
            ]
        ),
        stock / "stock_daily.parquet",
        index=["trade_date", "ts_code"],
    )
    _write(
        pd.DataFrame(
            [
                {
                    "ts_code": "510300.SH",
                    "csname": "沪深300ETF",
                    "cname": "沪深300交易型开放式指数基金",
                    "list_date": "20120528",
                    "etf_type": "股票型",
                }
            ]
        ),
        fund / "etf_basic_data.parquet",
    )
    _write(
        pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2025-01-02"),
                    "ts_code": "510300.SH",
                    "open": 4.0,
                    "high": 4.1,
                    "low": 3.9,
                    "close": 4.0,
                    "vol": 10.0,
                    "amount": 1.0,
                    "adj_factor": 1.0,
                }
            ]
        ),
        fund / "etf_daily.parquet",
        index=["trade_date", "ts_code"],
    )
    _write(
        pd.DataFrame(
            [
                {
                    "ts_code": "000001.SH",
                    "name": "上证指数",
                    "market": "SSE",
                    "publisher": "中证",
                    "category": "综合",
                    "base_date": "19901219",
                    "list_date": "19910715",
                },
                {
                    "ts_code": "000300.SH",
                    "name": "沪深300",
                    "market": "SSE",
                    "publisher": "中证",
                    "category": "规模",
                    "base_date": "20041231",
                    "list_date": "20050408",
                },
            ]
        ),
        index / "index_basic.parquet",
    )
    calendar_rows = []
    for day in ("2025-01-02", "2025-01-03", "2025-01-06"):
        calendar_rows.append(
            {
                "trade_date": pd.Timestamp(day),
                "ts_code": "000001.SH",
                "open": 3000.0,
                "high": 3010.0,
                "low": 2990.0,
                "close": 3005.0,
                "vol": 1000.0,
                "amount": 2000.0,
                "con_codes": None,
                "weights": None,
            }
        )
    _write(
        pd.DataFrame(calendar_rows),
        index / "index_daily" / "000001.SH.parquet",
        index=["trade_date"],
    )
    _write(
        pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2025-01-02"),
                    "ts_code": "000300.SH",
                    "open": 4000.0,
                    "high": 4010.0,
                    "low": 3990.0,
                    "close": 4005.0,
                    "vol": 1000.0,
                    "amount": 2000.0,
                    "con_codes": ["000001.SZ", "000002.SZ"],
                    "weights": [3.5, 2.5],
                }
            ]
        ),
        index / "index_daily" / "000300.SH.parquet",
        index=["trade_date"],
    )

    financial_rows = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "ann_date": "20240420",
                "end_date": "20240331",
                "report_type": "1",
                "update_flag": "0",
                "n_income": 10.0,
                "total_assets": 100.0,
                "n_cashflow_act": 5.0,
                "roe": 8.0,
            },
            {
                "ts_code": "000001.SZ",
                "ann_date": "20250420",
                "end_date": "20250331",
                "report_type": "1",
                "update_flag": "0",
                "n_income": 20.0,
                "total_assets": 120.0,
                "n_cashflow_act": 6.0,
                "roe": 9.0,
            },
        ]
    )
    for filename in (
        "income_cleaned.parquet",
        "balancesheet_cleaned.parquet",
        "cashflow_cleaned.parquet",
        "fina_indicator_cleaned.parquet",
    ):
        _write(financial_rows, finance / filename)
    return root


def test_local_provider_reads_and_fills_daily_bars(local_root: Path) -> None:
    provider = LocalDataProvider({"path": str(local_root)})

    result = provider.get_price(
        "000001.XSHE",
        start_date="2025-01-02",
        end_date="2025-01-06",
        fields=["close", "volume", "money", "paused"],
        fq=None,
    )

    assert list(result.index) == list(pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-06"]))
    assert result.loc["2025-01-02", "volume"] == pytest.approx(10_000.0)
    assert result.loc["2025-01-02", "money"] == pytest.approx(2_000.0)
    assert result.loc["2025-01-03", "close"] == pytest.approx(10.5)
    assert result.loc["2025-01-03", "volume"] == 0.0
    assert result.loc["2025-01-03", "paused"] == 1
    assert result.loc["2025-01-06", "paused"] == 0

    suspended_only = provider.get_price(
        "000001.XSHE",
        start_date="2025-01-03",
        end_date="2025-01-03",
        fields=["close", "volume", "paused"],
        fq=None,
    )
    assert suspended_only.loc["2025-01-03", "close"] == pytest.approx(10.5)
    assert suspended_only.loc["2025-01-03", "volume"] == 0.0
    assert suspended_only.loc["2025-01-03", "paused"] == 1


def test_local_provider_adjustment_and_multi_security_long_shape(local_root: Path) -> None:
    provider = LocalDataProvider({"path": str(local_root)})
    adjusted = provider.get_price(
        "000001.XSHE",
        start_date="2025-01-02",
        end_date="2025-01-06",
        fields=["close"],
        fq="pre",
        pre_factor_ref_date="2025-01-06",
    )
    assert adjusted.loc["2025-01-02", "close"] == pytest.approx(5.25)
    assert adjusted.loc["2025-01-06", "close"] == pytest.approx(11.0)
    post_adjusted = provider.get_price(
        "000001.XSHE",
        start_date="2025-01-02",
        end_date="2025-01-06",
        fields=["close"],
        fq="post",
    )
    assert post_adjusted.loc["2025-01-02", "close"] == pytest.approx(10.5)
    assert post_adjusted.loc["2025-01-06", "close"] == pytest.approx(22.0)

    long_frame = provider.get_price(
        ["000001.XSHE", "510300.XSHG"],
        start_date="2025-01-02",
        end_date="2025-01-02",
        fields=["close"],
        fq=None,
        panel=False,
    )
    assert list(long_frame.columns) == ["time", "code", "close"]
    assert set(long_frame["code"]) == {"000001.XSHE", "510300.XSHG"}


def test_local_provider_reads_historical_index_components(local_root: Path) -> None:
    provider = LocalDataProvider({"path": str(local_root)})

    assert provider.get_trade_day("000001.XSHE", "2025-01-05") == {
        "000001.XSHE": pd.Timestamp("2025-01-03").date()
    }
    assert provider.get_index_stocks("000300.XSHG", date="2025-01-02") == [
        "000001.XSHE",
        "000002.XSHE",
    ]
    weights = provider.get_index_weights("000300.XSHG", date="2025-01-02")
    assert weights.loc["000001.XSHE", "weight"] == pytest.approx(3.5)


def test_local_provider_uses_long_history_stock_gap_file(local_root: Path) -> None:
    _write(
        pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20081231",
                    "open": 8.0,
                    "high": 8.5,
                    "low": 7.5,
                    "close": 8.2,
                    "vol": 12.0,
                    "amount": 0.5,
                    "adj_factor": 0.8,
                }
            ]
        ),
        local_root / "legacy" / "daily_adj_19910101_20250101.parquet",
    )
    provider = LocalDataProvider({"path": str(local_root)})

    result = provider.get_price(
        "000001.XSHE",
        start_date="2008-12-31",
        end_date="2008-12-31",
        fields=["close", "volume", "money"],
        fq=None,
    )

    assert result.loc["2008-12-31", "close"] == pytest.approx(8.2)
    assert result.loc["2008-12-31", "volume"] == pytest.approx(1200.0)
    assert result.loc["2008-12-31", "money"] == pytest.approx(500.0)


def test_local_provider_executes_point_in_time_fundamentals(local_root: Path) -> None:
    jq = pytest.importorskip("jqdatasdk")
    provider = LocalDataProvider({"path": str(local_root)})
    query = jq.query(
        jq.income.code,
        jq.income.net_profit,
        jq.income.pubDate,
        jq.income.statDate,
    ).filter(jq.income.code.in_(["000001.XSHE"]))

    old = provider.get_fundamentals(query, date="2025-04-19")
    new = provider.get_fundamentals(query, date="2025-04-20")

    assert old.loc[0, "net_profit"] == pytest.approx(10.0)
    assert new.loc[0, "net_profit"] == pytest.approx(20.0)


def test_local_provider_valuation_units_and_ordering(local_root: Path) -> None:
    jq = pytest.importorskip("jqdatasdk")
    provider = LocalDataProvider({"path": str(local_root)})
    query = (
        jq.query(jq.valuation.code, jq.valuation.market_cap)
        .filter(jq.valuation.code.in_(["000001.XSHE"]))
        .order_by(jq.valuation.market_cap.asc())
        .limit(1)
    )

    result = provider.get_fundamentals(query, date="2025-01-06")

    assert result.loc[0, "code"] == "000001.XSHE"
    assert result.loc[0, "market_cap"] == pytest.approx(110.0)

    filter_only_query = jq.query(jq.valuation.code).filter(jq.valuation.market_cap > 100.0)
    filtered = provider.get_fundamentals(filter_only_query, date="2025-01-06")
    assert filtered["code"].tolist() == ["000001.XSHE"]


def test_local_provider_reports_configuration_and_schema_errors(tmp_path: Path) -> None:
    missing = LocalDataProvider({"path": str(tmp_path / "missing")})
    with pytest.raises(LocalDataConfigurationError, match="LOCAL_DATA_PATH"):
        missing.auth()

    incomplete = tmp_path / "incomplete"
    incomplete.mkdir()
    provider = LocalDataProvider({"path": str(incomplete)})
    with pytest.raises(LocalDataSchemaError, match="stock_basic_data"):
        provider.auth()


def test_local_provider_factory_aliases(local_root: Path) -> None:
    assert _normalize_provider_name("parquet") == "local"
    assert _normalize_provider_name("local-parquet") == "local"

    provider = _create_provider("parquet", {"path": str(local_root)})

    assert isinstance(provider, LocalDataProvider)
    provider.auth()


def test_local_provider_resolves_relative_path_from_project_root(
    local_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_root = local_root.parent
    (project_root / "pyproject.toml").write_text(
        "[project]\nname='local-fixture'\n", encoding="utf-8"
    )
    nested = project_root / "strategies" / "demo"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)

    provider = LocalDataProvider({"path": "parquet"})
    provider.auth()

    assert Path(provider.diagnostics()["root"]) == local_root.resolve()


@pytest.mark.parametrize("panel", [True, False])
def test_local_provider_scalar_and_batch_price_contracts_are_equal(
    local_root: Path, panel: bool
) -> None:
    securities = ["000001.XSHE", "510300.XSHG"]
    common = {
        "start_date": "2025-01-02",
        "end_date": "2025-01-06",
        "fields": ["open", "close", "volume", "money", "paused"],
        "fq": None,
        "panel": panel,
    }
    scalar = LocalDataProvider({"path": str(local_root), "query_mode": "scalar"}).get_price(
        securities, **common
    )
    batch = LocalDataProvider({"path": str(local_root), "query_mode": "batch"}).get_price(
        securities, **common
    )

    pd.testing.assert_frame_equal(batch, scalar, check_dtype=True, check_exact=True)


@pytest.mark.parametrize("fq", ["pre", "post"])
def test_local_provider_vectorized_batch_adjustment_matches_scalar(
    local_root: Path, fq: str
) -> None:
    securities = ["000001.XSHE", "510300.XSHG"]
    common = {
        "start_date": "2025-01-02",
        "end_date": "2025-01-06",
        "fields": ["open", "high", "low", "close", "volume", "money", "paused"],
        "fq": fq,
        "pre_factor_ref_date": "2025-01-06",
        "panel": False,
    }
    scalar = LocalDataProvider({"path": str(local_root), "query_mode": "scalar"}).get_price(
        securities, **common
    )
    batch = LocalDataProvider({"path": str(local_root), "query_mode": "batch"}).get_price(
        securities, **common
    )

    pd.testing.assert_frame_equal(batch, scalar, check_dtype=True, check_exact=True)


def test_local_provider_batch_count_empty_and_error_semantics(local_root: Path) -> None:
    provider = LocalDataProvider({"path": str(local_root), "query_mode": "batch"})

    counted = provider.get_price(
        ["000001.XSHE", "510300.XSHG"],
        end_date="2025-01-06",
        count=1,
        fields=["close"],
        fq=None,
        panel=False,
    )
    assert counted.groupby("code").size().to_dict() == {
        "000001.XSHE": 1,
        "510300.XSHG": 1,
    }
    empty = provider.get_price([], fields=["close"], fq=None)
    assert empty.empty
    assert list(empty.columns) == ["close"]

    with pytest.raises(Exception, match="本地基础信息中不存在证券代码"):
        provider.get_price(["999999.XSHE"], fields=["close"], fq=None)
    with pytest.raises(LocalDataUnsupportedOperationError, match="仅支持 'is_st'"):
        provider.get_extras("industry", ["000001.XSHE"])
    with pytest.raises(LocalDataConfigurationError, match="LOCAL_DATA_QUERY_MODE"):
        LocalDataProvider({"path": str(local_root), "query_mode": "auto"})


def test_local_provider_capability_schema_and_query_diagnostics(local_root: Path) -> None:
    provider = LocalDataProvider({"path": str(local_root), "query_mode": "batch"})
    provider.backend.reset_query_statistics()
    provider.get_price(
        ["000001.XSHE", "510300.XSHG"],
        start_date="2025-01-02",
        end_date="2025-01-02",
        fields=["close"],
        fq=None,
        panel=False,
    )

    diagnostics = provider.diagnostics()
    assert diagnostics["schema_version"] == CANONICAL_SCHEMA_VERSION
    assert diagnostics["query_mode"] == "batch"
    assert diagnostics["batch_capabilities"]["bars"] == "fallback"
    assert diagnostics["query_statistics"]["query_count"] >= 2
    assert diagnostics["query_statistics"]["rows_read"] >= 2
    assert diagnostics["query_statistics"]["peak_result_bytes"] > 0
    assert diagnostics["cache_statistics"]["schema"]["hits"] >= 0
    assert diagnostics["capabilities"]["get_price"]["status"] == "supported"
    assert diagnostics["capabilities"]["get_fundamentals"]["status"] == "limited"
    assert diagnostics["capabilities"]["tick/live/subscription"]["status"] == "unsupported"

    registry = schema_registry_snapshot()
    assert registry["schema_version"] == CANONICAL_SCHEMA_VERSION
    assert registry["datasets"]["stock_daily"]["key_columns"] == [
        "ts_code",
        "trade_date",
    ]
    assert registry["datasets"]["corporate_actions"]["shard_rule"] == "actions"


def test_local_provider_live_and_tick_operations_fail_explicitly(local_root: Path) -> None:
    provider = LocalDataProvider({"path": str(local_root)})

    with pytest.raises(LocalDataUnsupportedOperationError, match="get_ticks"):
        provider.get_ticks("000001.XSHE", end_dt="2025-01-06")
    with pytest.raises(LocalDataUnsupportedOperationError, match="get_current_tick"):
        provider.get_current_tick("000001.XSHE", dt="2025-01-06")
    with pytest.raises(LocalDataUnsupportedOperationError, match="subscribe_ticks"):
        provider.subscribe_ticks(["000001.XSHE"])


def test_local_and_jqdata_shared_public_parameter_contracts_are_stable() -> None:
    pytest.importorskip("jqdatasdk")
    from bullet_trade.data.providers.jqdata import JQDataProvider

    shared = (
        "get_price",
        "get_trade_days",
        "get_trade_day",
        "get_all_securities",
        "get_security_info",
        "get_index_stocks",
        "get_index_weights",
        "get_bars",
        "get_extras",
        "get_fundamentals",
        "get_fundamentals_continuously",
        "get_split_dividend",
        "get_ticks",
        "get_current_tick",
    )

    def parameter_contract(method):
        return [
            (parameter.name, parameter.kind, parameter.default)
            for parameter in inspect.signature(method).parameters.values()
        ]

    for name in shared:
        assert parameter_contract(getattr(LocalDataProvider, name)) == parameter_contract(
            getattr(JQDataProvider, name)
        ), name


def test_typed_local_data_errors_are_distinguishable() -> None:
    assert issubclass(LocalDataMissingShardError, LocalDataSchemaError)
    assert issubclass(LocalDataIncompatibleGenerationError, LocalDataSchemaError)
    assert issubclass(LocalDataUnsupportedOperationError, LocalDataCapabilityError)


class _ScalarOnlyBackend(LocalDataBackend):
    """Old-style backend proving new batch methods are non-breaking."""

    name = "scalar-only"

    def validate(self) -> None:
        return None

    def asset_type(self, security: str) -> AssetType:
        return AssetType.STOCK

    def read_bars(self, request: BarRequest) -> pd.DataFrame:
        return pd.DataFrame(
            [{"time": pd.Timestamp("2025-01-02"), "code": request.security, "close": 1.0}]
        )

    def read_trade_days(self, start=None, end=None):
        return [pd.Timestamp("2025-01-02").to_pydatetime()]

    def read_securities(self, asset_types, date=None):
        return pd.DataFrame(index=pd.Index(["000001.SZ"], name="ts_code"))

    def read_index_components(self, index_code, date=None):
        return pd.DataFrame(columns=["code", "weight"])

    def read_financial_table(self, table, columns, *, as_of, stat_date=None, securities=None):
        return pd.DataFrame()


def test_old_backend_subclass_gets_correct_batch_fallback() -> None:
    backend = _ScalarOnlyBackend()
    result = backend.read_bars_batch(
        BatchBarRequest(
            securities=("000002.SZ", "000001.SZ"),
            asset_type=AssetType.STOCK,
            frequency="1d",
            columns=("close",),
            count=1,
        )
    )

    assert result["code"].tolist() == ["000001.SZ", "000002.SZ"]
    assert backend.batch_capabilities()["bars"] == "fallback"
    with pytest.raises(LocalDataCapabilityError, match="corporate_actions"):
        backend.read_corporate_actions("000001.SZ")


def _corporate_action_rows() -> pd.DataFrame:
    common = {
        "status": "effective",
        "source_updated_at": pd.Timestamp("2025-01-01"),
        "payload_hash": "hash",
        "ann_date": pd.Timestamp("2024-12-01"),
        "record_date": pd.Timestamp("2025-01-02"),
        "payment_date": pd.Timestamp("2025-01-06"),
        "listing_date": pd.NaT,
    }
    return pd.DataFrame(
        [
            {
                **common,
                "event_id": "stock-event",
                "security": "000001.SZ",
                "security_type": "stock",
                "event_date": pd.Timestamp("2025-01-03"),
                "scale_factor": 1.3,
                "bonus_pre_tax": 2.5,
                "per_base": 10.0,
                "source": "tushare.dividend",
                "source_event_id": "stock-event",
            },
            {
                **common,
                "event_id": "stock-event",
                "security": "000001.SZ",
                "security_type": "stock",
                "event_date": pd.Timestamp("2025-01-03"),
                "scale_factor": 1.3,
                "bonus_pre_tax": 2.5,
                "per_base": 10.0,
                "source": "tushare.dividend",
                "source_event_id": "stock-event",
            },
            {
                **common,
                "event_id": "cancelled-event",
                "security": "000001.SZ",
                "security_type": "stock",
                "event_date": pd.Timestamp("2025-01-04"),
                "scale_factor": 1.0,
                "bonus_pre_tax": 1.0,
                "per_base": 10.0,
                "status": "rejected",
                "source": "tushare.dividend",
                "source_event_id": "cancelled-event",
            },
            {
                **common,
                "event_id": "fund-event",
                "security": "510300.SH",
                "security_type": "fund",
                "event_date": pd.Timestamp("2025-01-06"),
                "scale_factor": 1.0,
                "bonus_pre_tax": 0.12,
                "per_base": 1.0,
                "source": "tushare.fund_div",
                "source_event_id": "fund-event",
            },
        ]
    )


def test_local_provider_reads_canonical_corporate_actions(local_root: Path) -> None:
    _write(_corporate_action_rows(), local_root / "actions" / "corporate_actions.parquet")
    provider = LocalDataProvider({"path": str(local_root)})

    stock = provider.get_split_dividend(
        "000001.XSHE", start_date="2025-01-03", end_date="2025-01-03"
    )
    fund = provider.get_split_dividend(
        "510300.XSHG", start_date="2025-01-01", end_date="2025-01-06"
    )

    assert stock == [
        {
            "security": "000001.XSHE",
            "date": pd.Timestamp("2025-01-03").date(),
            "security_type": "stock",
            "scale_factor": 1.3,
            "bonus_pre_tax": 2.5,
            "per_base": 10.0,
        }
    ]
    assert fund[0]["security_type"] == "fund"
    assert fund[0]["bonus_pre_tax"] == pytest.approx(0.12)
    assert (
        provider.get_split_dividend("000001.XSHE", start_date="2025-01-04", end_date="2025-01-04")
        == []
    )


def test_local_provider_missing_corporate_actions_is_actionable(local_root: Path) -> None:
    provider = LocalDataProvider({"path": str(local_root)})
    with pytest.raises(LocalDataCapabilityError, match="corporate_actions.parquet"):
        provider.get_split_dividend("000001.XSHE", start_date="2025-01-01", end_date="2025-01-31")


def test_local_backtest_preflight_requires_actions_and_accepts_explicit_opt_out(
    local_root: Path,
) -> None:
    required = LocalDataProvider({"path": str(local_root)})
    with pytest.raises(LocalDataConfigurationError, match="import-corporate-actions"):
        required.preflight_backtest(start_date="2025-01-02", end_date="2025-01-06", frequency="day")

    price_only = LocalDataProvider({"path": str(local_root), "require_corporate_actions": False})
    report = price_only.preflight_backtest(
        start_date="2025-01-02", end_date="2025-01-06", frequency="day"
    )
    assert report["frequency"] == "1d"
    assert report["first_trade_day"] == "2025-01-02"


def test_local_backtest_preflight_accepts_canonical_actions(local_root: Path) -> None:
    _write(_corporate_action_rows(), local_root / "actions" / "corporate_actions.parquet")
    provider = LocalDataProvider({"path": str(local_root)})

    report = provider.preflight_backtest(
        start_date="2025-01-02",
        end_date="2025-01-06",
        frequency="daily",
        securities=["000001.XSHE"],
    )

    assert "corporate_actions" in report["required_datasets"]


def test_tushare_corporate_action_normalizers() -> None:
    stock, stock_error = normalize_tushare_stock_dividend(
        {
            "ann_date": "20241201",
            "end_date": "20241231",
            "div_proc": "实施",
            "ex_date": "20250103",
            "cash_div_tax": 0.25,
            "stk_bo_rate": 0.1,
            "stk_co_rate": 0.2,
        },
        security="000001.SZ",
    )
    fund, fund_error = normalize_tushare_fund_dividend(
        {
            "ann_date": "20241201",
            "div_proc": "实施",
            "ex_date": "20250106",
            "div_cash": 0.12,
        },
        security="510300.SH",
    )
    cancelled, reason = normalize_tushare_stock_dividend(
        {"div_proc": "取消分红", "ex_date": "20250103"}, security="000001.SZ"
    )

    assert stock_error is None
    assert stock["scale_factor"] == pytest.approx(1.3)
    assert stock["bonus_pre_tax"] == pytest.approx(2.5)
    assert stock["per_base"] == 10.0
    assert stock["period_end_date"] == pd.Timestamp("2024-12-31")
    assert fund_error is None
    assert fund["bonus_pre_tax"] == pytest.approx(0.12)
    assert fund["per_base"] == 1.0
    assert cancelled is None
    assert "not implemented" in reason


class _FakeTusharePro:
    def __init__(self) -> None:
        self.dividend_calls = []
        self.fund_div_calls = []
        self.cash = 0.25

    def dividend(self, **kwargs):
        self.dividend_calls.append(kwargs)
        return pd.DataFrame(
            [
                {
                    "ann_date": "20241201",
                    "end_date": "20241231",
                    "div_proc": "实施",
                    "ex_date": "20250103",
                    "cash_div_tax": self.cash,
                    "stk_bo_rate": 0.1,
                    "stk_co_rate": 0.2,
                },
                {"div_proc": "预案", "ex_date": "20250104"},
            ]
        )

    def fund_div(self, **kwargs):
        self.fund_div_calls.append(kwargs)
        return pd.DataFrame(
            [
                {
                    "ann_date": "20241201",
                    "div_proc": "实施",
                    "ex_date": "20250106",
                    "div_cash": 0.12,
                }
            ]
        )


def test_tushare_import_is_offline_queryable_idempotent_and_correctable(
    local_root: Path,
) -> None:
    client = _FakeTusharePro()
    importer = TushareCorporateActionImporter(
        client,
        {"000001.SZ": AssetType.STOCK, "510300.SH": AssetType.FUND},
        requests_per_minute=1000000,
        sleep=lambda _: None,
    )
    output = local_root / "actions" / "corporate_actions.parquet"

    first = importer.publish(
        output, start=pd.Timestamp("2025-01-01"), end=pd.Timestamp("2025-01-31")
    )
    second = importer.publish(
        output, start=pd.Timestamp("2025-01-01"), end=pd.Timestamp("2025-01-31")
    )
    client.cash = 0.3
    corrected = importer.publish(
        output, start=pd.Timestamp("2025-01-01"), end=pd.Timestamp("2025-01-31")
    )

    assert first.inserted == 2
    assert first.rejected == 1
    assert second.inserted == 0
    assert second.updated == 0
    assert corrected.updated == 1
    stored = pd.read_parquet(output)
    assert len(stored) == 2
    provider = LocalDataProvider({"path": str(local_root)})
    event = provider.get_split_dividend(
        "000001.XSHE", start_date="2025-01-03", end_date="2025-01-03"
    )[0]
    assert event["bonus_pre_tax"] == pytest.approx(3.0)
    assert client.dividend_calls and client.fund_div_calls


def test_tushare_import_collapses_republished_plan_but_preserves_periods(
    local_root: Path,
) -> None:
    class _RepublishedPlans(_FakeTusharePro):
        def dividend(self, **kwargs):
            self.dividend_calls.append(kwargs)
            return pd.DataFrame(
                [
                    {
                        "end_date": "20241231",
                        "ann_date": "20250301",
                        "div_proc": "实施",
                        "ex_date": "20250601",
                        "record_date": "20250530",
                        "cash_div_tax": 0.2,
                    },
                    {
                        "end_date": "20241231",
                        "ann_date": "20250520",
                        "div_proc": "实施完成",
                        "ex_date": "20250601",
                        "record_date": "20250530",
                        "cash_div_tax": 0.3,
                    },
                    {
                        "end_date": "20250331",
                        "ann_date": "20250521",
                        "div_proc": "实施",
                        "ex_date": "20250601",
                        "record_date": "20250530",
                        "cash_div_tax": 0.1,
                    },
                ]
            )

    importer = TushareCorporateActionImporter(
        _RepublishedPlans(),
        {"000001.SZ": AssetType.STOCK},
        requests_per_minute=1000000,
        sleep=lambda _: None,
    )

    frame, rejected = importer.collect(["000001.SZ"])

    assert rejected == []
    assert len(frame) == 2
    annual = frame.loc[frame["period_end_date"] == pd.Timestamp("2024-12-31")].iloc[0]
    assert annual["bonus_pre_tax"] == pytest.approx(3.0)
    assert annual["ann_date"] == pd.Timestamp("2025-05-20")
    assert frame["event_id"].is_unique

    legacy = frame.copy()
    legacy["event_id"] = ["legacy-1", "legacy-2"]
    legacy["source_event_id"] = legacy["event_id"]
    migrated = canonicalize_tushare_event_ids(legacy)
    assert migrated["event_id"].tolist() == frame["event_id"].tolist()
    assert migrated["source_event_id"].tolist() == frame["source_event_id"].tolist()


def test_tushare_import_resume_skips_checkpointed_securities(local_root: Path) -> None:
    class _InterruptOnce(_FakeTusharePro):
        def __init__(self) -> None:
            super().__init__()
            self.interrupted = False

        def fund_div(self, **kwargs):
            self.fund_div_calls.append(kwargs)
            if not self.interrupted:
                self.interrupted = True
                raise RuntimeError("simulated network interruption")
            return pd.DataFrame(
                [
                    {
                        "ann_date": "20241201",
                        "div_proc": "实施",
                        "ex_date": "20250106",
                        "div_cash": 0.12,
                    }
                ]
            )

    client = _InterruptOnce()
    importer = TushareCorporateActionImporter(
        client,
        {"000001.SZ": AssetType.STOCK, "510300.SH": AssetType.FUND},
        requests_per_minute=1000000,
        sleep=lambda _: None,
    )
    output = local_root / "actions" / "corporate_actions.parquet"
    checkpoint = local_root / "actions" / "import-state.json"
    with pytest.raises(RuntimeError, match="simulated network interruption"):
        importer.publish_resumable(
            output,
            checkpoint_path=checkpoint,
            checkpoint_every=1,
        )
    assert checkpoint.is_file()
    assert len(client.dividend_calls) == 1

    report = importer.publish_resumable(
        output,
        checkpoint_path=checkpoint,
        checkpoint_every=1,
    )
    assert report.inserted == 2
    assert len(client.dividend_calls) == 1
    assert len(client.fund_div_calls) == 2
    assert not checkpoint.exists()
    assert not checkpoint.with_name(checkpoint.name + ".events.parquet").exists()


def test_tushare_import_retries_transient_network_errors(local_root: Path) -> None:
    class _ResetOnce(_FakeTusharePro):
        def __init__(self) -> None:
            super().__init__()
            self.reset = False

        def dividend(self, **kwargs):
            self.dividend_calls.append(kwargs)
            if not self.reset:
                self.reset = True
                raise ConnectionResetError(10054, "simulated connection reset")
            return super().dividend(**kwargs)

    delays = []
    client = _ResetOnce()
    importer = TushareCorporateActionImporter(
        client,
        {"000001.SZ": AssetType.STOCK},
        requests_per_minute=1000000,
        sleep=delays.append,
        network_max_attempts=3,
        retry_backoff_seconds=1.25,
    )

    frame, rejected = importer.collect(["000001.SZ"])

    assert len(frame) == 1
    assert rejected == ["000001.SZ[1]: stock dividend is not implemented"]
    assert len(client.dividend_calls) == 3
    assert delays == [1.25]


def test_corporate_action_import_cli_is_explicit_and_relative() -> None:
    args = create_parser().parse_args(
        [
            "data",
            "import-corporate-actions",
            "--root",
            "./data/parquet",
            "--output",
            "./data/parquet/corporate_actions.parquet",
            "--start",
            "2025-01-01",
            "--end",
            "2025-01-31",
        ]
    )
    assert args.command == "data"
    assert args.data_command == "import-corporate-actions"
    assert not Path(args.root).is_absolute()


def _add_second_stock_and_legacy_overlap(root: Path) -> None:
    basic_path = root / "stock" / "stock_basic_data.parquet"
    basic = pd.read_parquet(basic_path).reset_index(drop=True)
    basic = pd.concat(
        [
            basic,
            pd.DataFrame(
                [
                    {
                        "ts_code": "000002.SZ",
                        "name": "万科A",
                        "list_date": "19910129",
                        "delist_date": None,
                        "industry": "房地产",
                        "market": "主板",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    _write(basic, basic_path)

    daily_path = root / "stock" / "stock_daily.parquet"
    daily = pd.read_parquet(daily_path).reset_index()
    second = daily.copy()
    second["ts_code"] = "000002.SZ"
    second["open"] += 10.0
    second["high"] += 10.0
    second["low"] += 10.0
    second["close"] += 10.0
    _write(
        pd.concat([daily, second], ignore_index=True),
        daily_path,
        index=["trade_date", "ts_code"],
    )
    _write(
        pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20250102",
                    "open": 999.0,
                    "high": 999.0,
                    "low": 999.0,
                    "close": 999.0,
                    "vol": 1.0,
                    "amount": 1.0,
                    "adj_factor": 1.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20081231",
                    "open": 8.0,
                    "high": 8.5,
                    "low": 7.5,
                    "close": 8.2,
                    "vol": 12.0,
                    "amount": 0.5,
                    "adj_factor": 0.8,
                },
            ]
        ),
        root / "legacy" / "daily_adj_19910101_20250101.parquet",
    )


@pytest.fixture()
def duckdb_generation(local_root: Path, tmp_path: Path):
    _add_second_stock_and_legacy_overlap(local_root)
    _write(_corporate_action_rows(), local_root / "actions" / "corporate_actions.parquet")
    before = {
        path: (path.stat().st_size, path.stat().st_mtime_ns)
        for path in local_root.rglob("*.parquet")
    }
    output = tmp_path / "duckdb"
    builder = DuckDBMaterializer(
        DuckDBBuildConfig(
            source_root=local_root,
            output_root=output,
            temp_directory=tmp_path / "duckdb-temp",
            threads=2,
            memory_limit="1GB",
            max_temp_directory_size="2GB",
            minimum_free_bytes=0,
            source_file_batch_size=1,
        )
    )
    preflight = builder.preflight()
    assert preflight.passed
    manifest = builder.build()
    after = {
        path: (path.stat().st_size, path.stat().st_mtime_ns)
        for path in local_root.rglob("*.parquet")
    }
    assert after == before
    return local_root, output / "current-manifest.json", manifest, builder


def test_duckdb_builder_manifest_precedence_and_safe_rerun(duckdb_generation) -> None:
    local_root, pointer, manifest, builder = duckdb_generation
    assert pointer.is_file()
    assert manifest.validation_passed
    assert {shard.domain for shard in manifest.shards} == {
        "meta",
        "daily",
        "finance",
        "actions",
    }
    assert manifest.dataset_shard("stock_daily").duplicate_counts["stock_daily"] == 0
    assert manifest.dataset_shard("stock_daily").row_counts["stock_daily"] == 5
    stock_shard = manifest.dataset_shard("stock_daily")
    assert stock_shard.null_counts["stock_daily"]["ts_code"] == 0
    assert "TIMESTAMP" in stock_shard.column_types["stock_daily"]["trade_date"]
    assert len(stock_shard.representative_hashes["stock_daily"]) == 64
    assert manifest.build_config["source_file_batch_size"] == 1
    rerun = builder.build()
    assert rerun.generation_id == manifest.generation_id
    assert builder.stale_staging_directories() == ()

    provider = LocalDataProvider({"backend": "duckdb", "path": str(pointer), "query_mode": "batch"})
    modern = provider.get_price(
        "000001.XSHE",
        start_date="2025-01-02",
        end_date="2025-01-02",
        fields=["close"],
        fq=None,
    )
    legacy = provider.get_price(
        "000001.XSHE",
        start_date="2008-12-31",
        end_date="2008-12-31",
        fields=["close"],
        fq=None,
    )
    assert modern.iloc[0]["close"] == pytest.approx(10.5)
    assert legacy.iloc[0]["close"] == pytest.approx(8.2)


def _write_minute_fixture(root: Path) -> None:
    for code, offset in (("000001.SZ", 0.0), ("000002.SZ", 10.0)):
        rows = []
        for when, close in (
            ("2024-12-31 14:59:00", 10.0),
            ("2024-12-31 15:00:00", 10.1),
            ("2025-01-02 09:30:00", 10.2),
            ("2025-01-02 09:31:00", 10.3),
        ):
            timestamp = pd.Timestamp(when)
            rows.append(
                {
                    "ts_code": code,
                    "trade_time": timestamp,
                    "trade_date": timestamp.normalize(),
                    "open": close + offset,
                    "high": close + offset + 0.1,
                    "low": close + offset - 0.1,
                    "close": close + offset,
                    "vol": 100.0,
                    "amount": 1_000.0,
                    "adj_factor": 1.0,
                }
            )
        _write(
            pd.DataFrame(rows),
            root / "stock_1min" / "{}.parquet".format(code),
            index=["trade_date", "trade_time"],
        )


@pytest.fixture()
def duckdb_minute_generation(tmp_path: Path):
    source = tmp_path / "minute-parquet"
    _write_minute_fixture(source)
    output = tmp_path / "minute-duckdb"
    config = DuckDBBuildConfig(
        source_root=source,
        output_root=output,
        temp_directory=tmp_path / "minute-temp",
        threads=2,
        memory_limit="512MB",
        max_temp_directory_size="1GB",
        domains=("minute",),
        datasets=("stock_1m",),
        minimum_free_bytes=0,
        estimated_output_ratio=1.0,
        estimated_temp_ratio=1.0,
        source_file_batch_size=1,
    )
    builder = DuckDBMaterializer(config)
    manifest = builder.build()
    return source, output / "current-manifest.json", manifest, builder


def test_duckdb_minute_builds_year_shards_and_queries_only_overlap(
    duckdb_minute_generation,
) -> None:
    _, pointer, manifest, _ = duckdb_minute_generation
    shards = manifest.dataset_shards("stock_1m")
    assert [shard.partition["value"] for shard in shards] == ["2024", "2025"]
    assert [shard.row_counts["stock_1m"] for shard in shards] == [4, 4]

    backend = DuckDBDataBackend(pointer, threads=1, memory_limit="256MB")
    backend.validate()
    one_year = backend.read_bars_batch(
        BatchBarRequest(
            securities=("000001.SZ", "000002.SZ"),
            asset_type=AssetType.STOCK,
            frequency="1m",
            start=pd.Timestamp("2025-01-01"),
            end=pd.Timestamp("2025-01-03"),
            columns=("close", "vol"),
        )
    )
    assert len(one_year) == 4
    assert backend.diagnostics()["query_statistics"]["query_count"] == 1

    latest = backend.read_bars_batch(
        BatchBarRequest(
            securities=("000001.SZ", "000002.SZ"),
            asset_type=AssetType.STOCK,
            frequency="1m",
            end=pd.Timestamp("2025-01-03"),
            columns=("close",),
            count=3,
        )
    )
    assert latest.groupby("code").size().to_dict() == {
        "000001.SZ": 3,
        "000002.SZ": 3,
    }
    assert latest.groupby("code")["time"].min().eq(pd.Timestamp("2024-12-31 15:00")).all()


def test_duckdb_minute_build_resumes_completed_year_shard(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "minute-parquet"
    _write_minute_fixture(source)
    config = DuckDBBuildConfig(
        source_root=source,
        output_root=tmp_path / "minute-duckdb",
        temp_directory=tmp_path / "minute-temp",
        domains=("minute",),
        datasets=("stock_1m",),
        minimum_free_bytes=0,
        estimated_output_ratio=1.0,
        estimated_temp_ratio=1.0,
        source_file_batch_size=1,
    )
    original = DuckDBMaterializer._build_minute_year_shard
    calls = {"count": 0}

    def fail_after_first(self, *args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 2:
            raise RuntimeError("simulated interruption")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(DuckDBMaterializer, "_build_minute_year_shard", fail_after_first)
    with pytest.raises(RuntimeError, match="simulated interruption"):
        DuckDBMaterializer(config).build()
    staging = next((config.output_root / "generations").glob(".staging-*"))
    completed_path = next(staging.glob("stock_1m_2024_*.duckdb"))
    completed_mtime = completed_path.stat().st_mtime_ns

    monkeypatch.setattr(DuckDBMaterializer, "_build_minute_year_shard", original)
    manifest = DuckDBMaterializer(config).build()
    final_completed = (
        config.output_root / "generations" / manifest.generation_id / completed_path.name
    )
    assert final_completed.stat().st_mtime_ns == completed_mtime
    assert len(manifest.dataset_shards("stock_1m")) == 2


def test_duckdb_minute_supports_explicit_quarter_shards(tmp_path: Path) -> None:
    source = tmp_path / "minute-parquet"
    _write_minute_fixture(source)
    output = tmp_path / "minute-quarter-duckdb"
    manifest = DuckDBMaterializer(
        DuckDBBuildConfig(
            source_root=source,
            output_root=output,
            temp_directory=tmp_path / "minute-quarter-temp",
            domains=("minute",),
            datasets=("stock_1m",),
            minute_quarterly_datasets=("stock_1m",),
            minimum_free_bytes=0,
            estimated_output_ratio=1.0,
            estimated_temp_ratio=1.0,
            source_file_batch_size=1,
        )
    ).build()
    shards = manifest.dataset_shards("stock_1m")
    assert [shard.partition["kind"] for shard in shards] == ["quarter", "quarter"]
    assert [shard.partition["value"] for shard in shards] == ["2024Q4", "2025Q1"]
    assert all(shard.validations["stock_1m"] for shard in shards)

    backend = DuckDBDataBackend(output / "current-manifest.json", threads=1)
    result = backend.read_bars_batch(
        BatchBarRequest(
            securities=("000001.SZ",),
            asset_type=AssetType.STOCK,
            frequency="1m",
            start=pd.Timestamp("2025-01-01"),
            end=pd.Timestamp("2025-01-03"),
            columns=("close",),
        )
    )
    assert len(result) == 2
    assert backend.query_statistics()["query_count"] == 1


def test_duckdb_domain_rebuild_inherits_unaffected_base_shards(
    duckdb_generation,
) -> None:
    local_root, pointer, base, _ = duckdb_generation
    _write_minute_fixture(local_root)
    output = pointer.parent
    combined = DuckDBMaterializer(
        DuckDBBuildConfig(
            source_root=local_root,
            output_root=output,
            base_manifest=pointer,
            temp_directory=output / "compose-temp",
            domains=("minute",),
            datasets=("stock_1m",),
            minute_quarterly_datasets=("stock_1m",),
            minimum_free_bytes=0,
            estimated_output_ratio=1.0,
            estimated_temp_ratio=1.0,
            source_file_batch_size=1,
        )
    ).build(publish=False)
    assert {shard.domain for shard in combined.shards} == {
        "meta",
        "daily",
        "finance",
        "actions",
        "minute",
    }
    assert combined.build_config["base_generation_id"] == base.generation_id
    inherited_meta = combined.dataset_shard("stock_basic")
    old_meta = base.dataset_shard("stock_basic")
    old_path = pointer.parent / "generations" / base.generation_id / old_meta.path
    new_path = output / "generations" / combined.generation_id / inherited_meta.path
    assert old_path.samefile(new_path)

    provider = LocalDataProvider(
        {
            "backend": "duckdb",
            "path": str(output / "generations" / combined.generation_id / "manifest.json"),
            "query_mode": "batch",
        }
    )
    result = provider.get_price(
        ["000001.XSHE", "000002.XSHE"],
        start_date="2025-01-02 09:30:00",
        end_date="2025-01-02 09:31:00",
        frequency="1m",
        fields=["close", "volume"],
        fq=None,
        fill_paused=False,
        panel=False,
    )
    assert len(result) == 4
    assert result.groupby("code").size().to_dict() == {
        "000001.XSHE": 2,
        "000002.XSHE": 2,
    }


@pytest.mark.parametrize("query_mode", ["scalar", "batch"])
def test_duckdb_three_way_public_price_parity(duckdb_generation, query_mode: str) -> None:
    local_root, pointer, _, _ = duckdb_generation
    securities = ["000001.XSHE", "000002.XSHE"]
    kwargs = {
        "start_date": "2025-01-02",
        "end_date": "2025-01-06",
        "fields": ["open", "close", "volume", "money", "paused"],
        "fq": None,
        "panel": False,
    }
    parquet = LocalDataProvider(
        {"backend": "parquet", "path": str(local_root), "query_mode": "scalar"}
    ).get_price(securities, **kwargs)
    duckdb = LocalDataProvider(
        {"backend": "duckdb", "path": str(pointer), "query_mode": query_mode}
    ).get_price(securities, **kwargs)

    pd.testing.assert_frame_equal(duckdb, parquet, check_dtype=True, check_exact=True)


def test_duckdb_backtest_preflight_rejects_missing_frequency_shard(
    duckdb_generation,
) -> None:
    _, pointer, _, _ = duckdb_generation
    provider = LocalDataProvider(
        {
            "backend": "duckdb",
            "path": str(pointer),
            "require_corporate_actions": False,
        }
    )

    with pytest.raises(LocalDataMissingShardError, match="stock_1m"):
        provider.preflight_backtest(
            start_date="2025-01-02", end_date="2025-01-06", frequency="minute"
        )


def test_duckdb_native_batch_count_and_query_bound(duckdb_generation) -> None:
    _, pointer, _, _ = duckdb_generation
    backend = DuckDBDataBackend(pointer, threads=2, memory_limit="1GB")
    backend.validate()
    backend.reset_query_statistics()
    result = backend.read_bars_batch(
        BatchBarRequest(
            securities=("000001.SZ", "000002.SZ"),
            asset_type=AssetType.STOCK,
            frequency="1d",
            end=pd.Timestamp("2025-01-06"),
            columns=("close", "vol"),
            count=1,
        )
    )

    assert result.groupby("code").size().to_dict() == {"000001.SZ": 1, "000002.SZ": 1}
    assert backend.query_statistics()["query_count"] == 1
    assert backend.batch_capabilities()["bars"] == "native"
    diagnostics = backend.diagnostics()
    assert diagnostics["fully_materialized"] is True
    assert diagnostics["parquet_fallback"] is False


def test_provider_batch_reuses_one_trade_calendar(duckdb_generation) -> None:
    _, pointer, _, _ = duckdb_generation
    provider = LocalDataProvider({"backend": "duckdb", "path": str(pointer), "query_mode": "batch"})
    kwargs = {
        "security": ["000001.XSHE", "000002.XSHE"],
        "start_date": "2025-01-02",
        "end_date": "2025-01-06",
        "fields": ["close", "volume", "paused"],
        "fq": None,
        "panel": False,
    }
    provider.get_price(**kwargs)
    provider._backend.reset_query_statistics()

    result = provider.get_price(**kwargs)

    assert len(result) == 6
    assert provider._backend.query_statistics()["query_count"] == 2


def test_duckdb_arrow_result_survives_cursor_and_connection_close(duckdb_generation) -> None:
    _, pointer, _, _ = duckdb_generation
    backend = DuckDBDataBackend(pointer, threads=2, memory_limit="1GB")
    request = BatchBarRequest(
        securities=("000001.SZ", "000002.SZ"),
        asset_type=AssetType.STOCK,
        frequency="1d",
        columns=("close",),
        count=1,
    )
    arrow = backend.read_bars_batch_arrow(request)
    backend.close()
    frame = arrow.to_pandas()

    assert arrow.num_rows == 2
    assert set(frame["code"]) == {"000001.SZ", "000002.SZ"}


def test_duckdb_catalog_index_finance_and_actions_parity(duckdb_generation) -> None:
    local_root, pointer, _, _ = duckdb_generation
    parquet = LocalDataProvider({"backend": "parquet", "path": str(local_root)})
    duckdb = LocalDataProvider({"backend": "duckdb", "path": str(pointer)})

    pd.testing.assert_frame_equal(
        duckdb.get_all_securities(["stock", "fund", "index"]),
        parquet.get_all_securities(["stock", "fund", "index"]),
    )
    assert duckdb.get_index_stocks("000300.XSHG", "2025-01-02") == parquet.get_index_stocks(
        "000300.XSHG", "2025-01-02"
    )
    assert duckdb.get_split_dividend(
        "000001.XSHE", "2025-01-01", "2025-01-06"
    ) == parquet.get_split_dividend("000001.XSHE", "2025-01-01", "2025-01-06")

    jq = pytest.importorskip("jqdatasdk")
    query = jq.query(jq.income.code, jq.income.net_profit).filter(
        jq.income.code.in_(["000001.XSHE"])
    )
    pd.testing.assert_frame_equal(
        duckdb.get_fundamentals(query, date="2025-04-20"),
        parquet.get_fundamentals(query, date="2025-04-20"),
    )


def test_duckdb_builder_cli_configuration_is_relative() -> None:
    args = create_parser().parse_args(
        [
            "data",
            "build-duckdb",
            "--source",
            "./data/parquet",
            "--output",
            "./data/duckdb",
            "--dry-run",
        ]
    )
    assert args.data_command == "build-duckdb"
    assert not Path(args.source).is_absolute()
    assert not Path(args.output).is_absolute()


@pytest.mark.parametrize("fq", ["pre", "post"])
def test_duckdb_three_way_adjustment_and_dtype_parity(duckdb_generation, fq: str) -> None:
    local_root, pointer, _, _ = duckdb_generation
    kwargs = {
        "start_date": "2025-01-02",
        "end_date": "2025-01-06",
        "fields": ["open", "high", "low", "close", "volume", "money", "paused"],
        "fq": fq,
        "pre_factor_ref_date": "2025-01-06",
    }
    expected = LocalDataProvider(
        {"backend": "parquet", "path": str(local_root), "query_mode": "scalar"}
    ).get_price("000001.XSHE", **kwargs)
    scalar = LocalDataProvider(
        {"backend": "duckdb", "path": str(pointer), "query_mode": "scalar"}
    ).get_price("000001.XSHE", **kwargs)
    batch = LocalDataProvider(
        {"backend": "duckdb", "path": str(pointer), "query_mode": "batch"}
    ).get_price("000001.XSHE", **kwargs)

    pd.testing.assert_frame_equal(scalar, expected, check_dtype=True, check_exact=True)
    pd.testing.assert_frame_equal(batch, expected, check_dtype=True, check_exact=True)


def test_duckdb_manifest_errors_are_typed_and_preflight_preserves_active(
    duckdb_generation, tmp_path: Path
) -> None:
    local_root, pointer, manifest, _ = duckdb_generation
    pointer_before = pointer.read_bytes()
    failing = DuckDBMaterializer(
        DuckDBBuildConfig(
            source_root=local_root,
            output_root=pointer.parent,
            temp_directory=tmp_path / "too-small-temp",
            minimum_free_bytes=10**30,
        )
    )
    assert not failing.preflight().passed
    with pytest.raises(LocalDataConfigurationError, match="预检失败"):
        failing.build()
    assert pointer.read_bytes() == pointer_before

    generation_manifest = pointer.parent / "generations" / manifest.generation_id / "manifest.json"
    payload = json.loads(generation_manifest.read_text(encoding="utf-8"))
    incompatible = tmp_path / "incompatible-manifest.json"
    payload["schema_version"] = "999.0.0"
    incompatible.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(LocalDataIncompatibleGenerationError, match="不兼容"):
        DuckDBDataBackend(incompatible).validate()

    payload["schema_version"] = CANONICAL_SCHEMA_VERSION
    payload["shards"][0]["path"] = "missing-shard.duckdb"
    missing = tmp_path / "missing-shard-manifest.json"
    missing.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(LocalDataMissingShardError, match="缺少分片"):
        DuckDBDataBackend(missing).validate()


class _GenerationActionProvider:
    name = "local"

    def __init__(self) -> None:
        self.generation = "generation-a"
        self.calls = 0

    def diagnostics(self):
        return {"manifest_identity": self.generation}

    def get_split_dividend_batch(self, securities, start_date=None, end_date=None):
        self.calls += 1
        return {
            code: [
                {
                    "security": code,
                    "date": pd.Timestamp("2025-01-03").date(),
                    "security_type": "stock",
                    "scale_factor": 1.0,
                    "bonus_pre_tax": 1.0,
                    "per_base": 10.0,
                }
            ]
            for code in securities
        }


def test_engine_batch_event_calendar_cache_is_generation_isolated(monkeypatch) -> None:
    provider = _GenerationActionProvider()
    monkeypatch.setattr(data_api, "_provider", provider)
    engine = BacktestEngine(initialize=lambda context: None)
    codes = ["000001.XSHE", "000002.XSHE"]
    start = pd.Timestamp("2025-01-01").date()
    end = pd.Timestamp("2025-01-03").date()

    first = engine._load_corporate_actions_batch(codes, start, end)
    second = engine._load_corporate_actions_batch(codes, start, end)
    provider.generation = "generation-b"
    third = engine._load_corporate_actions_batch(codes, start, end)

    assert first == second == third
    assert provider.calls == 2
