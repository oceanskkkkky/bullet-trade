from pathlib import Path

import pandas as pd
import pytest

from bullet_trade.data.local import LocalDataConfigurationError, LocalDataSchemaError
from bullet_trade.data.api import _create_provider, _normalize_provider_name
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
                    "con_codes": ["000001.SZ"],
                    "weights": [3.5],
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
    assert provider.get_index_stocks("000300.XSHG", date="2025-01-02") == ["000001.XSHE"]
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
    query = jq.query(jq.income.code, jq.income.net_profit).filter(
        jq.income.code.in_(["000001.XSHE"])
    )

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
