"""Adapter from JoinQuant/SQLAlchemy fundamentals queries to local tables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from .base import AssetType, LocalDataBackend, LocalDataQueryError


@dataclass(frozen=True)
class QueryField:
    table: str
    output_name: str
    source_name: str
    expression: Any


_TABLE_NAMES = {
    "stock_valuation": "valuation",
    "valuation": "valuation",
    "income_statement_day": "income",
    "income_statement": "income",
    "balance_sheet_day": "balance",
    "balance_sheet": "balance",
    "cash_flow_statement_day": "cash_flow",
    "cash_flow_statement": "cash_flow",
    "financial_indicator_day": "indicator",
    "financial_indicator": "indicator",
}

_FIELD_MAP: Dict[str, Dict[str, str]] = {
    "valuation": {
        "code": "ts_code",
        "display_name": "__metadata__",
        "market_cap": "total_mv",
        "circulating_market_cap": "circ_mv",
        "pe_ratio": "pe",
        "pe_ratio_lyr": "pe_ttm",
        "pb_ratio": "pb",
        "ps_ratio": "ps",
        "turnover_ratio": "turnover_rate",
        "capitalization": "total_share",
        "circulating_cap": "float_share",
    },
    "income": {
        "code": "ts_code",
        "statDate": "end_date",
        "pubDate": "ann_date",
        "basic_eps": "basic_eps",
        "diluted_eps": "diluted_eps",
        "operating_revenue": "revenue",
        "total_operating_revenue": "total_revenue",
        "operating_cost": "oper_cost",
        "total_operating_cost": "total_cogs",
        "operating_profit": "operate_profit",
        "total_profit": "total_profit",
        "income_tax_expense": "income_tax",
        "net_profit": "n_income",
        "net_profit_to_parent": "n_income_attr_p",
        "np_parent_company_owners": "n_income_attr_p",
    },
    "balance": {
        "code": "ts_code",
        "statDate": "end_date",
        "pubDate": "ann_date",
        "cash_equivalents": "money_cap",
        "accounts_receivable": "accounts_receiv",
        "inventories": "inventories",
        "total_current_assets": "total_cur_assets",
        "fixed_assets": "fix_assets",
        "total_assets": "total_assets",
        "shortterm_loan": "st_borr",
        "accounts_payable": "acct_payable",
        "total_current_liability": "total_cur_liab",
        "longterm_loan": "lt_borr",
        "total_liability": "total_liab",
        "paidin_capital": "total_share",
        "capital_reserve_fund": "cap_rese",
        "retained_profit": "undistr_porfit",
        "total_owner_equities": "total_hldr_eqy_inc_min_int",
        "total_sheet_owner_equities": "total_hldr_eqy_inc_min_int",
    },
    "cash_flow": {
        "code": "ts_code",
        "statDate": "end_date",
        "pubDate": "ann_date",
        "net_operate_cash_flow": "n_cashflow_act",
        "net_invest_cash_flow": "n_cashflow_inv_act",
        "net_finance_cash_flow": "n_cash_flows_fnc_act",
        "cash_equivalent_increase": "n_incr_cash_cash_equ",
        "goods_sale_and_service_render_cash": "c_fr_sale_sg",
        "cash_paid_for_goods_and_services": "c_paid_goods_s",
    },
    "indicator": {
        "code": "ts_code",
        "statDate": "end_date",
        "pubDate": "ann_date",
        "eps": "eps",
        "adjusted_profit": "profit_dedt",
        "roe": "roe",
        "roa": "roa",
        "net_profit_margin": "netprofit_margin",
        "gross_profit_margin": "grossprofit_margin",
        "inc_revenue_year_on_year": "or_yoy",
        "inc_net_profit_year_on_year": "q_profit_yoy",
        "current_ratio": "current_ratio",
        "quick_ratio": "quick_ratio",
        "cash_ratio": "cash_ratio",
    },
}


class FundamentalsQueryAdapter:
    """Execute the supported, deterministic subset of JoinQuant queries."""

    def __init__(
        self,
        backend: LocalDataBackend,
        *,
        to_ts_code: Callable[[str], str],
        to_jq_code: Callable[[str], str],
    ) -> None:
        self.backend = backend
        self.to_ts_code = to_ts_code
        self.to_jq_code = to_jq_code

    @staticmethod
    def _table_for_expression(expression: Any) -> str:
        table = getattr(expression, "table", None)
        table_name = getattr(table, "name", None)
        normalized = _TABLE_NAMES.get(str(table_name or "").lower())
        if normalized is None:
            raise LocalDataQueryError(
                "LocalDataProvider 暂不支持基本面表：{}".format(table_name or expression)
            )
        return normalized

    @classmethod
    def _field_for_expression(
        cls, expression: Any, output_name: Optional[str] = None
    ) -> QueryField:
        table = cls._table_for_expression(expression)
        name = output_name or getattr(expression, "name", None)
        if not name:
            raise LocalDataQueryError("基本面查询只支持明确的字段列，不支持整表实体")
        mapping = _FIELD_MAP[table]
        source = mapping.get(str(name))
        if source is None and str(name) in mapping.values():
            source = str(name)
        if source is None:
            raise LocalDataQueryError(
                "本地 {} 表没有 JoinQuant 字段 {!r} 的稳定映射".format(table, name)
            )
        return QueryField(table, str(name), source, expression)

    @classmethod
    def _selected_fields(cls, query: Any) -> List[QueryField]:
        descriptions = getattr(query, "column_descriptions", None)
        if not descriptions:
            raise LocalDataQueryError("无法解析基本面 query 的 column_descriptions")
        fields: List[QueryField] = []
        for description in descriptions:
            expression = description.get("expr") if isinstance(description, dict) else None
            output_name = description.get("name") if isinstance(description, dict) else None
            fields.append(cls._field_for_expression(expression, output_name))
        return fields

    @classmethod
    def _iter_column_expressions(cls, node: Any) -> Iterable[Any]:
        if node is None:
            return
        if getattr(node, "table", None) is not None and getattr(node, "name", None):
            yield node
            return
        for attribute in ("element", "left", "right"):
            child = getattr(node, attribute, None)
            if child is not None and child is not node:
                yield from cls._iter_column_expressions(child)
        clauses = getattr(node, "clauses", None)
        if clauses is not None:
            for clause in clauses:
                yield from cls._iter_column_expressions(clause)

    @classmethod
    def _filter_fields(cls, where_clause: Any) -> List[QueryField]:
        fields: Dict[Tuple[str, str], QueryField] = {}
        for expression in cls._iter_column_expressions(where_clause):
            field = cls._field_for_expression(expression)
            fields[(field.table, field.output_name)] = field
        return list(fields.values())

    @staticmethod
    def _bound_value(node: Any) -> Any:
        if node is None:
            return None
        if hasattr(node, "value"):
            return node.value
        element = getattr(node, "element", None)
        if element is not None and element is not node:
            return FundamentalsQueryAdapter._bound_value(element)
        clauses = getattr(node, "clauses", None)
        if clauses is not None:
            return [FundamentalsQueryAdapter._bound_value(clause) for clause in clauses]
        return node

    @classmethod
    def _extract_codes(cls, node: Any) -> Optional[List[str]]:
        if node is None:
            return None
        operator_name = getattr(getattr(node, "operator", None), "__name__", "")
        left = getattr(node, "left", None)
        if getattr(left, "name", None) == "code" and operator_name in ("in_op", "eq"):
            value = cls._bound_value(getattr(node, "right", None))
            if operator_name == "eq":
                value = [value]
            if value is not None:
                return [str(item) for item in value]
        collected: List[str] = []
        clauses = getattr(node, "clauses", None)
        if clauses is not None:
            for clause in clauses:
                values = cls._extract_codes(clause)
                if values:
                    collected.extend(values)
        return list(dict.fromkeys(collected)) or None

    @classmethod
    def _evaluate(cls, node: Any, frame: pd.DataFrame) -> pd.Series:
        if node is None:
            return pd.Series(True, index=frame.index)
        operator_name = getattr(getattr(node, "operator", None), "__name__", "")
        clauses = getattr(node, "clauses", None)
        if clauses is not None and operator_name in ("and_", "or_"):
            masks = [cls._evaluate(clause, frame) for clause in clauses]
            if not masks:
                return pd.Series(True, index=frame.index)
            result = masks[0]
            for mask in masks[1:]:
                result = result & mask if operator_name == "and_" else result | mask
            return result

        left = getattr(node, "left", None)
        field_name = getattr(left, "name", None)
        if field_name is None:
            element = getattr(node, "element", None)
            if element is not None and element is not node:
                mask = cls._evaluate(element, frame)
                return ~mask if operator_name in ("inv", "not_") else mask
            raise LocalDataQueryError("不支持的基本面过滤表达式：{}".format(node))
        if field_name not in frame.columns:
            raise LocalDataQueryError("过滤字段 {!r} 未出现在本地查询结果中".format(field_name))
        series = frame[field_name]
        value = cls._bound_value(getattr(node, "right", None))
        operations = {
            "eq": lambda: series == value,
            "ne": lambda: series != value,
            "gt": lambda: series > value,
            "ge": lambda: series >= value,
            "lt": lambda: series < value,
            "le": lambda: series <= value,
            "in_op": lambda: series.isin(list(value)),
            "not_in_op": lambda: ~series.isin(list(value)),
            "is_": lambda: series.isna() if value is None else series == value,
            "is_not": lambda: series.notna() if value is None else series != value,
        }
        operation = operations.get(operator_name)
        if operation is None:
            raise LocalDataQueryError(
                "LocalDataProvider 暂不支持过滤操作符：{}".format(operator_name or node)
            )
        try:
            return operation().fillna(False)
        except (TypeError, ValueError) as exc:
            raise LocalDataQueryError("基本面过滤失败：{}".format(exc)) from exc

    @staticmethod
    def _scale_valuation(frame: pd.DataFrame) -> pd.DataFrame:
        # Tushare total_mv/circ_mv use 万元; JoinQuant uses 亿元.
        for column in ("market_cap", "circulating_market_cap"):
            if column in frame:
                frame[column] = pd.to_numeric(frame[column], errors="coerce") / 10000.0
        return frame

    def execute(
        self,
        query: Any,
        *,
        as_of: pd.Timestamp,
        stat_date: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        selected = self._selected_fields(query)
        where_clause = getattr(query, "whereclause", None)
        filter_fields = self._filter_fields(where_clause)
        all_fields: Dict[Tuple[str, str], QueryField] = {
            (field.table, field.output_name): field for field in selected + filter_fields
        }
        raw_codes = self._extract_codes(where_clause)
        ts_codes = [self.to_ts_code(code) for code in raw_codes] if raw_codes else None

        by_table: Dict[str, List[QueryField]] = {}
        for field in all_fields.values():
            by_table.setdefault(field.table, []).append(field)

        table_frames: List[pd.DataFrame] = []
        for table, fields in by_table.items():
            source_columns = [
                field.source_name for field in fields if field.source_name != "__metadata__"
            ]
            source_columns.append("ts_code")
            local = self.backend.read_financial_table(
                table,
                source_columns,
                as_of=as_of,
                stat_date=stat_date,
                securities=ts_codes,
            )
            if local.empty:
                table_frames.append(
                    pd.DataFrame(columns=["code"] + [f.output_name for f in fields])
                )
                continue
            renamed = pd.DataFrame(index=local.index)
            renamed["code"] = local["ts_code"].map(self.to_jq_code)
            for field in fields:
                if field.output_name == "code":
                    continue
                if field.source_name in local:
                    renamed[field.output_name] = local[field.source_name]
            if table == "valuation":
                renamed = self._scale_valuation(renamed)
            table_frames.append(renamed.drop_duplicates("code", keep="last"))

        if not table_frames:
            return pd.DataFrame(columns=[field.output_name for field in selected])
        frame = table_frames[0]
        for other in table_frames[1:]:
            frame = frame.merge(other, on="code", how="inner", suffixes=("", "__duplicate"))
            duplicate_columns = [column for column in frame if column.endswith("__duplicate")]
            frame = frame.drop(columns=duplicate_columns)

        if any(field.output_name == "display_name" for field in selected + filter_fields):
            securities = self.backend.read_securities((AssetType.STOCK,), as_of)
            names = securities["display_name"].to_dict() if "display_name" in securities else {}
            frame["display_name"] = frame["code"].map(
                lambda code: names.get(self.to_ts_code(code), code)
            )

        if where_clause is not None and not frame.empty:
            frame = frame[self._evaluate(where_clause, frame)]

        order_clauses = list(getattr(query, "_order_by_clauses", ()) or ())
        if order_clauses and not frame.empty:
            order_columns: List[str] = []
            ascending: List[bool] = []
            for clause in order_clauses:
                element = getattr(clause, "element", clause)
                name = getattr(element, "name", None)
                if not name or name not in frame:
                    raise LocalDataQueryError("无法解析基本面排序字段：{}".format(clause))
                modifier = getattr(getattr(clause, "modifier", None), "__name__", "asc_op")
                order_columns.append(str(name))
                ascending.append(modifier != "desc_op")
            frame = frame.sort_values(order_columns, ascending=ascending)

        limit_clause = getattr(query, "_limit_clause", None)
        if limit_clause is not None:
            limit = self._bound_value(limit_clause)
            if limit is not None:
                frame = frame.head(int(limit))
        output_columns = [field.output_name for field in selected]
        return frame.reindex(columns=output_columns).reset_index(drop=True)
