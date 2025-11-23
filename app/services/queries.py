from __future__ import annotations

from typing import Any, List, Mapping, Sequence, Tuple

import duckdb

from app.config import settings


def _view_name(upload_id: int) -> str:
    return f"uploads.upload_{upload_id}"


def _connect():
    return duckdb.connect(str(settings.duckdb_path))


def _build_filters(search: str | None, filters: Mapping[str, Any] | None) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []
    if search:
        clauses.append(
            "(lower(CustomerName) LIKE '%' || lower(?) || '%' OR lower(ProductName) LIKE '%' || lower(?) || '%')"
        )
        params.extend([search, search])
    if filters:
        for column, value in filters.items():
            clauses.append(f"lower({column}) = lower(?)")
            params.append(str(value))
    where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, params


def fetch_data_page(
    upload_id: int,
    *,
    limit: int | None = 100,
    offset: int = 0,
    forex: float = 1.0,
    margin: float = 1.0,
    vat: float | None = None,
    search: str | None = None,
    filters: Mapping[str, Any] | None = None,
    columns: Sequence[str] | None = None,
) -> dict:
    vat = vat or settings.default_vat
    margin_safe = margin if margin else 1.0
    table = _view_name(upload_id)

    base_columns: List[str]
    if columns:
        exclusions = {
            "forex",
            "pretaxwithforex",
            "margin",
            "totalvatex",
            "totalvatinc",
            "vat",
        }
        base_columns = [col for col in columns if col.lower() not in exclusions]
        base_columns = base_columns or ["*"]
    else:
        base_columns = ["*"]

    select_parts = list(base_columns)
    select_parts.extend(
        [
            "? AS Forex",
            "CAST(PricingPreTaxTotal AS DOUBLE) * ? AS PreTaxWithForex",
            "? AS Margin",
            "(CAST(PricingPreTaxTotal AS DOUBLE) * ?) / ? AS TotalVATEx",
            "? AS VAT",
            "((CAST(PricingPreTaxTotal AS DOUBLE) * ?) / ?) * ? AS TotalVATInc",
        ]
    )

    select_sql = ",\n            ".join(select_parts)

    query_lines = [
        f"SELECT\n            {select_sql}\n        FROM {table}",
    ]

    params: List[Any] = [
        forex,
        forex,
        margin_safe,
        forex,
        margin_safe,
        vat,
        forex,
        margin_safe,
        vat,
    ]

    where_sql, where_params = _build_filters(search, filters)
    if where_sql:
        query_lines.append(where_sql)

    params.extend(where_params)

    query_lines.append("ORDER BY UsageDate DESC NULLS LAST")

    limit_value = limit if limit and limit > 0 else None
    offset_value = offset if limit_value else 0

    if limit_value:
        query_lines.append("LIMIT ? OFFSET ?")
        params.extend([limit_value, offset_value])

    count_query = f"SELECT COUNT(*) FROM {table}{where_sql}"
    count_params: List[Any] = list(where_params)

    with _connect() as con:
        rows = con.execute("\n".join(query_lines), params).fetchall()
        columns_out = [desc[0] for desc in con.description]
        total = con.execute(count_query, count_params).fetchone()[0]

    records = [dict(zip(columns_out, row)) for row in rows]
    return {"records": records, "total": int(total or 0)}


def summarize_upload(
    upload_id: int,
    *,
    forex: float,
    margin: float,
    vat: float,
    search: str | None = None,
    filters: Mapping[str, Any] | None = None,
) -> dict:
    table = _view_name(upload_id)
    where_sql, where_params = _build_filters(search, filters)
    query = """
        SELECT
            COALESCE(SUM(CAST(PricingPreTaxTotal AS DOUBLE)), 0) AS total_pricing,
            COALESCE(SUM(CAST(BillingPreTaxTotal AS DOUBLE)), 0) AS total_billing,
            COUNT(*) AS total_records
        FROM {table}{where_sql}
    """.format(table=table, where_sql=where_sql)

    with _connect() as con:
        total_pricing, total_billing, total_records = con.execute(query, where_params).fetchone()

    total_pricing = float(total_pricing or 0.0) * forex
    margin = margin if margin else 1.0
    vat = vat if vat else settings.default_vat
    total_vat_ex = total_pricing / margin if margin else total_pricing
    total_vat_inc = total_vat_ex * vat

    return {
        "total_pricing": total_pricing,
        "total_billing": float(total_billing or 0.0),
        "total_records": int(total_records or 0),
        "total_vat_ex": total_vat_ex,
        "total_vat_inc": total_vat_inc,
    }


def top_customers(
    upload_id: int,
    limit: int = 10,
    *,
    search: str | None = None,
    filters: Mapping[str, Any] | None = None,
) -> Sequence[Mapping[str, Any]]:
    table = _view_name(upload_id)
    where_sql, where_params = _build_filters(search, filters)
    query = """
        SELECT CustomerName, SUM(CAST(PricingPreTaxTotal AS DOUBLE)) AS TotalCost
        FROM {table}
        {where_sql}
        GROUP BY CustomerName
        ORDER BY TotalCost DESC NULLS LAST
        LIMIT ?
    """.format(table=table, where_sql=where_sql)

    with _connect() as con:
        params = list(where_params) + [limit]
        rows = con.execute(query, params).fetchall()
    return [
        {
            "label": row[0] or "Unknown",
            "value": float(row[1] or 0.0),
        }
        for row in rows
    ]


def list_invoices(
    upload_id: int,
    *,
    limit: int | None = 200,
    search: str | None = None,
    filters: Mapping[str, Any] | None = None,
) -> Sequence[str]:
    table = _view_name(upload_id)
    filters = dict(filters or {})
    where_sql, where_params = _build_filters(search, filters)
    if where_sql:
        invoice_where = f"{where_sql} AND TRIM(COALESCE(InvoiceNumber, '')) <> ''"
    else:
        invoice_where = " WHERE TRIM(COALESCE(InvoiceNumber, '')) <> ''"

    query = """
        SELECT DISTINCT InvoiceNumber
        FROM {table}
        {where_sql}
        ORDER BY InvoiceNumber
    """.format(table=table, where_sql=invoice_where)

    params = list(where_params)
    if limit and limit > 0:
        query += " LIMIT ?"
        params.append(limit)

    with _connect() as con:
        rows = con.execute(query, params).fetchall()

    return [row[0] for row in rows if row and row[0]]


def invoice_details(
    upload_id: int,
    *,
    filters: Mapping[str, Any],
    search: str | None = None,
    forex: float | None = None,
    margin: float | None = None,
    vat: float | None = None,
) -> Mapping[str, Any]:
    table = _view_name(upload_id)
    where_sql, where_params = _build_filters(search, filters)
    forex_rate = float(forex) if forex and forex > 0 else 1.0
    margin_rate = float(margin) if margin and margin > 0 else 1.0
    vat_rate = float(vat) if vat and vat > 0 else settings.default_vat

    details_query = """
        SELECT
            MeterCategory,
            MeterSubCategory,
            MeterName,
            MeterType,
            Unit,
            COALESCE(SUM(TRY_CAST(Quantity AS DOUBLE)), 0) AS Quantity,
            CASE
                WHEN SUM(TRY_CAST(Quantity AS DOUBLE)) = 0 THEN 0
                ELSE SUM(TRY_CAST(PricingPreTaxTotal AS DOUBLE)) / SUM(TRY_CAST(Quantity AS DOUBLE))
            END AS UnitPrice,
            COALESCE(SUM(TRY_CAST(PricingPreTaxTotal AS DOUBLE)), 0) AS PricingPreTaxTotal,
            COALESCE(SUM(TRY_CAST(BillingPreTaxTotal AS DOUBLE)), 0) AS BillingPreTaxTotal
        FROM {table}
        {where_sql}
        GROUP BY MeterCategory, MeterSubCategory, MeterName, MeterType, Unit
        ORDER BY MeterCategory, MeterSubCategory, MeterName
    """.format(table=table, where_sql=where_sql)

    totals_query = """
        SELECT
            COALESCE(SUM(TRY_CAST(Quantity AS DOUBLE)), 0) AS TotalQuantity,
            CASE
                WHEN SUM(TRY_CAST(Quantity AS DOUBLE)) = 0 THEN 0
                ELSE SUM(TRY_CAST(PricingPreTaxTotal AS DOUBLE)) / SUM(TRY_CAST(Quantity AS DOUBLE))
            END AS WeightedUnitPrice,
            COALESCE(SUM(TRY_CAST(PricingPreTaxTotal AS DOUBLE)), 0) AS TotalPreTax,
            COALESCE(SUM(TRY_CAST(BillingPreTaxTotal AS DOUBLE)), 0) AS TotalBilling
        FROM {table}
        {where_sql}
    """.format(table=table, where_sql=where_sql)

    period_query = """
        SELECT
            MIN(COALESCE(UsageDate, ChargeStartDate)) AS PeriodStart,
            MAX(COALESCE(UsageDate, ChargeEndDate)) AS PeriodEnd
        FROM {table}
        {where_sql}
    """.format(table=table, where_sql=where_sql)

    with _connect() as con:
        rows = con.execute(details_query, where_params).fetchall()
        totals_row = con.execute(totals_query, where_params).fetchone()
        period_row = con.execute(period_query, where_params).fetchone()

    items = [
        {
            "meter_category": row[0],
            "meter_sub_category": row[1],
            "meter_name": row[2],
            "meter_type": row[3],
            "unit": row[4],
            "quantity": float(row[5] or 0.0),
            "unit_price": float(row[6] or 0.0),
            "pricing_pretax_total": float(row[7] or 0.0),
            "billing_pretax_total": float(row[8] or 0.0),
        }
        for row in rows
    ]

    total_quantity = float(totals_row[0] or 0.0) if totals_row else 0.0
    total_unit_price = float(totals_row[1] or 0.0) if totals_row else 0.0
    total_pricing = float(totals_row[2] or 0.0) if totals_row else 0.0
    total_billing = float(totals_row[3] or 0.0) if totals_row else 0.0

    for item in items:
        pricing_total = item["pricing_pretax_total"]
        pretax_forex = pricing_total * forex_rate
        vat_ex = pretax_forex / margin_rate if margin_rate else pretax_forex
        item["total_vat_inc"] = vat_ex * vat_rate

    pretax_forex_total = total_pricing * forex_rate
    total_vat_ex = pretax_forex_total / margin_rate if margin_rate else pretax_forex_total
    total_vat_inc = total_vat_ex * vat_rate

    period_start = period_row[0] if period_row else None
    period_end = period_row[1] if period_row else None

    return {
        "items": items,
        "total_quantity": total_quantity,
        "total_unit_price": total_unit_price,
        "total_pricing": total_pricing,
        "total_billing": total_billing,
        "total_vat_inc": total_vat_inc,
        "period_start": period_start,
        "period_end": period_end,
    }
