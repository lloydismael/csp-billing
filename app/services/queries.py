from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, List, Mapping, Sequence, Tuple

import duckdb

from app.config import settings


def _decimal_or_default(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def _decimal_to_string(value: Decimal, *, minimum_fraction_digits: int = 0) -> str:
    text = format(value, "f")
    if "." not in text:
        if minimum_fraction_digits > 0:
            return f"{text}.{'0' * minimum_fraction_digits}"
        return text

    integer, fraction = text.split(".", 1)
    fraction = fraction.rstrip("0")
    if fraction:
        if minimum_fraction_digits > 0 and len(fraction) < minimum_fraction_digits:
            fraction = fraction.ljust(minimum_fraction_digits, "0")
        return f"{integer}.{fraction}"

    if minimum_fraction_digits > 0:
        return f"{integer}.{'0' * minimum_fraction_digits}"
    return integer


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


def _build_exempt_clause(column: str = "CustomerName") -> Tuple[str, List[Any]]:
    if not settings.vat_exempt_customers:
        return "FALSE", []

    conditions = []
    params = []
    for cust in settings.vat_exempt_customers:
        conditions.append(f"lower({column}) LIKE lower(?)")
        params.append(f"%{cust}%")

    return f"({' OR '.join(conditions)})", params


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
    vat = settings.default_vat if vat is None else vat
    vat = max(float(vat), 0.0)
    vat_multiplier = 1.0 if vat == 0 else vat
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

    exempt_sql, exempt_params = _build_exempt_clause()
    
    vat_display_expr = f"CASE WHEN {exempt_sql} THEN 0.0 ELSE ? END"
    vat_multiplier_expr = f"CASE WHEN {exempt_sql} THEN 1.0 ELSE ? END"

    select_parts = list(base_columns)
    select_parts.extend(
        [
            "? AS Forex",
            "CAST(PricingPreTaxTotal AS DOUBLE) * ? AS PreTaxWithForex",
            "? AS Margin",
            "(CAST(PricingPreTaxTotal AS DOUBLE) * ?) / ? AS TotalVATEx",
            f"{vat_display_expr} AS VAT",
            f"((CAST(PricingPreTaxTotal AS DOUBLE) * ?) / ?) * ({vat_multiplier_expr}) AS TotalVATInc",
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
    ]
    
    # Params for VAT column
    params.extend(exempt_params)
    params.append(vat)
    
    # Params for TotalVATInc column
    params.append(forex)
    params.append(margin_safe)
    params.extend(exempt_params)
    params.append(vat_multiplier)

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

    exempt_sql, exempt_params = _build_exempt_clause()
    vat_multiplier_expr = f"CASE WHEN {exempt_sql} THEN 1.0 ELSE ? END"
    margin_safe = margin if margin else 1.0
    vat = settings.default_vat if vat is None else vat
    vat = max(float(vat), 0.0)
    vat_multiplier = 1.0 if vat == 0 else vat

    query = """
        SELECT
            COALESCE(SUM(CAST(PricingPreTaxTotal AS DOUBLE) * ?), 0) AS total_pricing,
            COALESCE(SUM(CAST(BillingPreTaxTotal AS DOUBLE)), 0) AS total_billing,
            COUNT(*) AS total_records,
            COALESCE(SUM((CAST(PricingPreTaxTotal AS DOUBLE) * ?) / ?), 0) AS total_vat_ex,
            COALESCE(SUM(((CAST(PricingPreTaxTotal AS DOUBLE) * ?) / ?) * ({vat_multiplier_expr})), 0) AS total_vat_inc
        FROM {table}{where_sql}
    """.format(table=table, where_sql=where_sql, vat_multiplier_expr=vat_multiplier_expr)

    params: List[Any] = [
        forex,          # total_pricing
        forex,          # total_vat_ex
        margin_safe,    
        forex,          # total_vat_inc
        margin_safe,
    ]
    params.extend(exempt_params) # vat_expr in total_vat_inc
    params.append(vat_multiplier)
    params.extend(where_params)

    with _connect() as con:
        total_pricing, total_billing, total_records, total_vat_ex, total_vat_inc = con.execute(query, params).fetchone()

    return {
        "total_pricing": float(total_pricing or 0.0),
        "total_billing": float(total_billing or 0.0),
        "total_records": int(total_records or 0),
        "total_vat_ex": float(total_vat_ex or 0.0),
        "total_vat_inc": float(total_vat_inc or 0.0),
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
    default_vat_decimal = _decimal_or_default(settings.default_vat, Decimal("1"))
    forex_rate = _decimal_or_default(forex, Decimal("1"))
    if forex_rate <= Decimal("0"):
        forex_rate = Decimal("1")

    margin_rate = _decimal_or_default(margin, Decimal("1"))
    if margin_rate <= Decimal("0"):
        margin_rate = Decimal("1")

    # Check if we are filtering by a specific customer who is exempt
    customer_name_filter = filters.get("CustomerName")
    is_exempt = False
    if customer_name_filter and settings.vat_exempt_customers:
        c_name = str(customer_name_filter).lower()
        for exempt_kw in settings.vat_exempt_customers:
            if exempt_kw.lower() in c_name:
                is_exempt = True
                break

    vat_rate = _decimal_or_default(vat, default_vat_decimal)
    if vat_rate < Decimal("0"):
        vat_rate = default_vat_decimal

    vat_multiplier = Decimal("1") if vat_rate == Decimal("0") else vat_rate
    
    if is_exempt:
        vat_multiplier = Decimal("1")

    details_query = """
        SELECT
            MeterCategory,
            MeterSubCategory,
            MeterName,
            MeterType,
            Unit,
            EntitlementDescription,
            EntitlementId,
            STRING_AGG(DISTINCT NULLIF(TRIM(Tags), ''), ', ') AS Tags,
            COALESCE(SUM(TRY_CAST(Quantity AS DECIMAL(38, 12))), 0) AS Quantity,
            CASE
                WHEN SUM(TRY_CAST(Quantity AS DECIMAL(38, 12))) = 0 THEN 0
                ELSE SUM(TRY_CAST(PricingPreTaxTotal AS DECIMAL(38, 12))) / SUM(TRY_CAST(Quantity AS DECIMAL(38, 12)))
            END AS UnitPrice,
            COALESCE(SUM(TRY_CAST(PricingPreTaxTotal AS DECIMAL(38, 12))), 0) AS PricingPreTaxTotal,
            COALESCE(SUM(TRY_CAST(BillingPreTaxTotal AS DECIMAL(38, 12))), 0) AS BillingPreTaxTotal
        FROM {table}
        {where_sql}
        GROUP BY MeterCategory, MeterSubCategory, MeterName, MeterType, Unit, EntitlementDescription, EntitlementId
        ORDER BY MeterCategory, MeterSubCategory, MeterName, EntitlementDescription
    """.format(table=table, where_sql=where_sql)

    totals_query = """
        SELECT
            COALESCE(SUM(TRY_CAST(Quantity AS DECIMAL(38, 12))), 0) AS TotalQuantity,
            CASE
                WHEN SUM(TRY_CAST(Quantity AS DECIMAL(38, 12))) = 0 THEN 0
                ELSE SUM(TRY_CAST(PricingPreTaxTotal AS DECIMAL(38, 12))) / SUM(TRY_CAST(Quantity AS DECIMAL(38, 12)))
            END AS WeightedUnitPrice,
            COALESCE(SUM(TRY_CAST(PricingPreTaxTotal AS DECIMAL(38, 12))), 0) AS TotalPreTax,
            COALESCE(SUM(TRY_CAST(BillingPreTaxTotal AS DECIMAL(38, 12))), 0) AS TotalBilling
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

    items: list[dict[str, Any]] = []
    for row in rows:
        quantity_dec = _decimal_or_default(row[8])
        unit_price_dec = _decimal_or_default(row[9])
        pricing_total_dec = _decimal_or_default(row[10])
        billing_total_dec = _decimal_or_default(row[11])

        pretax_forex = pricing_total_dec * forex_rate
        vat_ex = pretax_forex / margin_rate
        total_vat_inc_dec = vat_ex * vat_multiplier

        items.append(
            {
                "meter_category": row[0],
                "meter_sub_category": row[1],
                "meter_name": row[2],
                "meter_type": row[3],
                "unit": row[4],
                "entitlement_description": row[5],
                "entitlement_id": row[6],
                "tags": row[7],
                "quantity": float(quantity_dec),
                "quantity_raw": _decimal_to_string(quantity_dec),
                "unit_price": float(unit_price_dec),
                "unit_price_raw": _decimal_to_string(unit_price_dec, minimum_fraction_digits=2),
                "pricing_pretax_total": float(pricing_total_dec),
                "pricing_pretax_total_raw": _decimal_to_string(pricing_total_dec, minimum_fraction_digits=2),
                "billing_pretax_total": float(billing_total_dec),
                "billing_pretax_total_raw": _decimal_to_string(billing_total_dec, minimum_fraction_digits=2),
                "total_vat_inc": float(total_vat_inc_dec),
                "total_vat_inc_raw": _decimal_to_string(total_vat_inc_dec, minimum_fraction_digits=2),
            }
        )

    if totals_row:
        total_quantity_dec = _decimal_or_default(totals_row[0])
        total_unit_price_dec = _decimal_or_default(totals_row[1])
        total_pricing_dec = _decimal_or_default(totals_row[2])
        total_billing_dec = _decimal_or_default(totals_row[3])
    else:
        total_quantity_dec = Decimal("0")
        total_unit_price_dec = Decimal("0")
        total_pricing_dec = Decimal("0")
        total_billing_dec = Decimal("0")

    pretax_forex_total = total_pricing_dec * forex_rate
    total_vat_ex = pretax_forex_total / margin_rate
    total_vat_inc_dec = total_vat_ex * vat_multiplier

    total_quantity = float(total_quantity_dec)
    total_unit_price = float(total_unit_price_dec)
    total_pricing = float(total_pricing_dec)
    total_billing = float(total_billing_dec)
    total_vat_inc = float(total_vat_inc_dec)

    period_start = period_row[0] if period_row else None
    period_end = period_row[1] if period_row else None

    return {
        "items": items,
        "total_quantity": total_quantity,
        "total_unit_price": total_unit_price,
        "total_pricing": total_pricing,
        "total_billing": total_billing,
        "total_vat_inc": total_vat_inc,
        "raw_totals": {
            "quantity": _decimal_to_string(total_quantity_dec),
            "unit_price": _decimal_to_string(total_unit_price_dec, minimum_fraction_digits=2),
            "pricing": _decimal_to_string(total_pricing_dec, minimum_fraction_digits=2),
            "billing": _decimal_to_string(total_billing_dec, minimum_fraction_digits=2),
            "total_vat_inc": _decimal_to_string(total_vat_inc_dec, minimum_fraction_digits=2),
        },
        "period_start": period_start,
        "period_end": period_end,
    }


def compare_uploads(
    current_upload_id: int,
    previous_upload_id: int,
    *,
    search: str | None = None,
    entitlement_search: str | None = None,
    filters: Mapping[str, Any] | None = None,
    forex: float | None = None,
    margin: float | None = None,
    vat: float | None = None,
) -> Mapping[str, Any]:
    current_table = _view_name(current_upload_id)
    prev_table = _view_name(previous_upload_id)
    where_sql, where_params = _build_filters(search, filters)

    default_vat_decimal = _decimal_or_default(settings.default_vat, Decimal("1"))
    forex_rate = _decimal_or_default(forex, Decimal("1"))
    if forex_rate <= Decimal("0"):
        forex_rate = Decimal("1")

    margin_rate = _decimal_or_default(margin, Decimal("1"))
    if margin_rate <= Decimal("0"):
        margin_rate = Decimal("1")

    vat_rate = _decimal_or_default(vat, default_vat_decimal)
    if vat_rate <= Decimal("0"):
        vat_rate = default_vat_decimal

    extra_clauses = []
    extra_params = []
    if entitlement_search:
        extra_clauses.append(
            "(lower(EntitlementDescription) LIKE '%' || lower(?) || '%' OR lower(EntitlementId) LIKE '%' || lower(?) || '%')"
        )
        # We need this param twice for each time the clause is used (once for current, once for prev)
        # Actually checking how params are used:
        # We concatenate params later.
        pass

    # Reconstruct where_sql to include entitlement search if needed
    # Since _build_filters returns string and list, we need to append to them.
    # But wait, we define CTEs using where_sql.
    
    current_where_sql = where_sql
    current_params = list(where_params)
    
    if entitlement_search:
        if current_where_sql:
            current_where_sql += " AND (lower(EntitlementDescription) LIKE '%' || lower(?) || '%' OR lower(EntitlementId) LIKE '%' || lower(?) || '%')"
        else:
            current_where_sql = " WHERE (lower(EntitlementDescription) LIKE '%' || lower(?) || '%' OR lower(EntitlementId) LIKE '%' || lower(?) || '%')"
        current_params.extend([entitlement_search, entitlement_search])

    prev_where_sql = current_where_sql # Same filters for both
    prev_params = list(current_params)

    # Use a CTE for each period to aggregate data
    query = """
        WITH current_period AS (
            SELECT
                MeterCategory, MeterSubCategory, MeterName, MeterType, Unit,
                EntitlementDescription, EntitlementId,
                COALESCE(SUM(TRY_CAST(Quantity AS DECIMAL(38, 12))), 0) AS Qty,
                COALESCE(SUM(TRY_CAST(PricingPreTaxTotal AS DECIMAL(38, 12))), 0) AS Pricing
            FROM {current_table}
            {where_sql}
            GROUP BY MeterCategory, MeterSubCategory, MeterName, MeterType, Unit, EntitlementDescription, EntitlementId
        ),
        prev_period AS (
            SELECT
                MeterCategory, MeterSubCategory, MeterName, MeterType, Unit,
                EntitlementDescription, EntitlementId,
                COALESCE(SUM(TRY_CAST(Quantity AS DECIMAL(38, 12))), 0) AS Qty,
                COALESCE(SUM(TRY_CAST(PricingPreTaxTotal AS DECIMAL(38, 12))), 0) AS Pricing
            FROM {prev_table}
            {where_sql}
            GROUP BY MeterCategory, MeterSubCategory, MeterName, MeterType, Unit, EntitlementDescription, EntitlementId
        )
        SELECT
            COALESCE(c.MeterCategory, p.MeterCategory) AS MeterCategory,
            COALESCE(c.MeterSubCategory, p.MeterSubCategory) AS MeterSubCategory,
            COALESCE(c.MeterName, p.MeterName) AS MeterName,
            COALESCE(c.MeterType, p.MeterType) AS MeterType,
            COALESCE(c.Unit, p.Unit) AS Unit,
            COALESCE(c.EntitlementDescription, p.EntitlementDescription) AS EntitlementDescription,
            COALESCE(c.EntitlementId, p.EntitlementId) AS EntitlementId,
            COALESCE(c.Qty, 0) AS CurrentQty,
            COALESCE(p.Qty, 0) AS PrevQty,
            COALESCE(c.Pricing, 0) AS CurrentPricing,
            COALESCE(p.Pricing, 0) AS PrevPricing
        FROM current_period c
        FULL OUTER JOIN prev_period p ON
            COALESCE(c.MeterCategory, '') = COALESCE(p.MeterCategory, '') AND
            COALESCE(c.MeterSubCategory, '') = COALESCE(p.MeterSubCategory, '') AND
            COALESCE(c.MeterName, '') = COALESCE(p.MeterName, '') AND
            COALESCE(c.MeterType, '') = COALESCE(p.MeterType, '') AND
            COALESCE(c.Unit, '') = COALESCE(p.Unit, '') AND
            COALESCE(c.EntitlementId, '') = COALESCE(p.EntitlementId, '')
    """.format(
        current_table=current_table,
        prev_table=prev_table,
        where_sql=current_where_sql
    )

    # Params: for current_period (current_params) AND prev_period (prev_params)
    # The where_sql is formatted into the string twice.
    all_params = current_params + prev_params

    with _connect() as con:
        rows = con.execute(query, all_params).fetchall()

    results = []
    for row in rows:
        c_qty = _decimal_or_default(row[7])
        p_qty = _decimal_or_default(row[8])
        c_pricing = _decimal_or_default(row[9])
        p_pricing = _decimal_or_default(row[10])

        # Apply Formula: (Pricing * Forex / Margin) * VAT
        def calculate_final_cost(pricing: Decimal) -> Decimal:
            pretax_forex = pricing * forex_rate
            vat_ex = pretax_forex / margin_rate
            return vat_ex * vat_rate

        c_cost = calculate_final_cost(c_pricing)
        p_cost = calculate_final_cost(p_pricing)

        diff_qty = c_qty - p_qty
        diff_cost = c_cost - p_cost

        status = "Unchanged"
        if p_qty == 0 and c_qty > 0:
            status = "Added"
        elif c_qty == 0 and p_qty > 0:
            status = "Deleted"
        elif c_cost > p_cost:
            status = "Increased"
        elif c_cost < p_cost:
            status = "Decreased"
        elif diff_cost == 0 and diff_qty == 0:
            status = "Unchanged"
        
        # refinement: user asked for "spiked". We can call "Increased" -> "Increased/Spiked"
        # or separate if increase is large (> 50%?). Let's stick to simple categories first.
        
        results.append({
            "meter_category": row[0],
            "meter_sub_category": row[1],
            "meter_name": row[2],
            "meter_type": row[3],
            "unit": row[4],
            "entitlement_description": row[5],
            "entitlement_id": row[6],
            "current_qty": float(c_qty),
            "prev_qty": float(p_qty),
            "diff_qty": float(diff_qty),
            "current_cost": float(c_cost),
            "prev_cost": float(p_cost),
            "diff_cost": float(diff_cost),
            "status": status
        })

    # Sort by absolute cost difference descending to show most impactful changes first
    results.sort(key=lambda x: abs(x["diff_cost"]), reverse=True)
    return {"results": results}
