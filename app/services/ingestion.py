from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb

from app.config import settings
from app.models import Upload, UploadStatus

CSV_COLUMNS = [
    "PartnerId",
    "PartnerName",
    "CustomerId",
    "CustomerName",
    "CustomerDomainName",
    "CustomerCountry",
    "MpnId",
    "Tier2MpnId",
    "InvoiceNumber",
    "ProductId",
    "SkuId",
    "AvailabilityId",
    "SkuName",
    "ProductName",
    "PublisherName",
    "PublisherId",
    "SubscriptionDescription",
    "SubscriptionId",
    "ChargeStartDate",
    "ChargeEndDate",
    "UsageDate",
    "MeterType",
    "MeterCategory",
    "MeterId",
    "MeterSubCategory",
    "MeterName",
    "MeterRegion",
    "Unit",
    "ResourceLocation",
    "ConsumedService",
    "ResourceGroup",
    "ResourceURI",
    "ChargeType",
    "UnitPrice",
    "Quantity",
    "UnitType",
    "BillingPreTaxTotal",
    "BillingCurrency",
    "PricingPreTaxTotal",
    "PricingCurrency",
    "ServiceInfo1",
    "ServiceInfo2",
    "Tags",
    "AdditionalInfo",
    "EffectiveUnitPrice",
    "PCToBCExchangeRate",
    "PCToBCExchangeRateDate",
    "EntitlementId",
    "EntitlementDescription",
    "PartnerEarnedCreditPercentage",
    "CreditPercentage",
    "CreditType",
    "BenefitId",
    "BenefitOrderId",
    "BenefitType",
]


def _parquet_path(upload_id: int) -> Path:
    return settings.processed_dir / f"upload_{upload_id}.parquet"


def process_upload_csv(upload: Upload) -> dict:
    csv_path = Path(upload.stored_path)
    parquet_path = _parquet_path(upload.id)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    def _literal(path: Path) -> str:
        # DuckDB COPY/read functions require string literals, so escape any single quotes.
        return path.as_posix().replace("'", "''")

    con = duckdb.connect(str(settings.duckdb_path))
    con.execute("CREATE SCHEMA IF NOT EXISTS uploads")

    copy_sql = f"""
        COPY (
            SELECT *
            FROM read_csv_auto(
                '{_literal(csv_path)}',
                header=True,
                union_by_name=True,
                ignore_errors=True,
                timestampformat='%Y-%m-%d',
                sample_size=-1
            )
        ) TO '{_literal(parquet_path)}' (FORMAT 'parquet', COMPRESSION 'zstd');
    """

    con.execute(copy_sql)

    stats_query = f"""
        SELECT
            COUNT(*) AS total_rows,
            COALESCE(SUM(TRY_CAST(PricingPreTaxTotal AS DOUBLE)), 0) AS total_pricing,
            COALESCE(SUM(TRY_CAST(BillingPreTaxTotal AS DOUBLE)), 0) AS total_billing,
            MIN(TRY_CAST(COALESCE(UsageDate, ChargeStartDate) AS DATE)) AS min_usage,
            MAX(TRY_CAST(COALESCE(UsageDate, ChargeEndDate) AS DATE)) AS max_usage
        FROM read_parquet('{_literal(parquet_path)}')
    """

    stats = con.execute(stats_query).fetchone()

    con.execute(
        """
        CREATE OR REPLACE VIEW uploads.upload_{id} AS
        SELECT * FROM read_parquet('{path}')
        """.format(id=upload.id, path=_literal(parquet_path))
    )

    con.close()

    return {
        "row_count": stats[0] or 0,
        "pricing_total": float(stats[1] or 0.0),
        "billing_total": float(stats[2] or 0.0),
        "usage_start": stats[3],
        "usage_end": stats[4],
        "parquet_path": str(parquet_path),
    }


def mark_upload_failed(upload: Upload, error: Exception, session) -> None:
    upload.status = UploadStatus.failed
    upload.error_message = str(error)
    upload.completed_at = dt.datetime.utcnow()
    session.add(upload)
    session.commit()


def mark_upload_completed(upload: Upload, stats: dict, session) -> None:
    upload.status = UploadStatus.completed
    upload.row_count = stats["row_count"]
    upload.pricing_pretax_total = stats["pricing_total"]
    upload.billing_pretax_total = stats["billing_total"]
    upload.completed_at = dt.datetime.utcnow()
    session.add(upload)
    session.commit()


def delete_upload(upload: Upload, session) -> None:
    csv_path = Path(upload.stored_path)
    parquet_path = _parquet_path(upload.id)
    csv_path.unlink(missing_ok=True)
    parquet_path.unlink(missing_ok=True)

    with duckdb.connect(str(settings.duckdb_path)) as con:
        con.execute(f"DROP VIEW IF EXISTS uploads.upload_{upload.id}")

    session.delete(upload)
    session.commit()

