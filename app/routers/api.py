from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from sqlmodel import Session

from app import auth
from app.config import settings
from app.database import get_session
from app.models import Upload, UploadStatus, User, UserRole
from app.schemas import DataPage
from app.services import queries

router = APIRouter(prefix="/api")


def _get_upload(upload_id: int, session: Session, user: User) -> Upload:
    upload = session.get(Upload, upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    if upload.status != UploadStatus.completed and user.role != UserRole.admin:
        raise HTTPException(status_code=403, detail="Upload still processing")
    return upload


@router.get("/uploads/{upload_id}/data", response_model=DataPage)
async def get_upload_data(
    upload_id: int,
    user: User = Depends(auth.get_current_user),
    session: Session = Depends(get_session),
    limit: int | None = Query(100, ge=0, le=5000),
    page: int = Query(1, ge=1),
    search: Optional[str] = Query(None),
    forex: float = Query(1.0, ge=0.0),
    margin: float = Query(1.0, gt=0.0),
    vat: float = Query(None, ge=0.0),
    customer: Optional[str] = Query(None),
    customer_domain: Optional[str] = Query(None),
    invoice: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    charge_type: Optional[str] = Query(None),
    columns: Optional[str] = Query(None),
    all_records: bool = Query(False, alias="all_records"),
):
    upload = _get_upload(upload_id, session, user)
    filters = {}
    if customer:
        filters["CustomerName"] = customer
    if customer_domain:
        filters["CustomerDomainName"] = customer_domain
    if invoice:
        filters["InvoiceNumber"] = invoice
    if product:
        filters["ProductName"] = product
    if charge_type:
        filters["ChargeType"] = charge_type

    column_list = columns.split(",") if columns else None
    limit_value = 100 if limit is None else limit
    if all_records:
        limit_value = 0
        page = 1
    offset = 0 if not limit_value else (page - 1) * limit_value
    page_data = queries.fetch_data_page(
        upload.id,
        limit=None if not limit_value else limit_value,
        offset=offset,
        forex=forex,
        margin=margin,
        vat=vat,
        search=search,
        filters=filters,
        columns=column_list,
    )
    return DataPage(**page_data)


@router.get("/uploads/{upload_id}/summary")
async def upload_summary(
    upload_id: int,
    user: User = Depends(auth.get_current_user),
    session: Session = Depends(get_session),
    forex: float = Query(1.0, ge=0.0),
    margin: float = Query(1.0, gt=0.0),
    vat: float = Query(None, ge=0.0),
    customer: Optional[str] = Query(None),
    customer_domain: Optional[str] = Query(None),
    invoice: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    charge_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    upload = _get_upload(upload_id, session, user)
    filters = {}
    if customer:
        filters["CustomerName"] = customer
    if customer_domain:
        filters["CustomerDomainName"] = customer_domain
    if invoice:
        filters["InvoiceNumber"] = invoice
    if product:
        filters["ProductName"] = product
    if charge_type:
        filters["ChargeType"] = charge_type

    summary = queries.summarize_upload(
        upload.id,
        forex=forex,
        margin=margin,
        vat=vat or settings.default_vat,
        search=search,
        filters=filters,
    )
    return summary


@router.get("/uploads/{upload_id}/top-customers")
async def chart_top_customers(
    upload_id: int,
    user: User = Depends(auth.get_current_user),
    session: Session = Depends(get_session),
    limit: int = Query(10, ge=1, le=50),
    customer: Optional[str] = Query(None),
    customer_domain: Optional[str] = Query(None),
    invoice: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    charge_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    upload = _get_upload(upload_id, session, user)
    filters = {}
    if customer:
        filters["CustomerName"] = customer
    if customer_domain:
        filters["CustomerDomainName"] = customer_domain
    if invoice:
        filters["InvoiceNumber"] = invoice
    if product:
        filters["ProductName"] = product
    if charge_type:
        filters["ChargeType"] = charge_type

    return queries.top_customers(upload.id, limit=limit, search=search, filters=filters)


@router.get("/uploads/{upload_id}/invoices")
async def list_upload_invoices(
    upload_id: int,
    user: User = Depends(auth.get_current_user),
    session: Session = Depends(get_session),
    limit: int = Query(200, ge=1, le=2000),
    customer: Optional[str] = Query(None),
    customer_domain: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    charge_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    upload = _get_upload(upload_id, session, user)
    filters = {}
    if customer:
        filters["CustomerName"] = customer
    if customer_domain:
        filters["CustomerDomainName"] = customer_domain
    if product:
        filters["ProductName"] = product
    if charge_type:
        filters["ChargeType"] = charge_type

    invoices = queries.list_invoices(upload.id, limit=limit, search=search, filters=filters)
    return {"invoices": invoices}

