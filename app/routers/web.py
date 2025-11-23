from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from sqlmodel import select

from app import auth
from app.config import settings
from app.database import get_session
from app.models import Upload, UploadStatus, User, UserRole
from app.services import queries
from app.services.ingestion import delete_upload, mark_upload_completed, mark_upload_failed, process_upload_csv

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/", response_class=HTMLResponse)
async def landing(request: Request, user: User | None = Depends(auth.get_optional_user)):
    if user:
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
    return templates.TemplateResponse("login.html", {"request": request})


@router.post("/login")
async def login(
    request: Request,
    role: str = Form(...),
    session=Depends(get_session),
):
    try:
        selected_role = UserRole(role)
    except ValueError:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Select a valid role"},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    user = auth.get_user_by_role(session, selected_role)
    if not user:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Role not available. Contact an administrator."},
            status_code=status.HTTP_404_NOT_FOUND,
        )
    auth.login_user(request, user)
    return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)


@router.get("/logout")
async def logout(request: Request):
    auth.logout_user(request)
    response = RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    response.delete_cookie(settings.session_cookie_name)
    return response


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    user: User = Depends(auth.get_current_user),
    session=Depends(get_session),
):
    uploads = (
        session.exec(select(Upload).order_by(Upload.created_at.desc()).limit(10)).all()
    )
    completed_uploads = session.exec(
        select(Upload)
        .where(Upload.status == UploadStatus.completed)
        .order_by(Upload.created_at.desc())
    ).all()
    latest_upload = completed_uploads[0] if completed_uploads else None
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": user,
            "uploads": uploads,
            "completed_uploads": completed_uploads,
            "latest_upload": latest_upload,
            "default_forex": 1.0,
            "default_margin": 1.0,
            "default_vat": settings.default_vat,
        },
    )


@router.get("/invoice", response_class=HTMLResponse)
async def invoice_view(
    request: Request,
    upload_id: int = Query(..., gt=0),
    customer: str = Query(...),
    customer_domain: str = Query(...),
    invoice: str | None = Query(default=None),
    search: str | None = Query(default=None),
    forex: float = Query(default=1.0, gt=0),
    margin: float = Query(default=1.0, gt=0),
    vat: float = Query(default=settings.default_vat, gt=0),
    user: User = Depends(auth.get_current_user),
):
    filters = {
        "CustomerName": customer,
        "CustomerDomainName": customer_domain,
    }
    if invoice:
        filters["InvoiceNumber"] = invoice

    invoice_data = queries.invoice_details(
        upload_id,
        filters=filters,
        search=search,
        forex=forex,
        margin=margin,
        vat=vat,
    )
    if not invoice_data["items"]:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No data available for invoice")

    grouped_items: list[dict] = []
    group_index: dict[tuple[str | None, str | None], dict] = {}
    for item in invoice_data["items"]:
        group_key = (item.get("meter_category"), item.get("meter_sub_category"))
        group = group_index.get(group_key)
        if not group:
            group = {
                "meter_category": item.get("meter_category"),
                "meter_sub_category": item.get("meter_sub_category"),
                "line_items": [],
                "subtotal_vat_inc": 0.0,
                "subtotal_quantity": 0.0,
            }
            group_index[group_key] = group
            grouped_items.append(group)
        group["line_items"].append(item)
        group["subtotal_vat_inc"] += item.get("total_vat_inc", 0.0)
        group["subtotal_quantity"] += item.get("quantity", 0.0)

    period_start = invoice_data["period_start"]
    period_end = invoice_data["period_end"]
    if period_start and period_end:
        try:
            start_dt = dt.datetime.fromisoformat(str(period_start))
            end_dt = dt.datetime.fromisoformat(str(period_end))
        except ValueError:
            start_dt = dt.datetime.utcnow()
            end_dt = start_dt
        if start_dt.strftime("%Y-%m") == end_dt.strftime("%Y-%m"):
            period_label = start_dt.strftime("%B %Y")
        else:
            period_label = f"{start_dt.strftime('%b %d, %Y')} - {end_dt.strftime('%b %d, %Y')}"
    else:
        period_label = "N/A"

    context = {
        "request": request,
        "user": user,
        "customer": customer,
        "domain": customer_domain,
        "invoice_number": invoice,
        "generated_at": dt.datetime.utcnow(),
        "period_label": period_label,
        "grouped_items": grouped_items,
        "items": invoice_data["items"],
        "total_quantity": invoice_data["total_quantity"],
        "total_unit_price": invoice_data["total_unit_price"],
        "total_pricing": invoice_data["total_pricing"],
        "total_vat_inc": invoice_data["total_vat_inc"],
        "forex": forex,
        "margin": margin,
        "vat": vat,
    }
    return templates.TemplateResponse("invoice.html", context)


@router.get("/uploads", response_class=HTMLResponse)
async def uploads_page(
    request: Request,
    user: User = Depends(auth.require_admin),
    session=Depends(get_session),
):
    uploads = session.exec(select(Upload).order_by(Upload.created_at.desc())).all()
    return templates.TemplateResponse(
        "uploads.html",
        {"request": request, "user": user, "uploads": uploads, "max_size": settings.max_upload_size_mb},
    )


@router.post("/uploads")
async def upload_file(
    request: Request,
    file: UploadFile,
    user: User = Depends(auth.require_admin),
    session=Depends(get_session),
):
    if not file:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No file provided")
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File must be a CSV")

    uploads_dir = settings.uploads_dir
    uploads_dir.mkdir(parents=True, exist_ok=True)
    stored_path = uploads_dir / f"{dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{file.filename}"

    size_accum = 0
    chunk_size = 4 * 1024 * 1024
    with stored_path.open("wb") as buffer:
        while True:
            chunk = await file.read(chunk_size)
            if not chunk:
                break
            size_accum += len(chunk)
            if (size_accum / (1024 * 1024)) > settings.max_upload_size_mb:
                stored_path.unlink(missing_ok=True)
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail="File too large",
                )
            buffer.write(chunk)

    upload = Upload(
        original_filename=file.filename,
        stored_path=str(stored_path),
        user_id=user.id,
        status=UploadStatus.processing,
        created_at=dt.datetime.utcnow(),
    )
    session.add(upload)
    session.commit()
    session.refresh(upload)

    try:
        stats = process_upload_csv(upload)
        mark_upload_completed(upload, stats, session)
    except Exception as exc:  # pragma: no cover
        mark_upload_failed(upload, exc, session)
        raise

    return RedirectResponse(url="/uploads", status_code=status.HTTP_302_FOUND)


@router.post("/uploads/{upload_id}/delete")
async def remove_upload(
    upload_id: int,
    request: Request,
    user: User = Depends(auth.require_admin),
    session=Depends(get_session),
):
    upload = session.get(Upload, upload_id)
    if not upload:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Upload not found")

    delete_upload(upload, session)
    return RedirectResponse(url="/uploads", status_code=status.HTTP_303_SEE_OTHER)
