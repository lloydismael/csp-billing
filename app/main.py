from __future__ import annotations

import pathlib

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from app.auth import ensure_default_accounts
from app.config import settings
from app.database import init_db, session_scope
from app.routers import api, web


def create_app() -> FastAPI:
    app = FastAPI(title=settings.app_name)

    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.secret_key,
        session_cookie=settings.session_cookie_name,
        max_age=60 * 60 * 12,
        same_site="lax",
        https_only=False,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    static_path = pathlib.Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=static_path), name="static")

    app.include_router(web.router)
    app.include_router(api.router)

    @app.on_event("startup")
    async def on_startup() -> None:  # pragma: no cover
        init_db()
        with session_scope() as session:
            ensure_default_accounts(session)

    @app.exception_handler(Exception)
    async def server_error_handler(request: Request, exc: Exception):  # pragma: no cover
        return JSONResponse(status_code=500, content={"detail": str(exc)})

    @app.get("/healthz")
    async def healthcheck() -> dict:
        return {"status": "ok"}

    return app


app = create_app()
