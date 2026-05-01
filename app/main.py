from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
import logging
from typing import Annotated

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .auth import (
    AuthenticatedUser,
    create_session,
    get_authenticated_user_by_token,
    get_scope_summary,
    list_login_users,
    login_user,
    logout_user_session,
)
from .config import Settings
from .db import close_pool, get_connection, get_pool, init_pool
from .notion_client import NotionClient
from .queries import (
    fetch_home_overview,
    fetch_products_for_project,
    fetch_project_dashboard,
    fetch_projects,
    fetch_workload_overview,
    fetch_tasks_for_product,
    fetch_tasks_for_project,
    fetch_timeline,
)
from .sync import perform_sync


logger = logging.getLogger(__name__)

settings = Settings.load()
notion_client = NotionClient(settings.notion_token, settings.notion_version, timeout=settings.http_timeout)


class LoginPayload(BaseModel):
    user_key: str
    password: str


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token.strip()


async def require_authenticated_user(
    authorization: Annotated[str | None, Header()] = None,
    conn=Depends(get_connection),
) -> AuthenticatedUser:
    token = _extract_bearer_token(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    user = await get_authenticated_user_by_token(conn, token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    return user


async def require_workload_admin(
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
) -> AuthenticatedUser:
    if not current_user.can_view_workload:
        raise HTTPException(status_code=403, detail="No autorizado para ver carga global")
    return current_user


def _auto_sync_interval_seconds() -> float:
    raw = os.getenv("AUTO_SYNC_INTERVAL_SECONDS", "").strip()
    if not raw:
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.0


async def _auto_sync_loop(interval_seconds: float) -> None:
    """Background task: periodically run perform_sync against the configured Notion source.

    Enabled by setting AUTO_SYNC_INTERVAL_SECONDS to a positive number (e.g. 21600 = 6h).
    Runs the first sync immediately on startup, then every `interval_seconds` thereafter.
    Errors are logged but do not stop the loop.
    """
    while True:
        try:
            pool = get_pool()
            summary = await perform_sync(pool, settings, notion_client)
            logger.info(
                "auto-sync ok counts=%s duration=%.2fs",
                summary.get("counts"),
                summary.get("duration_seconds", 0.0),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("auto-sync iteration failed; will retry after the interval")
        try:
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool(settings.effective_pg_dsn)
    sync_task: asyncio.Task | None = None
    interval_seconds = _auto_sync_interval_seconds()
    if interval_seconds > 0:
        logger.info("Starting auto-sync loop every %.0fs", interval_seconds)
        sync_task = asyncio.create_task(_auto_sync_loop(interval_seconds))
    try:
        yield
    finally:
        if sync_task is not None:
            sync_task.cancel()
            try:
                await sync_task
            except (asyncio.CancelledError, Exception):
                pass
        await close_pool()
        await notion_client.close()


app = FastAPI(title="Notion Sync Dashboard API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/sync")
async def sync_now(
    current_user: AuthenticatedUser = Depends(require_workload_admin),
):
    pool = get_pool()
    try:
        summary = await perform_sync(pool, settings, notion_client)
        return {**summary, "requested_by": current_user.user_key}
    except ValueError as exc:
        logger.exception("Sync rejected due to invalid configuration or payload")
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except httpx.HTTPStatusError as exc:
        detail = {
            "source": "notion_api",
            "status_code": exc.response.status_code if exc.response else None,
            "url": str(exc.request.url) if exc.request else None,
            "body": exc.response.text if exc.response else None,
        }
        logger.exception("Sync failed calling Notion API: %s", detail)
        raise HTTPException(status_code=502, detail=detail) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Sync failed with unexpected error")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/auth/users")
async def api_auth_users(conn=Depends(get_connection)):
    if not settings.public_login_directory:
        raise HTTPException(status_code=404, detail="Directorio de usuarios deshabilitado")
    return {"users": await list_login_users(conn)}


@app.post("/api/auth/login")
async def api_auth_login(payload: LoginPayload, conn=Depends(get_connection)):
    user = await login_user(conn, user_key=payload.user_key, password=payload.password)
    if not user:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    token, expires_at = await create_session(conn, user_key=user.user_key)
    scope = await get_scope_summary(conn, user.user_key)
    await conn.commit()
    return {
        "token": token,
        "expires_at": expires_at.isoformat(),
        "user": {
            "user_key": user.user_key,
            "display_name": user.display_name,
            "can_view_workload": user.can_view_workload,
            "can_view_all": user.can_view_all,
            "scope": {
                "projects": scope["project"],
                "products": scope["product"],
                "tasks": scope["task"],
            },
        },
    }


@app.get("/api/auth/me")
async def api_auth_me(
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    conn=Depends(get_connection),
):
    scope = await get_scope_summary(conn, current_user.user_key)
    return {
        "user_key": current_user.user_key,
        "display_name": current_user.display_name,
        "can_view_workload": current_user.can_view_workload,
        "can_view_all": current_user.can_view_all,
        "expires_at": current_user.session_expires_at.isoformat() if current_user.session_expires_at else None,
        "scope": {
            "projects": scope["project"],
            "products": scope["product"],
            "tasks": scope["task"],
        },
    }


@app.post("/api/auth/logout")
async def api_auth_logout(
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    authorization: Annotated[str | None, Header()] = None,
    conn=Depends(get_connection),
):
    token = _extract_bearer_token(authorization)
    if token:
        await logout_user_session(conn, token)
        await conn.commit()
    return {"ok": True, "user_key": current_user.user_key}


@app.get("/api/projects")
async def api_projects(
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    conn=Depends(get_connection),
):
    return await fetch_projects(conn, settings.done_statuses, current_user.user_key)


@app.get("/api/home")
async def api_home(
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    conn=Depends(get_connection),
):
    return await fetch_home_overview(conn, settings.done_statuses, current_user.user_key)


@app.get("/api/projects/{project_id}/products")
async def api_project_products(
    project_id: str,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    conn=Depends(get_connection),
):
    return await fetch_products_for_project(conn, project_id, settings.done_statuses, current_user.user_key)


@app.get("/api/products/{product_id}/tasks")
async def api_product_tasks(
    product_id: str,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    conn=Depends(get_connection),
):
    return await fetch_tasks_for_product(conn, product_id, settings.done_statuses, current_user.user_key)


@app.get("/api/projects/{project_id}/tasks")
async def api_project_tasks(
    project_id: str,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    conn=Depends(get_connection),
):
    return await fetch_tasks_for_project(conn, project_id, settings.done_statuses, current_user.user_key)


@app.get("/api/projects/{project_id}/timeline")
async def api_timeline(
    project_id: str,
    mode: Annotated[str, Query(pattern="^(products|tasks)$")] = "products",
    product_id: str | None = None,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    conn=Depends(get_connection),
):
    try:
        return await fetch_timeline(
            conn,
            project_id,
            mode,
            product_id,
            settings.done_statuses,
            current_user.user_key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/projects/{project_id}/dashboard")
async def api_project_dashboard(
    project_id: str,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    conn=Depends(get_connection),
):
    return await fetch_project_dashboard(conn, project_id, settings.done_statuses, current_user.user_key)


@app.get("/api/workload")
async def api_workload(
    year: int = Query(..., ge=2024, le=2100),
    month: int = Query(..., ge=1, le=12),
    current_user: AuthenticatedUser = Depends(require_workload_admin),
    conn=Depends(get_connection),
):
    return await fetch_workload_overview(conn, year=year, month=month)
