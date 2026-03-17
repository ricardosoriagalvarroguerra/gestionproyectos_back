from __future__ import annotations

import base64
import hashlib
import os
import re
import secrets
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import psycopg


SESSION_TTL_DAYS = 14
PASSWORD_ITERATIONS = 120_000
INTERNAL_USER_PROPERTY = "involucrados"
ENTITY_FALLBACK_PROPERTIES: dict[str, tuple[str, ...]] = {
    "project": (),
    "product": ("personas", "responsable"),
    "task": ("responsable", "asignado", "colaborador contraparte"),
}


@dataclass(slots=True)
class AuthenticatedUser:
    user_key: str
    display_name: str
    can_view_workload: bool = False
    can_view_all: bool = False
    session_token_hash: str | None = None
    session_expires_at: datetime | None = None


def normalize_name(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    collapsed = re.sub(r"[^a-zA-Z0-9]+", " ", ascii_name).strip().lower()
    return " ".join(collapsed.split())


def normalize_user_keys(values: Iterable[str]) -> frozenset[str]:
    normalized = {normalize_name(value) for value in values}
    normalized.discard("")
    return frozenset(normalized)


def _clean_display_name(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def _should_replace_display_name(current: str | None, candidate: str) -> bool:
    if not current:
        return True
    if len(candidate) > len(current):
        return True
    if candidate != candidate.lower() and current == current.lower():
        return True
    return False


def _remember_user_name(target: dict[str, str], raw_name: str | None) -> None:
    cleaned = _clean_display_name(raw_name)
    user_key = normalize_name(cleaned)
    if not user_key:
        return
    if _should_replace_display_name(target.get(user_key), cleaned):
        target[user_key] = cleaned


def _split_name_tokens(value: str) -> list[str]:
    return [token.strip() for token in re.split(r"[,\n;/]+", value) if token.strip()]


def _extract_plain_text(prop: dict[str, Any], field_name: str) -> list[str]:
    values = prop.get(field_name) or []
    if not isinstance(values, list):
        return []
    joined = "".join(item.get("plain_text", "") for item in values if isinstance(item, dict)).strip()
    if not joined:
        return []
    return _split_name_tokens(joined)


def _extract_names_from_person_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    names: list[str] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if not name:
            person = item.get("person") or {}
            name = person.get("email")
        if name:
            names.append(name)
    return names


def _extract_names_from_option_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    names: list[str] = []
    for item in values:
        if isinstance(item, dict) and item.get("name"):
            names.append(item["name"])
        elif isinstance(item, str):
            names.append(item)
    return names


def _extract_property_people(prop: Any, *, allow_text: bool) -> list[str]:
    if not isinstance(prop, dict):
        return []

    ptype = prop.get("type")
    if ptype == "people":
        return _extract_names_from_person_list(prop.get("people"))
    if ptype in {"person", "created_by", "last_edited_by"}:
        nested = prop.get(ptype)
        if isinstance(nested, dict):
            return _extract_names_from_person_list([nested])
    if ptype == "multi_select":
        return _extract_names_from_option_list(prop.get("multi_select"))
    if ptype in {"select", "status"}:
        option = prop.get(ptype)
        if isinstance(option, dict) and option.get("name"):
            return [option["name"]]
    if allow_text and ptype == "rich_text":
        return _extract_plain_text(prop, "rich_text")
    if allow_text and ptype == "title":
        return _extract_plain_text(prop, "title")
    if allow_text and ptype == "formula":
        return _extract_property_people(prop.get("formula"), allow_text=True)
    if allow_text and ptype == "rollup":
        rollup = prop.get("rollup") or {}
        if rollup.get("type") == "array":
            names: list[str] = []
            for item in rollup.get("array", []):
                names.extend(_extract_property_people(item, allow_text=True))
            return names
    return []


def extract_login_users(page: dict[str, Any]) -> dict[str, str]:
    properties = page.get("properties") or {}
    users: dict[str, str] = {}

    for property_name, prop in properties.items():
        normalized_property = normalize_name(property_name)
        if normalized_property == INTERNAL_USER_PROPERTY:
            for raw_name in _extract_property_people(prop, allow_text=True):
                _remember_user_name(users, raw_name)

    return users


def extract_access_users(page: dict[str, Any], entity_type: str) -> dict[str, str]:
    properties = page.get("properties") or {}
    users = extract_login_users(page)

    for property_name, prop in properties.items():
        normalized_property = normalize_name(property_name)
        if normalized_property not in ENTITY_FALLBACK_PROPERTIES[entity_type]:
            continue
        for raw_name in _extract_property_people(prop, allow_text=False):
            _remember_user_name(users, raw_name)

    return users


def build_access_index(
    project_pages: Iterable[dict[str, Any]],
    product_pages: Iterable[dict[str, Any]],
    task_pages: Iterable[dict[str, Any]],
    project_product_links: Iterable[tuple[str, str]],
    product_task_links: Iterable[tuple[str, str]],
) -> dict[str, Any]:
    login_users: dict[str, str] = {}
    for page in project_pages:
        for user_key, display_name in extract_login_users(page).items():
            if _should_replace_display_name(login_users.get(user_key), display_name):
                login_users[user_key] = display_name
    for page in product_pages:
        for user_key, display_name in extract_login_users(page).items():
            if _should_replace_display_name(login_users.get(user_key), display_name):
                login_users[user_key] = display_name
    for page in task_pages:
        for user_key, display_name in extract_login_users(page).items():
            if _should_replace_display_name(login_users.get(user_key), display_name):
                login_users[user_key] = display_name

    project_users = {page["id"]: extract_access_users(page, "project") for page in project_pages}
    product_users = {page["id"]: extract_access_users(page, "product") for page in product_pages}
    task_users = {page["id"]: extract_access_users(page, "task") for page in task_pages}
    assignment_rows: list[tuple[str, str, str]] = []

    visible_projects: dict[str, dict[str, str]] = {
        project_id: dict(users) for project_id, users in project_users.items()
    }
    visible_products: dict[str, dict[str, str]] = {
        product_id: dict(users) for product_id, users in product_users.items()
    }
    visible_tasks: dict[str, dict[str, str]] = {
        task_id: dict(users) for task_id, users in task_users.items()
    }

    for product_id, task_id in product_task_links:
        product_scope = visible_products.setdefault(product_id, {})
        for user_key, display_name in task_users.get(task_id, {}).items():
            if _should_replace_display_name(product_scope.get(user_key), display_name):
                product_scope[user_key] = display_name

    for project_id, product_id in project_product_links:
        project_scope = visible_projects.setdefault(project_id, {})
        for user_key, display_name in visible_products.get(product_id, {}).items():
            if _should_replace_display_name(project_scope.get(user_key), display_name):
                project_scope[user_key] = display_name

    access_users: dict[str, str] = {}
    access_rows: list[tuple[str, str, str]] = []

    for entity_type, scopes in (
        ("project", visible_projects),
        ("product", visible_products),
        ("task", visible_tasks),
    ):
        for entity_id, entity_users in scopes.items():
            for user_key, display_name in entity_users.items():
                if _should_replace_display_name(access_users.get(user_key), display_name):
                    access_users[user_key] = display_name
                access_rows.append((user_key, entity_type, entity_id))

    for entity_type, scopes in (
        ("project", project_users),
        ("product", product_users),
        ("task", task_users),
    ):
        for entity_id, entity_users in scopes.items():
            for user_key in entity_users:
                assignment_rows.append((user_key, entity_type, entity_id))

    # If a login-enabled user has no extra access fallback, keep the best display name from the login set.
    for user_key, display_name in login_users.items():
        if _should_replace_display_name(access_users.get(user_key), display_name):
            access_users[user_key] = display_name

    return {
        "login_users": login_users,
        "access_users": access_users,
        "access_rows": access_rows,
        "assignment_rows": assignment_rows,
    }


def _password_salt_bytes() -> bytes:
    env_salt = os.getenv("APP_PASSWORD_SALT", "").strip()
    if env_salt:
        return env_salt.encode("utf-8")
    return b"vpf-notion-internal"


def _hash_password(password: str, salt: bytes) -> str:
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PASSWORD_ITERATIONS,
    )
    return base64.b64encode(digest).decode("ascii")


def generate_temporary_password(display_name: str) -> str:
    token = secrets.token_urlsafe(9).replace("-", "").replace("_", "").upper()
    prefix = normalize_name(display_name).replace(" ", "")[:4].upper() or "USER"
    return f"{prefix}-{token[:8]}"


async def sync_users_and_access(
    conn: psycopg.AsyncConnection,
    login_users: dict[str, str],
    access_users: dict[str, str],
    access_rows: list[tuple[str, str, str]],
    assignment_rows: list[tuple[str, str, str]],
    workload_admin_user_keys: Iterable[str],
    view_all_user_keys: Iterable[str] = (),
) -> list[dict[str, str | bool]]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT user_key, display_name
            FROM notion_sync.app_user;
            """
        )
        existing_rows = await cur.fetchall()

    existing_users = {
        row["user_key"]: row
        for row in existing_rows
    }
    salt = _password_salt_bytes()
    active_user_keys = set(access_users)
    login_enabled_user_keys = set(login_users)
    workload_admin_keys = normalize_user_keys(workload_admin_user_keys)
    view_all_keys = normalize_user_keys(view_all_user_keys)
    created_login_users: list[dict[str, str | bool]] = []

    async with conn.cursor() as cur:
        for user_key, display_name in access_users.items():
            existing = existing_users.get(user_key)
            can_login = user_key in login_enabled_user_keys
            can_view_workload = user_key in workload_admin_keys
            can_view_all = user_key in view_all_keys
            if existing:
                await cur.execute(
                    """
                    UPDATE notion_sync.app_user
                    SET display_name = %s,
                        is_active = true,
                        can_login = %s,
                        can_view_workload = %s,
                        can_view_all = %s,
                        generated_password = NULL,
                        updated_at = NOW()
                    WHERE user_key = %s;
                    """,
                    (display_name, can_login, can_view_workload, can_view_all, user_key),
                )
                continue

            password = generate_temporary_password(display_name)
            await cur.execute(
                """
                INSERT INTO notion_sync.app_user (
                  user_key,
                  display_name,
                  password_hash,
                  password_salt,
                  generated_password,
                  is_active,
                  can_login,
                  can_view_workload,
                  can_view_all
                )
                VALUES (%s, %s, %s, %s, %s, true, %s, %s, %s);
                """,
                (
                    user_key,
                    display_name,
                    _hash_password(password, salt),
                    base64.b64encode(salt).decode("ascii"),
                    None,
                    can_login,
                    can_view_workload,
                    can_view_all,
                ),
            )
            if can_login:
                created_login_users.append(
                    {
                        "user_key": user_key,
                        "display_name": display_name,
                        "temporary_password": password,
                        "can_view_workload": can_view_workload,
                    }
                )

        if active_user_keys:
            await cur.execute(
                """
                UPDATE notion_sync.app_user
                SET is_active = false,
                    can_login = false,
                    can_view_workload = false,
                    generated_password = NULL,
                    updated_at = NOW()
                WHERE user_key <> ALL(%s);
                """,
                (list(active_user_keys),),
            )
        else:
            await cur.execute(
                """
                UPDATE notion_sync.app_user
                SET is_active = false,
                    can_login = false,
                    can_view_workload = false,
                    generated_password = NULL,
                    updated_at = NOW();
                """
            )

        await cur.execute("DELETE FROM notion_sync.user_entity_access;")
        if access_rows:
            await cur.executemany(
                """
                INSERT INTO notion_sync.user_entity_access (user_key, entity_type, entity_id)
                VALUES (%s, %s, %s::uuid)
                ON CONFLICT DO NOTHING;
                """,
                access_rows,
            )

        await cur.execute("DELETE FROM notion_sync.user_entity_assignment;")
        if assignment_rows:
            await cur.executemany(
                """
                INSERT INTO notion_sync.user_entity_assignment (user_key, entity_type, entity_id)
                VALUES (%s, %s, %s::uuid)
                ON CONFLICT DO NOTHING;
                """,
                assignment_rows,
            )

    return created_login_users


def _verify_password(password: str, stored_hash: str, stored_salt: str | None) -> bool:
    salt = base64.b64decode(stored_salt) if stored_salt else _password_salt_bytes()
    computed = _hash_password(password, salt)
    return secrets.compare_digest(computed, stored_hash)


def _hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


async def list_login_users(conn: psycopg.AsyncConnection) -> list[dict[str, str]]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT user_key, display_name
            FROM notion_sync.app_user
            WHERE is_active = true
              AND can_login = true
            ORDER BY display_name ASC;
            """
        )
        return await cur.fetchall()


async def _clear_expired_sessions(conn: psycopg.AsyncConnection) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM notion_sync.app_session
            WHERE expires_at <= NOW();
            """
        )


async def create_session(
    conn: psycopg.AsyncConnection,
    *,
    user_key: str,
) -> tuple[str, datetime]:
    await _clear_expired_sessions(conn)
    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(tz=timezone.utc) + timedelta(days=SESSION_TTL_DAYS)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO notion_sync.app_session (token_hash, user_key, expires_at)
            VALUES (%s, %s, %s);
            """,
            (_hash_session_token(token), user_key, expires_at),
        )
    return token, expires_at


async def login_user(
    conn: psycopg.AsyncConnection,
    *,
    user_key: str,
    password: str,
) -> AuthenticatedUser | None:
    await _clear_expired_sessions(conn)
    normalized_user_key = normalize_name(user_key)
    if not normalized_user_key:
        return None
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT user_key, display_name, password_hash, password_salt, can_view_workload, can_view_all
            FROM notion_sync.app_user
            WHERE user_key = %s
              AND is_active = true
              AND can_login = true;
            """,
            (normalized_user_key,),
        )
        row = await cur.fetchone()

    if not row or not _verify_password(password, row["password_hash"], row.get("password_salt")):
        return None

    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE notion_sync.app_user
            SET last_login_at = NOW(),
                updated_at = NOW()
            WHERE user_key = %s;
            """,
            (normalized_user_key,),
        )

    return AuthenticatedUser(
        user_key=row["user_key"],
        display_name=row["display_name"],
        can_view_workload=bool(row.get("can_view_workload")),
        can_view_all=bool(row.get("can_view_all")),
    )


async def get_authenticated_user_by_token(
    conn: psycopg.AsyncConnection,
    token: str,
) -> AuthenticatedUser | None:
    if not token:
        return None

    await _clear_expired_sessions(conn)
    token_hash = _hash_session_token(token)

    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT u.user_key, u.display_name, u.can_view_workload, u.can_view_all, s.token_hash, s.expires_at
            FROM notion_sync.app_session s
            JOIN notion_sync.app_user u ON u.user_key = s.user_key
            WHERE s.token_hash = %s
              AND s.expires_at > NOW()
              AND u.is_active = true
              AND u.can_login = true;
            """,
            (token_hash,),
        )
        row = await cur.fetchone()

    if not row:
        return None

    return AuthenticatedUser(
        user_key=row["user_key"],
        display_name=row["display_name"],
        can_view_workload=bool(row.get("can_view_workload")),
        can_view_all=bool(row.get("can_view_all")),
        session_token_hash=row["token_hash"],
        session_expires_at=row["expires_at"],
    )


async def logout_user_session(conn: psycopg.AsyncConnection, token: str) -> None:
    if not token:
        return
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM notion_sync.app_session
            WHERE token_hash = %s;
            """,
            (_hash_session_token(token),),
        )


async def get_scope_summary(conn: psycopg.AsyncConnection, user_key: str) -> dict[str, int]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT entity_type, COUNT(*) AS total
            FROM notion_sync.user_entity_access
            WHERE user_key = %s
            GROUP BY entity_type;
            """,
            (user_key,),
        )
        rows = await cur.fetchall()

    summary = {"project": 0, "product": 0, "task": 0}
    for row in rows:
        summary[row["entity_type"]] = row["total"]
    return summary
