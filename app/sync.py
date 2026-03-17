from __future__ import annotations

import asyncio
import hashlib
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, Iterable

import psycopg
from psycopg import sql
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from .auth import build_access_index, sync_users_and_access
from .config import Settings
from .notion_client import NotionClient


def _normalized_name_set(names: set[str]) -> frozenset[str]:
    normalized_names = set()
    for name in names:
        normalized = unicodedata.normalize("NFKD", name)
        ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
        collapsed = re.sub(r"[^a-zA-Z0-9]+", " ", ascii_name).strip()
        normalized_names.add(" ".join(collapsed.split()))
    return frozenset(normalized_names)


PROJECT_FIXED_PROPERTY_ALIASES: dict[str, tuple[str, ...]] = {
    "nombre": ("Nombre",),
    "fecha": ("Fecha",),
    "area_unidad": ("Area/Unidad",),
    "productos": ("Productos",),
    "fases_aprobacion": ("Fases de aprobación",),
    "fase_aprobacion_actual": ("Fases de aprobación Actual", "Fase de aprobación Actual"),
    "lineamiento_estrategico": ("Lineamiento Estratégico",),
    "objetivos_estrategicos": ("Objetivos Estratégicos",),
    "reuniones_backend": ("Reuniones Backend",),
}

PRODUCT_FIXED_PROPERTY_ALIASES: dict[str, tuple[str, ...]] = {
    "auto_id": ("Auto ID",),
    "nombre": ("Nombre",),
    "descripcion": ("Descripción",),
    "notas": ("Notas",),
    "estado": ("Estado",),
    "prioridad": ("Prioridad",),
    "area_unidad": ("Área/Unidad", "Area/Unidad"),
    "cliente": ("Cliente",),
    "contraparte": ("Contraparte",),
    "tipo_producto": ("Tipo de Producto",),
    "personas": ("Personas",),
    "responsable": ("Responsable",),
    "fecha_entrega": ("Fecha de Entrega",),
    "fases_aprobacion": ("Fases de Aprobacion", "Fases de aprobación"),
    "fase_aprobacion_actual": ("Fase de Aprobación Actual", "Fases de aprobación Actual"),
    "hito": ("Hito",),
    "alertas": ("Alertas",),
    "mes": ("Mes",),
    "trimestre": ("Trimestre",),
    "vencimiento": ("Vencimiento",),
    "rango_dias": ("Rango - Dias", "Rango - Días"),
    "proyectos": ("Proyectos",),
    "tareas_backend": ("Tareas Backend",),
    "bloqueando": ("Bloqueando",),
    "bloqueado_por": ("Bloqueado por",),
}

TASK_FIXED_PROPERTY_ALIASES: dict[str, tuple[str, ...]] = {
    "tarea": ("Tarea",),
    "estado": ("Estado",),
    "fecha": ("Fecha",),
    "area_unidad": ("Área/Unidad", "Area/Unidad"),
    "importancia": ("Importancia",),
    "responsable": ("Responsable",),
    "asignado": ("Asignado",),
    "hito": ("Hito",),
    "contraparte": ("Contraparte",),
    "colaborador_contraparte": ("Colaborador - Contraparte",),
    "vencimiento": ("Vencimiento",),
    "productos": ("Productos (Hub)", "Productos"),
    "bloqueando": ("Bloqueando",),
    "bloqueado_por": ("Bloqueado por",),
}


def _handled_property_names(alias_map: dict[str, tuple[str, ...]]) -> frozenset[str]:
    all_names = {alias for aliases in alias_map.values() for alias in aliases}
    return _normalized_name_set(all_names)


PROJECT_FIXED_PROPERTY_NAMES = _handled_property_names(PROJECT_FIXED_PROPERTY_ALIASES)
PRODUCT_FIXED_PROPERTY_NAMES = _handled_property_names(PRODUCT_FIXED_PROPERTY_ALIASES)
TASK_FIXED_PROPERTY_NAMES = _handled_property_names(TASK_FIXED_PROPERTY_ALIASES)

SOURCE_TABLE_CONFIGS = {
    "projects": {
        "table_name": "notion_sync.proyectos",
        "handled_properties": PROJECT_FIXED_PROPERTY_NAMES,
        "fixed_property_aliases": PROJECT_FIXED_PROPERTY_ALIASES,
    },
    "products": {
        "table_name": "notion_sync.productos",
        "handled_properties": PRODUCT_FIXED_PROPERTY_NAMES,
        "fixed_property_aliases": PRODUCT_FIXED_PROPERTY_ALIASES,
    },
    "tasks": {
        "table_name": "notion_sync.tareas",
        "handled_properties": TASK_FIXED_PROPERTY_NAMES,
        "fixed_property_aliases": TASK_FIXED_PROPERTY_ALIASES,
    },
}

def _extract_title(prop: Any) -> str | None:
    if not prop or prop.get("type") != "title":
        return None
    texts = prop.get("title", [])
    return "".join([t.get("plain_text", "") for t in texts]) or None


def _extract_rich_text(prop: Any) -> str | None:
    if not prop or prop.get("type") != "rich_text":
        return None
    texts = prop.get("rich_text", [])
    return "".join([t.get("plain_text", "") for t in texts]) or None


def _extract_select(prop: Any) -> str | None:
    if not prop:
        return None
    ptype = prop.get("type")
    if ptype == "select":
        sel = prop.get("select")
        return sel.get("name") if sel else None
    if ptype == "status":
        status = prop.get("status")
        return status.get("name") if status else None
    return None


def _extract_multi_select(prop: Any) -> list[dict[str, Any]]:
    if not prop or prop.get("type") != "multi_select":
        return []
    return prop.get("multi_select", []) or []


def _extract_people(prop: Any) -> list[dict[str, Any]]:
    if not prop or prop.get("type") not in {"people", "person"}:
        return []
    return prop.get("people", []) or []


def _extract_number(prop: Any) -> float | None:
    if not prop or prop.get("type") != "number":
        return None
    return prop.get("number")


def _extract_checkbox(prop: Any) -> bool | None:
    if not prop or prop.get("type") != "checkbox":
        return None
    return prop.get("checkbox")


def _extract_date(prop: Any) -> tuple[str | None, str | None, bool | None]:
    if not prop or prop.get("type") != "date":
        return None, None, None
    date_val = prop.get("date") or {}
    start = date_val.get("start")
    end = date_val.get("end")
    is_datetime = None
    if start:
        is_datetime = "T" in start
    return start, end, is_datetime


def _extract_relation_ids(prop: Any) -> list[str]:
    if not prop or prop.get("type") != "relation":
        return []
    rels = prop.get("relation", []) or []
    return [r.get("id") for r in rels if r.get("id")]


def _extract_url(prop: Any) -> str | None:
    if not prop or prop.get("type") != "url":
        return None
    return prop.get("url")


def _normalize_property_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    collapsed = re.sub(r"[^a-zA-Z0-9]+", " ", ascii_name).strip()
    return " ".join(collapsed.split())


def _split_table_name(table_name: str) -> tuple[str, str]:
    schema_name, bare_table_name = table_name.split(".", 1)
    return schema_name, bare_table_name


def _page_properties_by_id(page: dict[str, Any]) -> dict[str, dict[str, Any]]:
    properties = page.get("properties", {})
    return {
        prop.get("id"): prop
        for prop in properties.values()
        if isinstance(prop, dict) and prop.get("id")
    }


def _bound_prop(
    properties_by_id: dict[str, dict[str, Any]],
    bindings: dict[str, str],
    canonical_name: str,
) -> dict[str, Any] | None:
    property_id = bindings.get(canonical_name)
    if not property_id:
        return None
    return properties_by_id.get(property_id)


def _dynamic_pg_type(prop_def: dict[str, Any]) -> str:
    ptype = prop_def.get("type")
    if ptype in {"title", "rich_text", "select", "status", "email", "phone_number", "url", "unique_id"}:
        return "text"
    if ptype == "number":
        return "double precision"
    if ptype == "checkbox":
        return "boolean"
    return "jsonb"


def _dynamic_column_name(property_name: str, property_id: str, existing_columns: set[str]) -> str:
    normalized = _normalize_property_name(property_name).lower().replace(" ", "_")
    base_name = f"prop_{normalized}" if normalized else "prop_property"
    candidate = base_name[:63]
    if candidate not in existing_columns:
        return candidate

    suffix = hashlib.sha1(property_id.encode("utf-8")).hexdigest()[:8]
    trimmed = base_name[: 63 - len(suffix) - 1]
    candidate = f"{trimmed}_{suffix}"
    if candidate not in existing_columns:
        return candidate

    serial = 1
    while True:
        extra_suffix = f"{suffix}_{serial}"
        trimmed = base_name[: 63 - len(extra_suffix) - 1]
        candidate = f"{trimmed}_{extra_suffix}"
        if candidate not in existing_columns:
            return candidate
        serial += 1


async def _table_columns(conn: psycopg.AsyncConnection, table_name: str) -> set[str]:
    schema_name, bare_table_name = _split_table_name(table_name)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s;
            """,
            (schema_name, bare_table_name),
        )
        rows = await cur.fetchall()
        return {
            row["column_name"] if isinstance(row, dict) else row[0]
            for row in rows
        }


async def _dynamic_metadata(
    conn: psycopg.AsyncConnection, table_name: str
) -> dict[str, dict[str, str]]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT property_id, property_name, property_type, column_name, pg_type
            FROM notion_sync.dynamic_property_map
            WHERE table_name = %s;
            """,
            (table_name,),
        )
        rows = await cur.fetchall()

    metadata: dict[str, dict[str, str]] = {}
    for row in rows:
        if isinstance(row, dict):
            property_id = row["property_id"]
            property_name = row["property_name"]
            property_type = row["property_type"]
            column_name = row["column_name"]
            pg_type = row["pg_type"]
        else:
            property_id, property_name, property_type, column_name, pg_type = row

        metadata[property_id] = {
            "property_name": property_name,
            "property_type": property_type,
            "column_name": column_name,
            "pg_type": pg_type,
        }

    return metadata


async def _fixed_metadata(
    conn: psycopg.AsyncConnection, data_source_id: str
) -> dict[str, dict[str, str]]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT canonical_name, property_id, property_name
            FROM notion_sync.fixed_property_map
            WHERE data_source_id = %s;
            """,
            (data_source_id,),
        )
        rows = await cur.fetchall()

    metadata: dict[str, dict[str, str]] = {}
    for row in rows:
        if isinstance(row, dict):
            canonical_name = row["canonical_name"]
            property_id = row["property_id"]
            property_name = row["property_name"]
        else:
            canonical_name, property_id, property_name = row

        metadata[canonical_name] = {
            "property_id": property_id,
            "property_name": property_name,
        }

    return metadata


async def _upsert_fixed_metadata_row(
    conn: psycopg.AsyncConnection,
    *,
    data_source_id: str,
    canonical_name: str,
    property_id: str,
    property_name: str,
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO notion_sync.fixed_property_map (
              data_source_id, canonical_name, property_id, property_name, updated_at
            )
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (data_source_id, canonical_name) DO UPDATE SET
              property_id = EXCLUDED.property_id,
              property_name = EXCLUDED.property_name,
              updated_at = NOW();
            """,
            (data_source_id, canonical_name, property_id, property_name),
        )


async def _resolve_fixed_property_bindings(
    conn: psycopg.AsyncConnection,
    *,
    data_source_id: str,
    properties: dict[str, dict[str, Any]],
    aliases: dict[str, tuple[str, ...]],
) -> dict[str, str]:
    metadata = await _fixed_metadata(conn, data_source_id)
    property_ids_by_normalized_name: dict[str, str] = {}
    property_names_by_id: dict[str, str] = {}

    for property_name, prop_def in properties.items():
        property_id = prop_def.get("id")
        if not property_id:
            continue
        property_ids_by_normalized_name.setdefault(_normalize_property_name(property_name), property_id)
        property_names_by_id[property_id] = property_name

    bindings: dict[str, str] = {}
    for canonical_name, candidate_aliases in aliases.items():
        property_id: str | None = None
        for alias in candidate_aliases:
            property_id = property_ids_by_normalized_name.get(_normalize_property_name(alias))
            if property_id:
                break

        if property_id is None:
            stored = metadata.get(canonical_name)
            stored_id = stored["property_id"] if stored else None
            if stored_id in property_names_by_id:
                property_id = stored_id

        if property_id is None:
            continue

        bindings[canonical_name] = property_id
        await _upsert_fixed_metadata_row(
            conn,
            data_source_id=data_source_id,
            canonical_name=canonical_name,
            property_id=property_id,
            property_name=property_names_by_id[property_id],
        )

    return bindings


async def _add_dynamic_column(
    conn: psycopg.AsyncConnection, table_name: str, column_name: str, pg_type: str
) -> None:
    schema_name, bare_table_name = _split_table_name(table_name)
    stmt = sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}").format(
        sql.Identifier(schema_name),
        sql.Identifier(bare_table_name),
        sql.Identifier(column_name),
        sql.SQL(pg_type),
    )
    async with conn.cursor() as cur:
        await cur.execute(stmt)


async def _promote_dynamic_column_to_jsonb(
    conn: psycopg.AsyncConnection, table_name: str, column_name: str
) -> None:
    schema_name, bare_table_name = _split_table_name(table_name)
    stmt = sql.SQL(
        """
        ALTER TABLE {}.{}
        ALTER COLUMN {} TYPE jsonb
        USING CASE
          WHEN {} IS NULL THEN NULL
          ELSE to_jsonb({})
        END;
        """
    ).format(
        sql.Identifier(schema_name),
        sql.Identifier(bare_table_name),
        sql.Identifier(column_name),
        sql.Identifier(column_name),
        sql.Identifier(column_name),
    )
    async with conn.cursor() as cur:
        await cur.execute(stmt)


async def _upsert_dynamic_metadata_row(
    conn: psycopg.AsyncConnection,
    *,
    table_name: str,
    data_source_id: str,
    property_id: str,
    property_name: str,
    property_type: str,
    column_name: str,
    pg_type: str,
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO notion_sync.dynamic_property_map (
              table_name, data_source_id, property_id, property_name,
              property_type, column_name, pg_type, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (table_name, property_id) DO UPDATE SET
              data_source_id = EXCLUDED.data_source_id,
              property_name = EXCLUDED.property_name,
              property_type = EXCLUDED.property_type,
              column_name = EXCLUDED.column_name,
              pg_type = EXCLUDED.pg_type,
              updated_at = NOW();
            """,
            (
                table_name,
                data_source_id,
                property_id,
                property_name,
                property_type,
                column_name,
                pg_type,
            ),
        )


async def _ensure_dynamic_properties_for_table(
    conn: psycopg.AsyncConnection,
    *,
    table_name: str,
    data_source_id: str,
    properties: dict[str, dict[str, Any]],
    handled_properties: frozenset[str],
    handled_property_ids: set[str],
) -> dict[str, dict[str, str]]:
    existing_columns = await _table_columns(conn, table_name)
    metadata = await _dynamic_metadata(conn, table_name)
    mappings: dict[str, dict[str, str]] = {}

    for property_name, prop_def in sorted(properties.items()):
        normalized_name = _normalize_property_name(property_name)
        property_id = prop_def.get("id")
        if not property_id:
            continue
        if normalized_name in handled_properties or property_id in handled_property_ids:
            continue

        property_type = prop_def.get("type") or "unknown"

        metadata_row = metadata.get(property_id)
        if metadata_row is None:
            pg_type = _dynamic_pg_type(prop_def)
            column_name = _dynamic_column_name(property_name, property_id, existing_columns)
            await _add_dynamic_column(conn, table_name, column_name, pg_type)
            existing_columns.add(column_name)
        else:
            column_name = metadata_row["column_name"]
            pg_type = metadata_row["pg_type"]
            if column_name not in existing_columns:
                await _add_dynamic_column(conn, table_name, column_name, pg_type)
                existing_columns.add(column_name)

            if property_type != metadata_row["property_type"] and pg_type != "jsonb":
                await _promote_dynamic_column_to_jsonb(conn, table_name, column_name)
                pg_type = "jsonb"

        await _upsert_dynamic_metadata_row(
            conn,
            table_name=table_name,
            data_source_id=data_source_id,
            property_id=property_id,
            property_name=property_name,
            property_type=property_type,
            column_name=column_name,
            pg_type=pg_type,
        )

        mappings[property_id] = {
            "column_name": column_name,
            "pg_type": pg_type,
            "property_type": property_type,
            "property_name": property_name,
        }

    return mappings


async def _ensure_dynamic_property_support(
    conn: psycopg.AsyncConnection,
    source_schemas: dict[str, dict[str, Any]],
    fixed_bindings: dict[str, dict[str, str]],
) -> dict[str, dict[str, dict[str, str]]]:
    mappings: dict[str, dict[str, dict[str, str]]] = {}
    for source_name, config in SOURCE_TABLE_CONFIGS.items():
        schema = source_schemas[source_name]
        mappings[source_name] = await _ensure_dynamic_properties_for_table(
            conn,
            table_name=config["table_name"],
            data_source_id=schema["target_id"],
            properties=schema["properties"],
            handled_properties=config["handled_properties"],
            handled_property_ids=set(fixed_bindings[source_name].values()),
        )
    return mappings


def _dynamic_json_value(prop: Any) -> Any:
    if not prop:
        return None

    ptype = prop.get("type")
    if ptype == "date":
        start, end, is_datetime = _extract_date(prop)
        if start is None and end is None and is_datetime is None:
            return None
        return {"start": start, "end": end, "is_datetime": is_datetime}
    if ptype == "relation":
        return _extract_relation_ids(prop)
    if ptype == "people":
        return prop.get("people", []) or []
    if ptype == "multi_select":
        return prop.get("multi_select", []) or []
    if ptype == "files":
        return prop.get("files", []) or []
    if ptype in {"created_by", "last_edited_by"}:
        return prop.get(ptype)
    if ptype == "formula":
        return prop.get("formula")
    if ptype == "rollup":
        return prop.get("rollup")
    if ptype == "verification":
        return prop.get("verification")
    if ptype == "button":
        return prop.get("button")
    nested_value = prop.get(ptype)
    return nested_value if nested_value is not None else prop


def _dynamic_value(prop: Any, pg_type: str) -> Any:
    if not prop:
        return None

    if pg_type == "text":
        ptype = prop.get("type")
        if ptype == "title":
            return _extract_title(prop)
        if ptype == "rich_text":
            return _extract_rich_text(prop)
        if ptype in {"select", "status"}:
            return _extract_select(prop)
        if ptype == "url":
            return _extract_url(prop)
        if ptype == "email":
            return prop.get("email")
        if ptype == "phone_number":
            return prop.get("phone_number")
        if ptype == "unique_id":
            unique_id = prop.get("unique_id") or {}
            number = unique_id.get("number")
            if number is None:
                return None
            prefix = unique_id.get("prefix") or ""
            return f"{prefix}{number}"
        nested_value = prop.get(ptype)
        return nested_value if isinstance(nested_value, str) else None

    if pg_type == "double precision":
        return _extract_number(prop)

    if pg_type == "boolean":
        return _extract_checkbox(prop)

    payload = _dynamic_json_value(prop)
    return Json(payload) if payload is not None else None


def _apply_dynamic_properties(
    record: dict[str, Any],
    page: dict[str, Any],
    mappings: dict[str, dict[str, str]],
) -> None:
    props_by_id = _page_properties_by_id(page)
    for property_id, mapping in mappings.items():
        record[mapping["column_name"]] = _dynamic_value(props_by_id.get(property_id), mapping["pg_type"])


def _parse_project(
    page: dict[str, Any],
    bindings: dict[str, str],
) -> tuple[Dict[str, Any], list[tuple[str, str]]]:
    props_by_id = _page_properties_by_id(page)
    name = _extract_title(_bound_prop(props_by_id, bindings, "nombre"))
    fecha_prop = _bound_prop(props_by_id, bindings, "fecha")
    fecha_start, fecha_end, fecha_is_datetime = _extract_date(fecha_prop)

    record: Dict[str, Any] = {
        "notion_page_id": page["id"],
        "notion_url": page.get("url"),
        "nombre": name,
        "fecha_start": fecha_start,
        "fecha_end": fecha_end,
        "fecha_is_datetime": fecha_is_datetime,
        "area_unidad": Json(_extract_multi_select(_bound_prop(props_by_id, bindings, "area_unidad"))),
        "fases_aprobacion": Json(_extract_multi_select(_bound_prop(props_by_id, bindings, "fases_aprobacion"))),
        "fase_aprobacion_actual": _extract_select(_bound_prop(props_by_id, bindings, "fase_aprobacion_actual")),
        "lineamiento_estrategico_rel": Json(
            _extract_relation_ids(_bound_prop(props_by_id, bindings, "lineamiento_estrategico"))
        ),
        "objetivos_estrategicos_rel": Json(
            _extract_relation_ids(_bound_prop(props_by_id, bindings, "objetivos_estrategicos"))
        ),
        "reuniones_backend_rel": Json(
            _extract_relation_ids(_bound_prop(props_by_id, bindings, "reuniones_backend"))
        ),
        "created_time": page.get("created_time"),
        "last_edited_time": page.get("last_edited_time"),
        "archived": page.get("archived", False),
        "synced_at": datetime.now(tz=timezone.utc),
        "raw_notion": Json(page),
    }

    project_product_links = [(page["id"], pid) for pid in _extract_relation_ids(_bound_prop(props_by_id, bindings, "productos"))]

    return record, project_product_links


def _parse_product(
    page: dict[str, Any],
    bindings: dict[str, str],
) -> tuple[Dict[str, Any], Dict[str, list[tuple[str, str]]]]:
    props_by_id = _page_properties_by_id(page)
    fecha_prop = _bound_prop(props_by_id, bindings, "fecha_entrega")
    fecha_start, fecha_end, fecha_is_datetime = _extract_date(fecha_prop)

    record: Dict[str, Any] = {
        "notion_page_id": page["id"],
        "notion_url": page.get("url"),
        "notion_auto_id": _extract_number(_bound_prop(props_by_id, bindings, "auto_id")),
        "nombre": _extract_title(_bound_prop(props_by_id, bindings, "nombre")),
        "descripcion": _extract_rich_text(_bound_prop(props_by_id, bindings, "descripcion")),
        "notas": _extract_rich_text(_bound_prop(props_by_id, bindings, "notas")),
        "estado": _extract_select(_bound_prop(props_by_id, bindings, "estado")),
        "prioridad": _extract_select(_bound_prop(props_by_id, bindings, "prioridad")),
        "area_unidad": Json(_extract_multi_select(_bound_prop(props_by_id, bindings, "area_unidad"))),
        "cliente": Json(_extract_multi_select(_bound_prop(props_by_id, bindings, "cliente"))),
        "contraparte": Json(
            _extract_multi_select(_bound_prop(props_by_id, bindings, "contraparte"))
            or _extract_people(_bound_prop(props_by_id, bindings, "contraparte"))
        ),
        "tipo_producto": _extract_select(_bound_prop(props_by_id, bindings, "tipo_producto")),
        "personas": Json(_extract_people(_bound_prop(props_by_id, bindings, "personas"))),
        "responsable": Json(_extract_people(_bound_prop(props_by_id, bindings, "responsable"))),
        "fecha_entrega_start": fecha_start,
        "fecha_entrega_end": fecha_end,
        "fecha_entrega_is_datetime": fecha_is_datetime,
        "fases_aprobacion": Json(_extract_multi_select(_bound_prop(props_by_id, bindings, "fases_aprobacion"))),
        "fase_aprobacion_actual": _extract_select(_bound_prop(props_by_id, bindings, "fase_aprobacion_actual")),
        "hito": _extract_number(_bound_prop(props_by_id, bindings, "hito")),
        "alertas": _extract_select(_bound_prop(props_by_id, bindings, "alertas"))
        or _extract_rich_text(_bound_prop(props_by_id, bindings, "alertas")),
        "mes": _extract_select(_bound_prop(props_by_id, bindings, "mes")),
        "trimestre": _extract_select(_bound_prop(props_by_id, bindings, "trimestre")),
        "vencimiento": _extract_select(_bound_prop(props_by_id, bindings, "vencimiento"))
        or _extract_rich_text(_bound_prop(props_by_id, bindings, "vencimiento")),
        "rango_dias": _extract_number(_bound_prop(props_by_id, bindings, "rango_dias")),
        "created_time": page.get("created_time"),
        "last_edited_time": page.get("last_edited_time"),
        "creado_por": Json(page.get("created_by")),
        "ultima_edicion_por": Json(page.get("last_edited_by")),
        "archived": page.get("archived", False),
        "synced_at": datetime.now(tz=timezone.utc),
        "raw_notion": Json(page),
    }

    project_links = [(proj_id, page["id"]) for proj_id in _extract_relation_ids(_bound_prop(props_by_id, bindings, "proyectos"))]
    task_links = [(page["id"], t_id) for t_id in _extract_relation_ids(_bound_prop(props_by_id, bindings, "tareas_backend"))]
    blocking = [(page["id"], target) for target in _extract_relation_ids(_bound_prop(props_by_id, bindings, "bloqueando"))]
    blocked_by = [(source, page["id"]) for source in _extract_relation_ids(_bound_prop(props_by_id, bindings, "bloqueado_por"))]

    relations = {
        "projects": project_links,
        "tasks": task_links,
        "blocks": blocking + blocked_by,
    }
    return record, relations


def _parse_task(
    page: dict[str, Any],
    bindings: dict[str, str],
) -> tuple[Dict[str, Any], Dict[str, list[tuple[str, str]]]]:
    props_by_id = _page_properties_by_id(page)
    fecha_prop = _bound_prop(props_by_id, bindings, "fecha")
    fecha_start, fecha_end, fecha_is_datetime = _extract_date(fecha_prop)

    record: Dict[str, Any] = {
        "notion_page_id": page["id"],
        "notion_url": page.get("url"),
        "tarea": _extract_title(_bound_prop(props_by_id, bindings, "tarea")),
        "estado": _extract_select(_bound_prop(props_by_id, bindings, "estado")),
        "fecha_start": fecha_start,
        "fecha_end": fecha_end,
        "fecha_is_datetime": fecha_is_datetime,
        "area_unidad": Json(_extract_multi_select(_bound_prop(props_by_id, bindings, "area_unidad"))),
        "importancia": _extract_select(_bound_prop(props_by_id, bindings, "importancia")),
        "responsable": Json(_extract_people(_bound_prop(props_by_id, bindings, "responsable"))),
        "asignado": Json(_extract_people(_bound_prop(props_by_id, bindings, "asignado"))),
        "hito": _extract_number(_bound_prop(props_by_id, bindings, "hito")),
        "contraparte": Json(
            _extract_people(_bound_prop(props_by_id, bindings, "contraparte"))
            or _extract_multi_select(_bound_prop(props_by_id, bindings, "contraparte"))
        ),
        "colaborador_contraparte": Json(
            _extract_people(_bound_prop(props_by_id, bindings, "colaborador_contraparte"))
        ),
        "vencimiento": _extract_select(_bound_prop(props_by_id, bindings, "vencimiento"))
        or _extract_rich_text(_bound_prop(props_by_id, bindings, "vencimiento")),
        "created_time": page.get("created_time"),
        "last_edited_time": page.get("last_edited_time"),
        "archived": page.get("archived", False),
        "synced_at": datetime.now(tz=timezone.utc),
        "raw_notion": Json(page),
    }

    product_links = [
        (pid, page["id"])
        for pid in _extract_relation_ids(_bound_prop(props_by_id, bindings, "productos"))
    ]
    blocking = [(page["id"], target) for target in _extract_relation_ids(_bound_prop(props_by_id, bindings, "bloqueando"))]
    blocked_by = [(source, page["id"]) for source in _extract_relation_ids(_bound_prop(props_by_id, bindings, "bloqueado_por"))]

    relations = {
        "products": product_links,
        "blocks": blocking + blocked_by,
    }
    return record, relations


async def perform_sync(pool: AsyncConnectionPool, settings: Settings, notion: NotionClient) -> Dict[str, Any]:
    started = datetime.now(tz=timezone.utc)

    project_source, product_source, task_source = await asyncio.gather(
        notion.get_source_schema(settings.db_proyectos_id),
        notion.get_source_schema(settings.db_productos_id),
        notion.get_source_schema(settings.db_tareas_id),
    )
    projects_raw, products_raw, tasks_raw = await asyncio.gather(
        notion.query_all_pages(project_source["target_type"], project_source["target_id"]),
        notion.query_all_pages(product_source["target_type"], product_source["target_id"]),
        notion.query_all_pages(task_source["target_type"], task_source["target_id"]),
    )

    projects: list[dict[str, Any]] = []
    products: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []

    project_ids: set[str] = set()
    product_ids: set[str] = set()
    task_ids: set[str] = set()

    project_product_link_candidates: list[tuple[str, str]] = []
    product_task_link_candidates: list[tuple[str, str]] = []
    product_block_candidates: list[tuple[str, str]] = []
    task_block_candidates: list[tuple[str, str]] = []

    source_schemas = {
        "projects": project_source,
        "products": product_source,
        "tasks": task_source,
    }

    async with pool.connection() as conn:
        async with conn.transaction():
            fixed_bindings = {
                source_name: await _resolve_fixed_property_bindings(
                    conn,
                    data_source_id=source_schemas[source_name]["target_id"],
                    properties=source_schemas[source_name]["properties"],
                    aliases=SOURCE_TABLE_CONFIGS[source_name]["fixed_property_aliases"],
                )
                for source_name in SOURCE_TABLE_CONFIGS
            }
            dynamic_mappings = await _ensure_dynamic_property_support(conn, source_schemas, fixed_bindings)

            for page in projects_raw:
                record, links = _parse_project(page, fixed_bindings["projects"])
                _apply_dynamic_properties(record, page, dynamic_mappings["projects"])
                projects.append(record)
                project_ids.add(record["notion_page_id"])
                project_product_link_candidates.extend(links)

            for page in products_raw:
                record, rel = _parse_product(page, fixed_bindings["products"])
                _apply_dynamic_properties(record, page, dynamic_mappings["products"])
                products.append(record)
                product_ids.add(record["notion_page_id"])
                project_product_link_candidates.extend(rel.get("projects", []))
                product_task_link_candidates.extend(rel.get("tasks", []))
                product_block_candidates.extend(rel.get("blocks", []))

            for page in tasks_raw:
                record, rel = _parse_task(page, fixed_bindings["tasks"])
                _apply_dynamic_properties(record, page, dynamic_mappings["tasks"])
                tasks.append(record)
                task_ids.add(record["notion_page_id"])
                product_task_link_candidates.extend(rel.get("products", []))
                task_block_candidates.extend(rel.get("blocks", []))

            project_product_links = {
                (proj_id, prod_id)
                for (proj_id, prod_id) in project_product_link_candidates
                if proj_id in project_ids and prod_id in product_ids
            }
            product_task_links = {
                (prod_id, task_id)
                for (prod_id, task_id) in product_task_link_candidates
                if prod_id in product_ids and task_id in task_ids
            }
            product_blocks = {
                (prod_id, target_id)
                for (prod_id, target_id) in product_block_candidates
                if prod_id in product_ids and target_id in product_ids
            }
            task_blocks = {
                (task_id, target_id)
                for (task_id, target_id) in task_block_candidates
                if task_id in task_ids and target_id in task_ids
            }
            auth_index = build_access_index(
                projects_raw,
                products_raw,
                tasks_raw,
                project_product_links,
                product_task_links,
            )

            await _upsert_rows(conn, SOURCE_TABLE_CONFIGS["projects"]["table_name"], projects)
            await _upsert_rows(conn, SOURCE_TABLE_CONFIGS["products"]["table_name"], products)
            await _upsert_rows(conn, SOURCE_TABLE_CONFIGS["tasks"]["table_name"], tasks)
            await _sync_relation_table(
                conn=conn,
                temp_table="tmp_proyecto_producto",
                target_table="notion_sync.proyecto_producto",
                columns=("proyecto_id", "producto_id"),
                rows=project_product_links,
            )
            await _sync_relation_table(
                conn=conn,
                temp_table="tmp_producto_tarea",
                target_table="notion_sync.producto_tarea",
                columns=("producto_id", "tarea_id"),
                rows=product_task_links,
            )
            await _sync_relation_table(
                conn=conn,
                temp_table="tmp_producto_bloqueo",
                target_table="notion_sync.producto_bloqueo",
                columns=("producto_id", "bloquea_a_producto_id"),
                rows=product_blocks,
            )
            await _sync_relation_table(
                conn=conn,
                temp_table="tmp_tarea_bloqueo",
                target_table="notion_sync.tarea_bloqueo",
                columns=("tarea_id", "bloquea_a_tarea_id"),
                rows=task_blocks,
            )
            new_login_users = await sync_users_and_access(
                conn,
                auth_index["login_users"],
                auth_index["access_users"],
                auth_index["access_rows"],
                auth_index["assignment_rows"],
                settings.workload_admin_user_keys,
                settings.view_all_user_keys,
            )

    finished = datetime.now(tz=timezone.utc)
    duration = (finished - started).total_seconds()
    return {
        "counts": {
            "projects": len(projects),
            "products": len(products),
            "tasks": len(tasks),
            "project_product_links": len(project_product_links),
            "product_task_links": len(product_task_links),
            "product_blocks": len(product_blocks),
            "task_blocks": len(task_blocks),
        },
        "new_login_users": new_login_users,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "duration_seconds": duration,
    }


async def _upsert_rows(
    conn: psycopg.AsyncConnection,
    table_name: str,
    rows: Iterable[dict[str, Any]],
    conflict_columns: tuple[str, ...] = ("notion_page_id",),
) -> None:
    materialized_rows = [dict(row) for row in rows]
    if not materialized_rows:
        return

    columns: list[str] = []
    seen_columns: set[str] = set()
    for row in materialized_rows:
        for column_name in row.keys():
            if column_name not in seen_columns:
                seen_columns.add(column_name)
                columns.append(column_name)

    for row in materialized_rows:
        for column_name in columns:
            row.setdefault(column_name, None)

    schema_name, bare_table_name = _split_table_name(table_name)
    insert_columns = sql.SQL(", ").join(sql.Identifier(column_name) for column_name in columns)
    insert_values = sql.SQL(", ").join(sql.Placeholder(column_name) for column_name in columns)
    conflict_clause = sql.SQL(", ").join(sql.Identifier(column_name) for column_name in conflict_columns)
    update_columns = [column_name for column_name in columns if column_name not in conflict_columns]

    if update_columns:
        updates = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(
                sql.Identifier(column_name),
                sql.Identifier(column_name),
            )
            for column_name in update_columns
        )
        statement = sql.SQL(
            "INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(bare_table_name),
            insert_columns,
            insert_values,
            conflict_clause,
            updates,
        )
    else:
        statement = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING").format(
            sql.Identifier(schema_name),
            sql.Identifier(bare_table_name),
            insert_columns,
            insert_values,
            conflict_clause,
        )

    async with conn.cursor() as cur:
        await cur.executemany(statement, materialized_rows)


async def _sync_relation_table(
    conn: psycopg.AsyncConnection,
    temp_table: str,
    target_table: str,
    columns: tuple[str, str],
    rows: set[tuple[str, str]],
) -> None:
    left_col, right_col = columns
    ordered_rows = sorted(rows)

    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            CREATE TEMP TABLE {temp_table} (
              {left_col} uuid NOT NULL,
              {right_col} uuid NOT NULL
            ) ON COMMIT DROP;
            """
        )

        if ordered_rows:
            await cur.executemany(
                f"""
                INSERT INTO {temp_table} ({left_col}, {right_col})
                VALUES (%s::uuid, %s::uuid);
                """,
                ordered_rows,
            )

        await cur.execute(
            f"""
            DELETE FROM {target_table} AS current_rows
            WHERE NOT EXISTS (
              SELECT 1
              FROM {temp_table} AS staged_rows
              WHERE staged_rows.{left_col} = current_rows.{left_col}
                AND staged_rows.{right_col} = current_rows.{right_col}
            );
            """
        )
        await cur.execute(
            f"""
            INSERT INTO {target_table} ({left_col}, {right_col})
            SELECT staged_rows.{left_col}, staged_rows.{right_col}
            FROM {temp_table} AS staged_rows
            LEFT JOIN {target_table} AS current_rows
              ON current_rows.{left_col} = staged_rows.{left_col}
             AND current_rows.{right_col} = staged_rows.{right_col}
            WHERE current_rows.{left_col} IS NULL;
            """
        )
