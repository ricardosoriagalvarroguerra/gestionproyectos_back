from __future__ import annotations

import calendar
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Sequence

import psycopg
from psycopg import sql


SPANISH_MONTH_LABELS = {
    1: ("Enero", "ene"),
    2: ("Febrero", "feb"),
    3: ("Marzo", "mar"),
    4: ("Abril", "abr"),
    5: ("Mayo", "may"),
    6: ("Junio", "jun"),
    7: ("Julio", "jul"),
    8: ("Agosto", "ago"),
    9: ("Septiembre", "sep"),
    10: ("Octubre", "oct"),
    11: ("Noviembre", "nov"),
    12: ("Diciembre", "dic"),
}


TASK_BLOCK_FLAGS_CTE = """
WITH task_block_stats AS (
  SELECT
    tarea_id AS task_id,
    BOOL_OR(true) AS blocks_other_tasks,
    false AS is_blocked
  FROM notion_sync.tarea_bloqueo
  GROUP BY tarea_id
  UNION ALL
  SELECT
    bloquea_a_tarea_id AS task_id,
    false AS blocks_other_tasks,
    BOOL_OR(true) AS is_blocked
  FROM notion_sync.tarea_bloqueo
  GROUP BY bloquea_a_tarea_id
),
task_block_flags AS (
  SELECT
    task_id,
    BOOL_OR(blocks_other_tasks) AS blocks_other_tasks,
    BOOL_OR(is_blocked) AS is_blocked
  FROM task_block_stats
  GROUP BY task_id
)
"""


def _as_array(done_statuses: Sequence[str]) -> list[str]:
    return list(done_statuses)


def _as_utc_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            normalized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            try:
                parsed = datetime.combine(date.fromisoformat(value), time.min, tzinfo=timezone.utc)
            except ValueError:
                return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def _split_table_name(table_name: str) -> tuple[str, str]:
    if "." in table_name:
        schema_name, bare_table_name = table_name.split(".", 1)
        return schema_name, bare_table_name
    return "public", table_name


def _access_exists(entity_type: str, alias: str, column_name: str = "notion_page_id") -> str:
    return f"""
      (
        EXISTS (
          SELECT 1
          FROM notion_sync.app_user _au
          WHERE _au.user_key = %(user_key)s
            AND _au.can_view_all = true
        )
        OR EXISTS (
          SELECT 1
          FROM notion_sync.user_entity_access uea
          WHERE uea.user_key = %(user_key)s
            AND uea.entity_type = '{entity_type}'
            AND uea.entity_id = {alias}.{column_name}
        )
      )
    """


async def _dynamic_property_projection(
    conn: psycopg.AsyncConnection, table_name: str, alias: str
) -> sql.Composed:
    schema_name, bare_table_name = _split_table_name(table_name)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT dpm.property_id, dpm.property_name, dpm.column_name
            FROM notion_sync.dynamic_property_map dpm
            JOIN information_schema.columns cols
              ON cols.table_schema = %s
             AND cols.table_name = %s
             AND cols.column_name = dpm.column_name
            WHERE dpm.table_name = %s
            ORDER BY dpm.property_name ASC;
            """,
            (schema_name, bare_table_name, table_name),
        )
        rows = await cur.fetchall()

    if not rows:
        return sql.SQL("'{}'::jsonb AS extra_properties")

    arguments: list[sql.Composable] = []
    for row in rows:
        if isinstance(row, dict):
            property_id = row["property_id"]
            property_name = row["property_name"]
            column_name = row["column_name"]
        else:
            property_id, property_name, column_name = row
        arguments.append(sql.Literal(property_id))
        arguments.append(
            sql.SQL(
                "CASE WHEN {}.{} IS NULL THEN NULL ELSE jsonb_build_object('label', {}, 'value', {}.{}) END"
            ).format(
                sql.Identifier(alias),
                sql.Identifier(column_name),
                sql.Literal(property_name),
                sql.Identifier(alias),
                sql.Identifier(column_name),
            )
        )

    return sql.SQL("jsonb_strip_nulls(jsonb_build_object({})) AS extra_properties").format(
        sql.SQL(", ").join(arguments)
    )


async def fetch_projects(
    conn: psycopg.AsyncConnection, done_statuses: Sequence[str], user_key: str
) -> List[Dict[str, Any]]:
    extra_properties = await _dynamic_property_projection(conn, "notion_sync.proyectos", "p")
    extra_properties_str = extra_properties.as_string(conn).replace("%", "%%")
    query = f"""
    WITH prod AS (
      SELECT
        pp.proyecto_id,
        COUNT(*) AS products_total,
        SUM(CASE WHEN pr.estado = ANY(%(done)s) THEN 1 ELSE 0 END) AS products_done
      FROM notion_sync.proyecto_producto pp
      JOIN notion_sync.productos pr ON pr.notion_page_id = pp.producto_id
      WHERE pr.archived = false
        AND {_access_exists("product", "pr")}
      GROUP BY pp.proyecto_id
    ),
    task AS (
      SELECT
        pp.proyecto_id,
        COUNT(t.notion_page_id) AS tasks_total,
        SUM(CASE WHEN t.estado = ANY(%(done)s) THEN 1 ELSE 0 END) AS tasks_done,
        SUM(CASE WHEN COALESCE(t.fecha_end, t.fecha_start) < NOW()
                 AND t.estado <> ALL(%(done)s)
                 THEN 1 ELSE 0 END) AS tasks_overdue
      FROM notion_sync.proyecto_producto pp
      JOIN notion_sync.producto_tarea pt ON pt.producto_id = pp.producto_id
      JOIN notion_sync.tareas t ON t.notion_page_id = pt.tarea_id
      WHERE t.archived = false
        AND {_access_exists("task", "t")}
      GROUP BY pp.proyecto_id
    )
    SELECT
      p.notion_page_id AS project_id,
      p.nombre,
      p.notion_url,
      p.area_unidad,
      p.fase_aprobacion_actual,
      p.fecha_start,
      p.fecha_end,
      p.synced_at,
      COALESCE(prod.products_total,0) AS products_total,
      COALESCE(prod.products_done,0) AS products_done,
      COALESCE(task.tasks_total,0) AS tasks_total,
      COALESCE(task.tasks_done,0) AS tasks_done,
      COALESCE(task.tasks_overdue,0) AS tasks_overdue,
      CASE WHEN COALESCE(task.tasks_total,0) = 0 THEN 0
           ELSE ROUND(100.0 * COALESCE(task.tasks_done,0) / task.tasks_total, 1)
      END AS progress_pct,
      {extra_properties_str}
    FROM notion_sync.proyectos p
    LEFT JOIN prod ON prod.proyecto_id = p.notion_page_id
    LEFT JOIN task ON task.proyecto_id = p.notion_page_id
    WHERE p.archived = false
      AND {_access_exists("project", "p")}
    ORDER BY p.last_edited_time DESC;
    """
    async with conn.cursor() as cur:
        await cur.execute(query, {"done": _as_array(done_statuses), "user_key": user_key})
        return await cur.fetchall()


async def fetch_home_project_products(
    conn: psycopg.AsyncConnection, done_statuses: Sequence[str], user_key: str
) -> Dict[str, List[Dict[str, Any]]]:
    extra_properties = await _dynamic_property_projection(conn, "notion_sync.productos", "pr")
    extra_properties_str = extra_properties.as_string(conn).replace("%", "%%")
    query = f"""
    WITH tstats AS (
      SELECT
        pt.producto_id,
        COUNT(*) AS tasks_total,
        SUM(CASE WHEN t.estado = ANY(%(done)s) THEN 1 ELSE 0 END) AS tasks_done,
        SUM(CASE WHEN COALESCE(t.fecha_end, t.fecha_start) < NOW()
                 AND t.estado <> ALL(%(done)s)
                 THEN 1 ELSE 0 END) AS tasks_overdue
      FROM notion_sync.producto_tarea pt
      JOIN notion_sync.tareas t ON t.notion_page_id = pt.tarea_id
      WHERE t.archived = false
        AND {_access_exists("task", "t")}
      GROUP BY pt.producto_id
    )
    SELECT
      pp.proyecto_id AS project_id,
      pr.notion_page_id AS product_id,
      pr.nombre,
      pr.hito,
      pr.estado,
      pr.prioridad,
      pr.responsable,
      pr.fecha_entrega_start,
      pr.fecha_entrega_end,
      pr.notion_url,
      COALESCE(tstats.tasks_total,0) AS tasks_total,
      COALESCE(tstats.tasks_done,0) AS tasks_done,
      COALESCE(tstats.tasks_overdue,0) AS tasks_overdue,
      CASE WHEN COALESCE(tstats.tasks_total,0) = 0 THEN 0
           ELSE ROUND(100.0 * COALESCE(tstats.tasks_done,0) / tstats.tasks_total, 1)
      END AS progress_pct,
      {extra_properties_str}
    FROM notion_sync.proyecto_producto pp
    JOIN notion_sync.productos pr ON pr.notion_page_id = pp.producto_id
    LEFT JOIN tstats ON tstats.producto_id = pr.notion_page_id
    WHERE pr.archived = false
      AND {_access_exists("project", "pp", "proyecto_id")}
      AND {_access_exists("product", "pr")}
    ORDER BY
      pp.proyecto_id,
      COALESCE(pr.hito, 999999),
      pr.fecha_entrega_start NULLS LAST,
      pr.fecha_entrega_end NULLS LAST,
      pr.last_edited_time DESC;
    """
    async with conn.cursor() as cur:
        await cur.execute(query, {"done": _as_array(done_statuses), "user_key": user_key})
        rows = await cur.fetchall()

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        project_id = str(row["project_id"])
        product = dict(row)
        product.pop("project_id", None)
        grouped.setdefault(project_id, []).append(product)

    return grouped


async def fetch_home_overview(
    conn: psycopg.AsyncConnection, done_statuses: Sequence[str], user_key: str
) -> Dict[str, Any]:
    projects = await fetch_projects(conn, done_statuses, user_key)
    project_products = await fetch_home_project_products(conn, done_statuses, user_key)
    alerts_sql = f"""
    WITH ranked_alerts AS (
      SELECT
        p.notion_page_id AS project_id,
        p.nombre AS project_nombre,
        pr.notion_page_id AS product_id,
        pr.nombre AS product_nombre,
        t.notion_page_id AS task_id,
        t.tarea,
        t.estado,
        t.importancia,
        t.fecha_start,
        t.fecha_end,
        CASE
          WHEN COALESCE(t.fecha_end, t.fecha_start) < NOW()
           AND t.estado <> ALL(%(done)s)
          THEN 'overdue'
          WHEN COALESCE(t.fecha_end, t.fecha_start) >= NOW()
           AND COALESCE(t.fecha_end, t.fecha_start) <= NOW() + INTERVAL '7 days'
           AND t.estado <> ALL(%(done)s)
          THEN 'upcoming'
          ELSE NULL
        END AS alert_type,
        ROW_NUMBER() OVER (
          PARTITION BY
            p.notion_page_id,
            CASE
              WHEN COALESCE(t.fecha_end, t.fecha_start) < NOW()
               AND t.estado <> ALL(%(done)s)
              THEN 'overdue'
              WHEN COALESCE(t.fecha_end, t.fecha_start) >= NOW()
               AND COALESCE(t.fecha_end, t.fecha_start) <= NOW() + INTERVAL '7 days'
               AND t.estado <> ALL(%(done)s)
              THEN 'upcoming'
              ELSE 'ignore'
            END
          ORDER BY COALESCE(t.fecha_end, t.fecha_start) ASC, t.last_edited_time DESC
        ) AS project_rank
      FROM notion_sync.proyectos p
      JOIN notion_sync.proyecto_producto pp ON pp.proyecto_id = p.notion_page_id
      JOIN notion_sync.productos pr ON pr.notion_page_id = pp.producto_id
      JOIN notion_sync.producto_tarea pt ON pt.producto_id = pr.notion_page_id
      JOIN notion_sync.tareas t ON t.notion_page_id = pt.tarea_id
      WHERE p.archived = false
        AND pr.archived = false
        AND t.archived = false
        AND {_access_exists("project", "p")}
        AND {_access_exists("product", "pr")}
        AND {_access_exists("task", "t")}
    )
    SELECT
      project_id,
      project_nombre,
      product_id,
      product_nombre,
      task_id,
      tarea,
      estado,
      importancia,
      fecha_start,
      fecha_end,
      alert_type
    FROM ranked_alerts
    WHERE alert_type IS NOT NULL
      AND project_rank <= 5
    ORDER BY
      CASE WHEN alert_type = 'overdue' THEN 0 ELSE 1 END,
      COALESCE(fecha_end, fecha_start) ASC,
      project_nombre ASC;
    """
    async with conn.cursor() as cur:
        await cur.execute(alerts_sql, {"done": _as_array(done_statuses), "user_key": user_key})
        alerts = await cur.fetchall()

    return {"projects": projects, "alerts": alerts, "project_products": project_products}


async def fetch_products_for_project(
    conn: psycopg.AsyncConnection, project_id: str, done_statuses: Sequence[str], user_key: str
) -> List[Dict[str, Any]]:
    extra_properties = await _dynamic_property_projection(conn, "notion_sync.productos", "pr")
    extra_properties_str = extra_properties.as_string(conn).replace("%", "%%")
    query = f"""
    WITH tstats AS (
      SELECT
        pt.producto_id,
        COUNT(*) AS tasks_total,
        SUM(CASE WHEN t.estado = ANY(%(done)s) THEN 1 ELSE 0 END) AS tasks_done,
        SUM(CASE WHEN COALESCE(t.fecha_end, t.fecha_start) < NOW()
                 AND t.estado <> ALL(%(done)s)
                 THEN 1 ELSE 0 END) AS tasks_overdue
      FROM notion_sync.producto_tarea pt
      JOIN notion_sync.tareas t ON t.notion_page_id = pt.tarea_id
      WHERE t.archived = false
        AND {_access_exists("task", "t")}
      GROUP BY pt.producto_id
    )
    SELECT
      pr.notion_page_id AS product_id,
      pr.nombre,
      pr.hito,
      pr.estado,
      pr.prioridad,
      pr.responsable,
      pr.fecha_entrega_start,
      pr.fecha_entrega_end,
      pr.notion_url,
      COALESCE(tstats.tasks_total,0) AS tasks_total,
      COALESCE(tstats.tasks_done,0) AS tasks_done,
      COALESCE(tstats.tasks_overdue,0) AS tasks_overdue,
      CASE WHEN COALESCE(tstats.tasks_total,0) = 0 THEN 0
           ELSE ROUND(100.0 * COALESCE(tstats.tasks_done,0) / tstats.tasks_total, 1)
      END AS progress_pct,
      {extra_properties_str}
    FROM notion_sync.proyecto_producto pp
    JOIN notion_sync.productos pr ON pr.notion_page_id = pp.producto_id
    LEFT JOIN tstats ON tstats.producto_id = pr.notion_page_id
    WHERE pp.proyecto_id = %(project_id)s::uuid
      AND pr.archived = false
      AND {_access_exists("project", "pp", "proyecto_id")}
      AND {_access_exists("product", "pr")}
    ORDER BY
      COALESCE(pr.hito, 999999),
      pr.fecha_entrega_start NULLS LAST,
      pr.fecha_entrega_end NULLS LAST,
      pr.last_edited_time DESC;
    """
    async with conn.cursor() as cur:
        await cur.execute(
            query,
            {"project_id": project_id, "done": _as_array(done_statuses), "user_key": user_key},
        )
        return await cur.fetchall()


async def fetch_tasks_for_product(
    conn: psycopg.AsyncConnection, product_id: str, done_statuses: Sequence[str], user_key: str
) -> List[Dict[str, Any]]:
    extra_properties = await _dynamic_property_projection(conn, "notion_sync.tareas", "t")
    extra_properties_str = extra_properties.as_string(conn).replace("%", "%%")
    query = f"""
    {TASK_BLOCK_FLAGS_CTE}
    SELECT
      t.notion_page_id AS task_id,
      t.tarea,
      t.hito,
      t.estado,
      t.importancia,
      t.responsable,
      t.asignado,
      t.fecha_start,
      t.fecha_end,
      t.contraparte,
      t.notion_url,
      CASE WHEN COALESCE(t.fecha_end, t.fecha_start) < NOW()
             AND t.estado <> ALL(%(done)s)
           THEN true ELSE false END AS is_overdue,
      COALESCE(tbf.blocks_other_tasks, false) AS blocks_other_tasks,
      COALESCE(tbf.is_blocked, false) AS is_blocked,
      {extra_properties_str}
    FROM notion_sync.producto_tarea pt
    JOIN notion_sync.tareas t ON t.notion_page_id = pt.tarea_id
    LEFT JOIN task_block_flags tbf ON tbf.task_id = t.notion_page_id
    WHERE pt.producto_id = %(product_id)s::uuid
      AND t.archived = false
      AND {_access_exists("product", "pt", "producto_id")}
      AND {_access_exists("task", "t")}
    ORDER BY
      COALESCE(t.fecha_end, t.fecha_start) NULLS LAST,
      t.last_edited_time DESC;
    """
    async with conn.cursor() as cur:
        await cur.execute(
            query,
            {"product_id": product_id, "done": _as_array(done_statuses), "user_key": user_key},
        )
        return await cur.fetchall()


async def fetch_tasks_for_project(
    conn: psycopg.AsyncConnection, project_id: str, done_statuses: Sequence[str], user_key: str
) -> List[Dict[str, Any]]:
    extra_properties = await _dynamic_property_projection(conn, "notion_sync.tareas", "t")
    extra_properties_str = extra_properties.as_string(conn).replace("%", "%%")
    query = f"""
    {TASK_BLOCK_FLAGS_CTE}
    SELECT
      pr.notion_page_id AS product_id,
      pr.nombre AS product_nombre,
      pr.hito AS product_hito,
      t.notion_page_id AS task_id,
      t.tarea,
      t.hito,
      t.estado,
      t.importancia,
      t.responsable,
      t.asignado,
      t.fecha_start,
      t.fecha_end,
      t.contraparte,
      t.notion_url,
      CASE WHEN COALESCE(t.fecha_end, t.fecha_start) < NOW()
             AND t.estado <> ALL(%(done)s)
           THEN true ELSE false END AS is_overdue,
      COALESCE(tbf.blocks_other_tasks, false) AS blocks_other_tasks,
      COALESCE(tbf.is_blocked, false) AS is_blocked,
      {extra_properties_str}
    FROM notion_sync.proyecto_producto pp
    JOIN notion_sync.productos pr ON pr.notion_page_id = pp.producto_id
    JOIN notion_sync.producto_tarea pt ON pt.producto_id = pp.producto_id
    JOIN notion_sync.tareas t ON t.notion_page_id = pt.tarea_id
    LEFT JOIN task_block_flags tbf ON tbf.task_id = t.notion_page_id
    WHERE pp.proyecto_id = %(project_id)s::uuid
      AND pr.archived = false
      AND t.archived = false
      AND {_access_exists("project", "pp", "proyecto_id")}
      AND {_access_exists("product", "pr")}
      AND {_access_exists("task", "t")}
    ORDER BY
      COALESCE(pr.hito, 999999),
      pr.nombre ASC,
      COALESCE(t.fecha_end, t.fecha_start) NULLS LAST,
      t.last_edited_time DESC;
    """
    async with conn.cursor() as cur:
        await cur.execute(
            query,
            {"project_id": project_id, "done": _as_array(done_statuses), "user_key": user_key},
        )
        return await cur.fetchall()


async def fetch_project_dashboard(
    conn: psycopg.AsyncConnection, project_id: str, done_statuses: Sequence[str], user_key: str
) -> Dict[str, Any]:
    kpi_sql = f"""
    WITH prod AS (
      SELECT
        pp.proyecto_id,
        COUNT(*) AS products_total,
        SUM(CASE WHEN pr.estado = ANY(%(done)s) THEN 1 ELSE 0 END) AS products_done
      FROM notion_sync.proyecto_producto pp
      JOIN notion_sync.productos pr ON pr.notion_page_id = pp.producto_id
      WHERE pr.archived = false
        AND {_access_exists("product", "pr")}
      GROUP BY pp.proyecto_id
    ),
    task AS (
      SELECT
        pp.proyecto_id,
        COUNT(t.notion_page_id) AS tasks_total,
        SUM(CASE WHEN t.estado = ANY(%(done)s) THEN 1 ELSE 0 END) AS tasks_done,
        SUM(CASE WHEN COALESCE(t.fecha_end, t.fecha_start) < NOW()
                 AND t.estado <> ALL(%(done)s)
                 THEN 1 ELSE 0 END) AS tasks_overdue
      FROM notion_sync.proyecto_producto pp
      JOIN notion_sync.producto_tarea pt ON pt.producto_id = pp.producto_id
      JOIN notion_sync.tareas t ON t.notion_page_id = pt.tarea_id
      WHERE t.archived = false
        AND {_access_exists("task", "t")}
      GROUP BY pp.proyecto_id
    )
    SELECT
      p.notion_page_id AS project_id,
      p.nombre,
      COALESCE(prod.products_total,0) AS products_total,
      COALESCE(prod.products_done,0) AS products_done,
      COALESCE(task.tasks_total,0) AS tasks_total,
      COALESCE(task.tasks_done,0) AS tasks_done,
      COALESCE(task.tasks_overdue,0) AS tasks_overdue,
      CASE WHEN COALESCE(task.tasks_total,0) = 0 THEN 0
           ELSE ROUND(100.0 * COALESCE(task.tasks_done,0) / task.tasks_total, 1)
      END AS progress_pct
    FROM notion_sync.proyectos p
    LEFT JOIN prod ON prod.proyecto_id = p.notion_page_id
    LEFT JOIN task ON task.proyecto_id = p.notion_page_id
    WHERE p.notion_page_id = %(project_id)s::uuid
      AND p.archived = false
      AND {_access_exists("project", "p")};
    """

    overdue_sql = f"""
    SELECT
      pr.notion_page_id AS product_id,
      pr.nombre AS product_nombre,
      t.tarea,
      t.estado,
      t.importancia,
      t.fecha_start,
      t.fecha_end,
      t.notion_page_id AS task_id
    FROM notion_sync.proyecto_producto pp
    JOIN notion_sync.productos pr ON pr.notion_page_id = pp.producto_id
    JOIN notion_sync.producto_tarea pt ON pt.producto_id = pp.producto_id
    JOIN notion_sync.tareas t ON t.notion_page_id = pt.tarea_id
    WHERE pp.proyecto_id = %(project_id)s::uuid
      AND t.archived = false
      AND {_access_exists("project", "pp", "proyecto_id")}
      AND {_access_exists("product", "pr")}
      AND {_access_exists("task", "t")}
      AND COALESCE(t.fecha_end, t.fecha_start) < NOW()
      AND t.estado <> ALL(%(done)s)
    ORDER BY COALESCE(t.fecha_end, t.fecha_start) ASC
    LIMIT 5;
    """

    upcoming_sql = f"""
    SELECT
      pr.notion_page_id AS product_id,
      pr.nombre AS product_nombre,
      t.tarea,
      t.estado,
      t.importancia,
      t.fecha_start,
      t.fecha_end,
      t.notion_page_id AS task_id
    FROM notion_sync.proyecto_producto pp
    JOIN notion_sync.productos pr ON pr.notion_page_id = pp.producto_id
    JOIN notion_sync.producto_tarea pt ON pt.producto_id = pp.producto_id
    JOIN notion_sync.tareas t ON t.notion_page_id = pt.tarea_id
    WHERE pp.proyecto_id = %(project_id)s::uuid
      AND t.archived = false
      AND {_access_exists("project", "pp", "proyecto_id")}
      AND {_access_exists("product", "pr")}
      AND {_access_exists("task", "t")}
      AND COALESCE(t.fecha_end, t.fecha_start) >= NOW()
      AND COALESCE(t.fecha_end, t.fecha_start) <= NOW() + INTERVAL '7 days'
      AND t.estado <> ALL(%(done)s)
    ORDER BY COALESCE(t.fecha_end, t.fecha_start) ASC
    LIMIT 5;
    """

    review_sql = f"""
    SELECT pr.nombre, pr.estado, pr.fecha_entrega_start, pr.notion_page_id AS product_id
    FROM notion_sync.proyecto_producto pp
    JOIN notion_sync.productos pr ON pr.notion_page_id = pp.producto_id
    WHERE pp.proyecto_id = %(project_id)s::uuid
      AND pr.archived = false
      AND {_access_exists("project", "pp", "proyecto_id")}
      AND {_access_exists("product", "pr")}
      AND pr.estado = 'Revisión'
    ORDER BY pr.last_edited_time ASC
    LIMIT 5;
    """

    async with conn.cursor() as cur:
        params = {"project_id": project_id, "done": _as_array(done_statuses), "user_key": user_key}
        await cur.execute(kpi_sql, params)
        kpi_row = await cur.fetchone() or {}

        await cur.execute(overdue_sql, params)
        overdue_tasks = await cur.fetchall()

        await cur.execute(upcoming_sql, params)
        upcoming_tasks = await cur.fetchall()

        await cur.execute(review_sql, params)
        products_in_review = await cur.fetchall()

    return {
        "kpis": kpi_row,
        "overdue_tasks": overdue_tasks,
        "upcoming_tasks": upcoming_tasks,
        "products_in_review": products_in_review,
    }


async def fetch_timeline(
    conn: psycopg.AsyncConnection,
    project_id: str,
    mode: str,
    product_id: str | None,
    done_statuses: Sequence[str],
    user_key: str,
) -> Dict[str, Any]:
    async with conn.cursor() as cur:
        if mode == "tasks":
            base_sql = f"""
                SELECT
                  pr.notion_page_id AS product_id,
                  pr.nombre AS product_nombre,
                  pr.hito AS product_hito,
                  t.notion_page_id AS id,
                  t.tarea AS label,
                  t.hito AS hito,
                  COALESCE(t.fecha_start, t.created_time) AS start,
                  COALESCE(t.fecha_end, t.fecha_start, t.created_time) AS "end",
                  t.estado AS status,
                  CASE WHEN COALESCE(t.fecha_end, t.fecha_start) < NOW()
                         AND t.estado <> ALL(%(done)s)
                       THEN true ELSE false END AS is_overdue
                FROM notion_sync.producto_tarea pt
                JOIN notion_sync.tareas t ON t.notion_page_id = pt.tarea_id
                JOIN notion_sync.productos pr ON pr.notion_page_id = pt.producto_id
                JOIN notion_sync.proyecto_producto pp ON pp.producto_id = pr.notion_page_id
                WHERE pp.proyecto_id = %(project_id)s::uuid
                  AND pr.archived = false
                  AND t.archived = false
                  AND {_access_exists("project", "pp", "proyecto_id")}
                  AND {_access_exists("product", "pr")}
                  AND {_access_exists("task", "t")}
            """
            params = {"project_id": project_id, "done": _as_array(done_statuses), "user_key": user_key}
            if product_id:
                base_sql += " AND pt.producto_id = %(product_id)s::uuid"
                params["product_id"] = product_id
            base_sql += """
                ORDER BY
                  COALESCE(pr.hito, 999999),
                  pr.nombre ASC,
                  COALESCE(t.fecha_start, t.created_time) NULLS LAST,
                  COALESCE(t.fecha_end, t.fecha_start, t.created_time) NULLS LAST,
                  t.last_edited_time DESC;
            """
            await cur.execute(base_sql, params)
            items = await cur.fetchall()
            for item in items:
                item["group"] = item["product_id"]
                if not product_id and item.get("product_nombre") and item.get("label"):
                    item["label"] = f"{item['product_nombre']} · {item['label']}"
            groups = []
            if product_id:
                groups = [{"id": product_id, "label": "Tareas del producto"}]
            else:
                seen = set()
                for item in items:
                    pid = item.get("product_id")
                    if pid and pid not in seen:
                        seen.add(pid)
                        groups.append({"id": pid, "label": item.get("product_nombre") or "Producto"})
            return {
                "mode": "tasks",
                "product_id": product_id,
                "groups": groups,
                "items": items,
            }

        await cur.execute(
            f"""
            WITH tstats AS (
              SELECT
                pt.producto_id,
                COUNT(*) AS tasks_total,
                SUM(CASE WHEN t.estado = ANY(%(done)s) THEN 1 ELSE 0 END) AS tasks_done
              FROM notion_sync.producto_tarea pt
              JOIN notion_sync.tareas t ON t.notion_page_id = pt.tarea_id
              WHERE t.archived = false
                AND {_access_exists("task", "t")}
              GROUP BY pt.producto_id
            )
            SELECT
              pr.notion_page_id AS id,
              pr.nombre AS label,
              pr.hito AS hito,
              COALESCE(pr.fecha_entrega_start, pr.created_time) AS start,
              COALESCE(pr.fecha_entrega_end, pr.fecha_entrega_start, pr.created_time) AS "end",
              pr.estado AS status,
              CASE WHEN COALESCE(tstats.tasks_total,0) = 0 THEN 0
                   ELSE ROUND(100.0 * COALESCE(tstats.tasks_done,0) / tstats.tasks_total, 1)
              END AS progress_pct
            FROM notion_sync.proyecto_producto pp
            JOIN notion_sync.productos pr ON pr.notion_page_id = pp.producto_id
            LEFT JOIN tstats ON tstats.producto_id = pr.notion_page_id
            WHERE pp.proyecto_id = %(project_id)s::uuid
              AND pr.archived = false
              AND {_access_exists("project", "pp", "proyecto_id")}
              AND {_access_exists("product", "pr")}
            ORDER BY
              COALESCE(pr.hito, 999999),
              pr.nombre ASC,
              COALESCE(pr.fecha_entrega_start, pr.created_time) NULLS LAST,
              COALESCE(pr.fecha_entrega_end, pr.fecha_entrega_start, pr.created_time) NULLS LAST;
            """,
            {"project_id": project_id, "done": _as_array(done_statuses), "user_key": user_key},
        )
        items = await cur.fetchall()
        return {
            "mode": "products",
            "groups": [{"id": "g1", "label": "Productos"}],
            "items": [{**item, "group": "g1"} for item in items],
        }


async def fetch_workload_overview(
    conn: psycopg.AsyncConnection,
    *,
    year: int,
    month: int,
) -> Dict[str, Any]:
    period_start = date(year, month, 1)
    month_days = calendar.monthrange(year, month)[1]
    period_end = date(year, month, month_days)
    month_label, month_short = SPANISH_MONTH_LABELS[month]
    first_slot_start = period_start - timedelta(days=period_start.weekday())
    last_slot_end = period_end + timedelta(days=(6 - period_end.weekday()))

    weeks: list[dict[str, Any]] = []
    cursor = first_slot_start
    index = 1
    while cursor <= last_slot_end:
        slot_end = cursor + timedelta(days=6)
        clipped_start = max(cursor, period_start)
        clipped_end = min(slot_end, period_end)
        weeks.append(
            {
                "id": f"w{index}",
                "index": index,
                "label": f"Sem {index}",
                "range_label": f"{clipped_start.day:02d}-{clipped_end.day:02d} {month_short}",
                "start": datetime.combine(cursor, time.min, tzinfo=timezone.utc),
                "end": datetime.combine(slot_end, time.max, tzinfo=timezone.utc),
            }
        )
        cursor = slot_end + timedelta(days=1)
        index += 1

    async with conn.cursor() as cur:
        await cur.execute(
            """
            WITH user_list AS (
              SELECT user_key, display_name, can_login
              FROM notion_sync.app_user
              WHERE is_active = true
            ),
            project_assignments AS (
              SELECT
                ua.user_key,
                u.display_name,
                u.can_login,
                'project'::text AS entity_type,
                p.notion_page_id AS entity_id,
                p.nombre AS entity_name,
                COALESCE(p.fecha_start, p.created_time) AS start_at,
                COALESCE(p.fecha_end, p.fecha_start, p.created_time) AS end_at
              FROM notion_sync.user_entity_assignment ua
              JOIN user_list u ON u.user_key = ua.user_key
              JOIN notion_sync.proyectos p ON p.notion_page_id = ua.entity_id
              WHERE ua.entity_type = 'project'
                AND p.archived = false
            ),
            product_assignments AS (
              SELECT
                ua.user_key,
                u.display_name,
                u.can_login,
                'product'::text AS entity_type,
                pr.notion_page_id AS entity_id,
                pr.nombre AS entity_name,
                COALESCE(pr.fecha_entrega_start, pr.created_time) AS start_at,
                COALESCE(pr.fecha_entrega_end, pr.fecha_entrega_start, pr.created_time) AS end_at
              FROM notion_sync.user_entity_assignment ua
              JOIN user_list u ON u.user_key = ua.user_key
              JOIN notion_sync.productos pr ON pr.notion_page_id = ua.entity_id
              WHERE ua.entity_type = 'product'
                AND pr.archived = false
            ),
            task_assignments AS (
              SELECT
                ua.user_key,
                u.display_name,
                u.can_login,
                'task'::text AS entity_type,
                t.notion_page_id AS entity_id,
                t.tarea AS entity_name,
                COALESCE(t.fecha_start, t.created_time) AS start_at,
                COALESCE(t.fecha_end, t.fecha_start, t.created_time) AS end_at
              FROM notion_sync.user_entity_assignment ua
              JOIN user_list u ON u.user_key = ua.user_key
              JOIN notion_sync.tareas t ON t.notion_page_id = ua.entity_id
              WHERE ua.entity_type = 'task'
                AND t.archived = false
            )
            SELECT * FROM project_assignments
            UNION ALL
            SELECT * FROM product_assignments
            UNION ALL
            SELECT * FROM task_assignments
            ORDER BY display_name ASC, entity_type ASC, entity_name ASC;
            """
        )
        assignment_rows = await cur.fetchall()

        await cur.execute(
            """
            SELECT user_key, display_name, can_login
            FROM notion_sync.app_user
            WHERE is_active = true
            ORDER BY display_name ASC;
            """
        )
        user_rows = await cur.fetchall()

    users_by_key: dict[str, dict[str, Any]] = {}
    for user in user_rows:
        week_cells = [
            {
                "week_id": week["id"],
                "projects": 0,
                "products": 0,
                "tasks": 0,
                "total": 0,
                "project_names": [],
                "product_names": [],
                "task_names": [],
            }
            for week in weeks
        ]
        users_by_key[user["user_key"]] = {
            "user_key": user["user_key"],
            "display_name": user["display_name"],
            "can_login": user["can_login"],
            "totals": {"projects": 0, "products": 0, "tasks": 0, "total": 0},
            "weeks": week_cells,
        }

    for row in assignment_rows:
        user = users_by_key.get(row["user_key"])
        if not user:
            continue
        start_at = _as_utc_datetime(row["start_at"])
        end_at = _as_utc_datetime(row["end_at"]) or start_at
        if not start_at or not end_at:
            continue
        if end_at < start_at:
            end_at = start_at

        entity_type = row["entity_type"]
        counter_key = f"{entity_type}s"
        entity_name = row["entity_name"] or "Sin nombre"

        for week_index, week in enumerate(weeks):
            if end_at < week["start"] or start_at > week["end"]:
                continue
            week_cell = user["weeks"][week_index]
            week_cell[counter_key] += 1
            week_cell["total"] += 1
            week_cell[f"{entity_type}_names"].append(entity_name)

    for user in users_by_key.values():
        totals = user["totals"]
        for week_cell in user["weeks"]:
            totals["projects"] += week_cell["projects"]
            totals["products"] += week_cell["products"]
            totals["tasks"] += week_cell["tasks"]
            totals["total"] += week_cell["total"]
            week_cell["project_names"].sort()
            week_cell["product_names"].sort()
            week_cell["task_names"].sort()

    users = sorted(users_by_key.values(), key=lambda item: item["display_name"].lower())
    summary = {
        "users": len(users),
        "projects": sum(user["totals"]["projects"] for user in users),
        "products": sum(user["totals"]["products"] for user in users),
        "tasks": sum(user["totals"]["tasks"] for user in users),
        "total": sum(user["totals"]["total"] for user in users),
    }

    return {
        "year": year,
        "month": month,
        "label": f"{month_label} {year}",
        "weeks": [
            {
                "id": week["id"],
                "index": week["index"],
                "label": week["label"],
                "range_label": week["range_label"],
                "start": week["start"].isoformat(),
                "end": week["end"].isoformat(),
            }
            for week in weeks
        ],
        "users": users,
        "summary": summary,
    }
