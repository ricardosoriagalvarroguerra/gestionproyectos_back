from collections.abc import AsyncGenerator
from typing import Optional

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


pool: Optional[AsyncConnectionPool] = None

BASE_SYNC_SCHEMA_STATEMENTS = (
    """
    CREATE SCHEMA IF NOT EXISTS notion_sync;
    """,
    """
    CREATE TABLE IF NOT EXISTS notion_sync.proyectos (
      notion_page_id uuid PRIMARY KEY,
      notion_url text,
      nombre text,
      fecha_start timestamptz,
      fecha_end timestamptz,
      fecha_is_datetime boolean,
      area_unidad jsonb,
      fases_aprobacion jsonb,
      fase_aprobacion_actual text,
      lineamiento_estrategico_rel jsonb,
      objetivos_estrategicos_rel jsonb,
      reuniones_backend_rel jsonb,
      created_time timestamptz,
      last_edited_time timestamptz,
      archived boolean NOT NULL DEFAULT false,
      synced_at timestamptz,
      raw_notion jsonb
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS notion_sync.productos (
      notion_page_id uuid PRIMARY KEY,
      notion_url text,
      notion_auto_id integer,
      nombre text,
      descripcion text,
      notas text,
      estado text,
      prioridad text,
      area_unidad jsonb,
      cliente jsonb,
      contraparte jsonb,
      tipo_producto text,
      personas jsonb,
      responsable jsonb,
      fecha_entrega_start timestamptz,
      fecha_entrega_end timestamptz,
      fecha_entrega_is_datetime boolean,
      fases_aprobacion jsonb,
      fase_aprobacion_actual text,
      hito numeric,
      alertas text,
      mes text,
      trimestre text,
      vencimiento text,
      rango_dias numeric,
      created_time timestamptz,
      last_edited_time timestamptz,
      creado_por jsonb,
      ultima_edicion_por jsonb,
      archived boolean NOT NULL DEFAULT false,
      synced_at timestamptz,
      raw_notion jsonb
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS notion_sync.tareas (
      notion_page_id uuid PRIMARY KEY,
      notion_url text,
      tarea text,
      estado text,
      fecha_start timestamptz,
      fecha_end timestamptz,
      fecha_is_datetime boolean,
      area_unidad jsonb,
      importancia text,
      responsable jsonb,
      asignado jsonb,
      hito numeric,
      contraparte jsonb,
      colaborador_contraparte jsonb,
      vencimiento text,
      created_time timestamptz,
      last_edited_time timestamptz,
      archived boolean NOT NULL DEFAULT false,
      synced_at timestamptz,
      raw_notion jsonb
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS notion_sync.proyecto_producto (
      proyecto_id uuid NOT NULL,
      producto_id uuid NOT NULL,
      PRIMARY KEY (proyecto_id, producto_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS notion_sync.producto_tarea (
      producto_id uuid NOT NULL,
      tarea_id uuid NOT NULL,
      PRIMARY KEY (producto_id, tarea_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS notion_sync.producto_bloqueo (
      producto_id uuid NOT NULL,
      bloquea_a_producto_id uuid NOT NULL,
      PRIMARY KEY (producto_id, bloquea_a_producto_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS notion_sync.tarea_bloqueo (
      tarea_id uuid NOT NULL,
      bloquea_a_tarea_id uuid NOT NULL,
      PRIMARY KEY (tarea_id, bloquea_a_tarea_id)
    );
    """,
)

BASE_SYNC_CONSTRAINT_STATEMENTS = (
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'proyecto_producto_proyecto_id_fkey'
      ) THEN
        ALTER TABLE notion_sync.proyecto_producto
        ADD CONSTRAINT proyecto_producto_proyecto_id_fkey
        FOREIGN KEY (proyecto_id)
        REFERENCES notion_sync.proyectos(notion_page_id)
        ON DELETE CASCADE;
      END IF;
    END
    $$;
    """,
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'proyecto_producto_producto_id_fkey'
      ) THEN
        ALTER TABLE notion_sync.proyecto_producto
        ADD CONSTRAINT proyecto_producto_producto_id_fkey
        FOREIGN KEY (producto_id)
        REFERENCES notion_sync.productos(notion_page_id)
        ON DELETE CASCADE;
      END IF;
    END
    $$;
    """,
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'producto_tarea_producto_id_fkey'
      ) THEN
        ALTER TABLE notion_sync.producto_tarea
        ADD CONSTRAINT producto_tarea_producto_id_fkey
        FOREIGN KEY (producto_id)
        REFERENCES notion_sync.productos(notion_page_id)
        ON DELETE CASCADE;
      END IF;
    END
    $$;
    """,
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'producto_tarea_tarea_id_fkey'
      ) THEN
        ALTER TABLE notion_sync.producto_tarea
        ADD CONSTRAINT producto_tarea_tarea_id_fkey
        FOREIGN KEY (tarea_id)
        REFERENCES notion_sync.tareas(notion_page_id)
        ON DELETE CASCADE;
      END IF;
    END
    $$;
    """,
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'producto_bloqueo_producto_id_fkey'
      ) THEN
        ALTER TABLE notion_sync.producto_bloqueo
        ADD CONSTRAINT producto_bloqueo_producto_id_fkey
        FOREIGN KEY (producto_id)
        REFERENCES notion_sync.productos(notion_page_id)
        ON DELETE CASCADE;
      END IF;
    END
    $$;
    """,
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'producto_bloqueo_bloquea_a_producto_id_fkey'
      ) THEN
        ALTER TABLE notion_sync.producto_bloqueo
        ADD CONSTRAINT producto_bloqueo_bloquea_a_producto_id_fkey
        FOREIGN KEY (bloquea_a_producto_id)
        REFERENCES notion_sync.productos(notion_page_id)
        ON DELETE CASCADE;
      END IF;
    END
    $$;
    """,
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'tarea_bloqueo_tarea_id_fkey'
      ) THEN
        ALTER TABLE notion_sync.tarea_bloqueo
        ADD CONSTRAINT tarea_bloqueo_tarea_id_fkey
        FOREIGN KEY (tarea_id)
        REFERENCES notion_sync.tareas(notion_page_id)
        ON DELETE CASCADE;
      END IF;
    END
    $$;
    """,
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'tarea_bloqueo_bloquea_a_tarea_id_fkey'
      ) THEN
        ALTER TABLE notion_sync.tarea_bloqueo
        ADD CONSTRAINT tarea_bloqueo_bloquea_a_tarea_id_fkey
        FOREIGN KEY (bloquea_a_tarea_id)
        REFERENCES notion_sync.tareas(notion_page_id)
        ON DELETE CASCADE;
      END IF;
    END
    $$;
    """,
)

BASE_SYNC_INDEX_STATEMENTS = (
    """
    CREATE INDEX IF NOT EXISTS idx_productos_estado
    ON notion_sync.productos (estado);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_productos_last_edited
    ON notion_sync.productos (last_edited_time);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_proyectos_last_edited
    ON notion_sync.proyectos (last_edited_time);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_tareas_estado
    ON notion_sync.tareas (estado);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_tareas_last_edited
    ON notion_sync.tareas (last_edited_time);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_proyecto_producto_producto
    ON notion_sync.proyecto_producto (producto_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_producto_tarea_tarea
    ON notion_sync.producto_tarea (tarea_id);
    """,
)


async def init_pool(dsn: str, min_size: int = 1, max_size: int = 8) -> AsyncConnectionPool:
    """Initialize a global async connection pool."""
    global pool
    if pool is None:
        pool = AsyncConnectionPool(conninfo=dsn, min_size=min_size, max_size=max_size, open=False)
        await pool.open()
        await pool.wait()
        await ensure_base_sync_schema(pool)
        await ensure_sync_metadata_tables(pool)
        await ensure_performance_indexes(pool)
    return pool


def get_pool() -> AsyncConnectionPool:
    if pool is None:
        raise RuntimeError("Database pool not initialized")
    return pool


async def get_connection() -> AsyncGenerator[psycopg.AsyncConnection, None]:
    """FastAPI dependency to get an async connection with dict row factory."""
    active_pool = get_pool()
    async with active_pool.connection() as conn:
        conn.row_factory = dict_row
        yield conn


async def ensure_base_sync_schema(active_pool: AsyncConnectionPool) -> None:
    async with active_pool.connection() as conn:
        async with conn.cursor() as cur:
            for statement in BASE_SYNC_SCHEMA_STATEMENTS:
                await cur.execute(statement)
            for statement in BASE_SYNC_CONSTRAINT_STATEMENTS:
                await cur.execute(statement)
            for statement in BASE_SYNC_INDEX_STATEMENTS:
                await cur.execute(statement)
        await conn.commit()


async def ensure_performance_indexes(active_pool: AsyncConnectionPool) -> None:
    async with active_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tarea_bloqueo_blocked
                ON notion_sync.tarea_bloqueo (bloquea_a_tarea_id);
                """
            )
        await conn.commit()


async def ensure_sync_metadata_tables(active_pool: AsyncConnectionPool) -> None:
    async with active_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("CREATE SCHEMA IF NOT EXISTS notion_sync;")
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS notion_sync.dynamic_property_map (
                  table_name text NOT NULL,
                  data_source_id text NOT NULL,
                  property_id text NOT NULL,
                  property_name text NOT NULL,
                  property_type text NOT NULL,
                  column_name text NOT NULL,
                  pg_type text NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT NOW(),
                  updated_at timestamptz NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (table_name, property_id),
                  UNIQUE (table_name, column_name)
                );
                """
            )
            await cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_dynamic_property_map_data_source
                ON notion_sync.dynamic_property_map (data_source_id);
                """
            )
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS notion_sync.fixed_property_map (
                  data_source_id text NOT NULL,
                  canonical_name text NOT NULL,
                  property_id text NOT NULL,
                  property_name text NOT NULL,
                  updated_at timestamptz NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (data_source_id, canonical_name),
                  UNIQUE (data_source_id, property_id)
                );
                """
            )
            await cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_fixed_property_map_property
                ON notion_sync.fixed_property_map (data_source_id, property_id);
                """
            )
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS notion_sync.app_user (
                  user_key text PRIMARY KEY,
                  display_name text NOT NULL,
                  password_hash text NOT NULL,
                  password_salt text,
                  generated_password text,
                  is_active boolean NOT NULL DEFAULT true,
                  can_login boolean NOT NULL DEFAULT true,
                  can_view_workload boolean NOT NULL DEFAULT false,
                  last_login_at timestamptz,
                  created_at timestamptz NOT NULL DEFAULT NOW(),
                  updated_at timestamptz NOT NULL DEFAULT NOW()
                );
                """
            )
            await cur.execute(
                """
                ALTER TABLE notion_sync.app_user
                ADD COLUMN IF NOT EXISTS can_login boolean NOT NULL DEFAULT true;
                """
            )
            await cur.execute(
                """
                ALTER TABLE notion_sync.app_user
                ADD COLUMN IF NOT EXISTS can_view_workload boolean NOT NULL DEFAULT false;
                """
            )
            await cur.execute(
                """
                ALTER TABLE notion_sync.app_user
                ADD COLUMN IF NOT EXISTS can_view_all boolean NOT NULL DEFAULT false;
                """
            )
            await cur.execute(
                """
                ALTER TABLE notion_sync.app_user
                ALTER COLUMN generated_password DROP NOT NULL;
                """
            )
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS notion_sync.user_entity_access (
                  user_key text NOT NULL REFERENCES notion_sync.app_user(user_key) ON DELETE CASCADE,
                  entity_type text NOT NULL,
                  entity_id uuid NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (user_key, entity_type, entity_id),
                  CHECK (entity_type IN ('project', 'product', 'task'))
                );
                """
            )
            await cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_user_entity_access_lookup
                ON notion_sync.user_entity_access (user_key, entity_type, entity_id);
                """
            )
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS notion_sync.user_entity_assignment (
                  user_key text NOT NULL REFERENCES notion_sync.app_user(user_key) ON DELETE CASCADE,
                  entity_type text NOT NULL,
                  entity_id uuid NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (user_key, entity_type, entity_id),
                  CHECK (entity_type IN ('project', 'product', 'task'))
                );
                """
            )
            await cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_user_entity_assignment_lookup
                ON notion_sync.user_entity_assignment (user_key, entity_type, entity_id);
                """
            )
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS notion_sync.app_session (
                  token_hash text PRIMARY KEY,
                  user_key text NOT NULL REFERENCES notion_sync.app_user(user_key) ON DELETE CASCADE,
                  expires_at timestamptz NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT NOW()
                );
                """
            )
            await cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_app_session_user
                ON notion_sync.app_session (user_key, expires_at);
                """
            )
        await conn.commit()


async def close_pool() -> None:
    if pool is not None:
        await pool.close()
