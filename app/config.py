import os
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BACKEND_DIR = Path(__file__).resolve().parents[1]
ENV_FILE = BACKEND_DIR / ".env"


class Settings(BaseSettings):
    notion_token: str = Field(..., alias="NOTION_TOKEN")
    # Mantener alineado con la versión documentada del repo y el uso de data_sources.
    notion_version: str = Field("2025-09-03", alias="NOTION_VERSION")
    db_proyectos_id: str = Field(..., alias="DB_PROYECTOS_ID")
    db_productos_id: str = Field(..., alias="DB_PRODUCTOS_ID")
    db_tareas_id: str = Field(..., alias="DB_TAREAS_ID")
    # Railway provee DATABASE_URL; para desarrollo local se puede usar PG_DSN.
    pg_dsn: str = Field(default="", alias="PG_DSN")
    database_url: str = Field(default="", alias="DATABASE_URL")
    done_statuses_raw: str = Field("Listo,Cerrado", alias="DONE_STATUSES")
    http_timeout: float = Field(30.0, alias="HTTP_TIMEOUT_SECONDS")
    cors_allowed_origins_raw: str = Field(
        "http://localhost:5173,http://127.0.0.1:5173,http://localhost:4173,http://127.0.0.1:4173",
        alias="CORS_ALLOWED_ORIGINS",
    )
    public_login_directory: bool = Field(False, alias="PUBLIC_LOGIN_DIRECTORY")
    workload_admin_user_keys_raw: str = Field(
        "matias mednik,ricardo soria galvarro",
        alias="WORKLOAD_ADMIN_USER_KEYS",
    )
    view_all_user_keys_raw: str = Field(
        "matias mednik,ricardo soria galvarro",
        alias="VIEW_ALL_USER_KEYS",
    )

    model_config = SettingsConfigDict(env_file=str(ENV_FILE), env_file_encoding="utf-8", extra="ignore")

    @classmethod
    def load(cls) -> "Settings":
        skip_dotenv = os.getenv("SKIP_DOTENV", "").lower() in {"1", "true", "yes"}
        env_file = None if skip_dotenv else str(ENV_FILE)
        return cls(_env_file=env_file)  # type: ignore[arg-type]

    @property
    def effective_pg_dsn(self) -> str:
        """Return DATABASE_URL (Railway) if set, otherwise fall back to PG_DSN."""
        dsn = self.database_url or self.pg_dsn
        if not dsn:
            raise RuntimeError(
                "No database connection configured. "
                "Set DATABASE_URL (Railway) or PG_DSN (local)."
            )
        return dsn

    @property
    def done_statuses(self) -> tuple[str, ...]:
        raw = self.done_statuses_raw
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        return tuple(parts) if parts else ("Listo", "Cerrado")

    @property
    def cors_allowed_origins(self) -> list[str]:
        raw = self.cors_allowed_origins_raw
        origins = [origin.strip() for origin in raw.split(",") if origin.strip()]
        return origins or ["http://localhost:5173", "http://127.0.0.1:5173"]

    @property
    def workload_admin_user_keys(self) -> tuple[str, ...]:
        raw = self.workload_admin_user_keys_raw
        return tuple(user.strip() for user in raw.split(",") if user.strip())

    @property
    def view_all_user_keys(self) -> tuple[str, ...]:
        raw = self.view_all_user_keys_raw
        return tuple(user.strip() for user in raw.split(",") if user.strip())
