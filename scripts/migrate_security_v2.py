"""One-shot migration:

1. Deactivates 4 known duplicate users (consolidation).
2. Re-issues per-user random salt + new temporary password for every login-enabled user
   (forces password reset; the previous global APP_PASSWORD_SALT-derived hashes become invalid).

Run once, hand out the printed credentials, and never store them.

Usage:
  python -m scripts.migrate_security_v2          # dry run, prints plan only
  python -m scripts.migrate_security_v2 --apply  # writes to the configured PG_DSN/DATABASE_URL
"""

from __future__ import annotations

import argparse
import base64
import os
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.auth import _hash_password, _new_random_salt, generate_temporary_password


DUPLICATE_USER_KEYS = (
    "matiasm",
    "ricardo soria galvarro guerra",
    "alvaro miradna",
    "javier g",
)


def load_local_env(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip()


def resolve_dsn() -> str:
    return (os.getenv("DATABASE_URL") or os.getenv("PG_DSN") or "").strip()


def print_table(title: str, headers: list[str], rows: list[list[str]]) -> None:
    if not rows:
        print(f"{title}: (vacio)")
        return
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    divider = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    print(f"\n{title}")
    print(divider)
    print("| " + " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers)) + " |")
    print(divider)
    for row in rows:
        print("| " + " | ".join(row[i].ljust(widths[i]) for i in range(len(headers))) + " |")
    print(divider)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes. Without this flag the script only prints what it would do.",
    )
    args = parser.parse_args()

    load_local_env(PROJECT_ROOT / ".env")
    dsn = resolve_dsn()
    if not dsn:
        parser.error("DATABASE_URL/PG_DSN no esta definido")

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_key, display_name, is_active, can_login,
                       can_view_workload, can_view_all
                FROM notion_sync.app_user
                ORDER BY can_view_all DESC, can_view_workload DESC, user_key ASC;
                """
            )
            existing = cur.fetchall()

        duplicates = [r for r in existing if r["user_key"] in DUPLICATE_USER_KEYS]
        login_users = [r for r in existing if r["can_login"] and r["user_key"] not in DUPLICATE_USER_KEYS]

        print_table(
            "Duplicados a desactivar",
            ["user_key", "display_name", "is_active", "can_login"],
            [
                [r["user_key"], r["display_name"], str(r["is_active"]), str(r["can_login"])]
                for r in duplicates
            ],
        )
        print_table(
            "Usuarios cuyo password se rotara",
            ["user_key", "display_name", "admin"],
            [
                [
                    r["user_key"],
                    r["display_name"],
                    "SI" if r["can_view_all"] or r["can_view_workload"] else "",
                ]
                for r in login_users
            ],
        )

        if not args.apply:
            print("\n[dry-run] No se modifico nada. Re-corre con --apply para confirmar.")
            return 0

        new_credentials: list[list[str]] = []

        with conn.cursor() as cur:
            if duplicates:
                cur.execute(
                    """
                    UPDATE notion_sync.app_user
                    SET is_active = false,
                        can_login = false,
                        can_view_workload = false,
                        can_view_all = false,
                        generated_password = NULL,
                        updated_at = NOW()
                    WHERE user_key = ANY(%s);
                    """,
                    ([r["user_key"] for r in duplicates],),
                )

            for row in login_users:
                user_key = row["user_key"]
                display_name = row["display_name"]
                temp_password = generate_temporary_password(display_name)
                user_salt = _new_random_salt()
                cur.execute(
                    """
                    UPDATE notion_sync.app_user
                    SET password_hash = %s,
                        password_salt = %s,
                        generated_password = NULL,
                        updated_at = NOW()
                    WHERE user_key = %s;
                    """,
                    (
                        _hash_password(temp_password, user_salt),
                        base64.b64encode(user_salt).decode("ascii"),
                        user_key,
                    ),
                )
                new_credentials.append([user_key, display_name, temp_password])

        conn.commit()

    print_table(
        "Credenciales temporales (entregar AHORA — no se almacenan en cleartext)",
        ["user_key", "display_name", "temporary_password"],
        new_credentials,
    )
    print(
        "\nMigracion completa. "
        f"{len(duplicates)} duplicados desactivados, "
        f"{len(new_credentials)} contrasenas rotadas."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
