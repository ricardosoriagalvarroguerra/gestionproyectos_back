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

from app.auth import _hash_password, _password_salt_bytes


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reset local temporary passwords for login-enabled users."
    )
    parser.add_argument(
        "--password",
        required=True,
        help="New temporary password to apply to every selected login-enabled user.",
    )
    parser.add_argument(
        "--user-key",
        action="append",
        dest="user_keys",
        default=[],
        help="Optional user_key filter. Repeat the flag to target multiple users.",
    )
    return parser


def print_table(rows: list[dict[str, str]]) -> None:
    if not rows:
        print("No se encontraron usuarios habilitados para login.")
        return

    key_width = max(len("user_key"), *(len(row["user_key"]) for row in rows))
    name_width = max(len("display_name"), *(len(row["display_name"]) for row in rows))
    divider = f"+-{'-' * key_width}-+-{'-' * name_width}-+"

    print(divider)
    print(
        f"| {'user_key'.ljust(key_width)} | {'display_name'.ljust(name_width)} |"
    )
    print(divider)
    for row in rows:
        print(
            f"| {row['user_key'].ljust(key_width)} | {row['display_name'].ljust(name_width)} |"
        )
    print(divider)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    load_local_env(PROJECT_ROOT / ".env")

    dsn = os.getenv("PG_DSN", "").strip()
    if not dsn:
        parser.error("PG_DSN no esta definido en el entorno ni en backend/.env")

    salt = _password_salt_bytes()
    password_hash = _hash_password(args.password, salt)
    encoded_salt = base64.b64encode(salt).decode("ascii")

    where_filters = ["is_active = true", "can_login = true"]
    params: list[object] = []
    if args.user_keys:
        where_filters.append("user_key = ANY(%s)")
        params.append(args.user_keys)
    where_sql = " AND ".join(where_filters)

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT user_key, display_name
                FROM notion_sync.app_user
                WHERE {where_sql}
                ORDER BY display_name ASC;
                """,
                params,
            )
            rows = cur.fetchall()

            if not rows:
                print("No se encontraron usuarios habilitados para login.")
                return 0

            cur.execute(
                f"""
                UPDATE notion_sync.app_user
                SET password_hash = %s,
                    password_salt = %s,
                    generated_password = NULL,
                    updated_at = NOW()
                WHERE {where_sql};
                """,
                [password_hash, encoded_salt, *params],
            )
        conn.commit()

    print("Contrasena temporal reasignada para los siguientes usuarios:")
    print_table(rows)
    print(f"Contrasena temporal aplicada: {args.password}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
