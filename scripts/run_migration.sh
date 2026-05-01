#!/usr/bin/env bash
# Wrapper to run the security migration against the prod Railway DB.
# Usage:
#   bash scripts/run_migration.sh           # dry-run
#   bash scripts/run_migration.sh --apply   # apply changes
#
# Reads DATABASE_URL from .env.local (gitignored). Delete .env.local after use.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f .env.local ]; then
  echo "ERROR: .env.local not found. Create it with: DATABASE_URL=postgresql://..." >&2
  exit 1
fi

# shellcheck disable=SC1091
set -a
. ./.env.local
set +a

if [ -z "${DATABASE_URL:-}" ]; then
  echo "ERROR: DATABASE_URL not set in .env.local" >&2
  exit 1
fi

if [ ! -x .venv/bin/python ]; then
  echo "ERROR: .venv missing. Run: python3 -m venv .venv && .venv/bin/pip install 'psycopg[binary]'" >&2
  exit 1
fi

SKIP_DOTENV=1 .venv/bin/python -m scripts.migrate_security_v2 "$@"
