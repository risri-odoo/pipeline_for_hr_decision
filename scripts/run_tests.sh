#!/usr/bin/env bash
# Unit + integration tests with coverage.
#   UNIT_ONLY=1   skip Docker; integration tests are skipped
#   PYTHON=...    interpreter (default: .venv python if present, else python3 or python)
# Settings already in the shell win over .env, e.g. PG_HOST_PORT=6000 scripts/run_tests.sh
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=scripts/_env.sh
. scripts/_env.sh

if [ -z "${PYTHON:-}" ]; then
  if [ -x .venv/bin/python ]; then PYTHON=.venv/bin/python
  elif [ -x .venv/Scripts/python.exe ]; then PYTHON=.venv/Scripts/python.exe
  elif python3 -c '' >/dev/null 2>&1; then PYTHON=python3
  else PYTHON=python
  fi
fi

if [ ! -f data/generated/manifest.json ]; then "$PYTHON" scripts/generate_data.py; fi

if [ "${UNIT_ONLY:-0}" != "1" ]; then
  [ -f .env ] || { echo "no .env: run scripts/run_compose.sh once first"; exit 1; }
  load_dotenv .env
  docker compose up -d --wait postgres minio
  export_test_connection
fi

exec "$PYTHON" -m pytest --cov --cov-report=term-missing "$@"
