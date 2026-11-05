#!/usr/bin/env bash
# Run one pipeline layer (or all) in Docker.
#
#   scripts/run_layer.sh bronze|silver|gold|all
#
# bronze = extract (Postgres -> landing) + bronze; needs Postgres + MinIO.
# silver, gold = MinIO only; each fails fast if its upstream layer's _SUCCESS marker is missing.
# gold and all also build site/ (dashboard + notebook) unless SKIP_SITE=1.
#
# Options (env vars):
#   SKIP_BUILD=1      reuse the existing images
#   SKIP_SITE=1       don't build site/ after gold
#   SKIP_NOTEBOOK=1   build site/ without executing the notebook
#   IMPORT_STATE=1    load the lake from $LAKE_STATE_DIR before running (CI: previous job's artifact)
#   EXPORT_STATE=1    save the whole lake to $LAKE_STATE_DIR afterwards (CI: next job's artifact)
#   LAKE_STATE_DIR    default ./.lake-state
set -euo pipefail

cd "$(dirname "$0")/.."
# Git Bash on Windows rewrites /lake-state into a Windows path; keep container paths as written
export MSYS_NO_PATHCONV=1
# shellcheck source=scripts/_env.sh
. scripts/_env.sh
export_host_ids

layer="${1:-}"
case "$layer" in
  bronze|silver|gold|all) ;;
  *) echo "usage: $0 bronze|silver|gold|all" >&2; exit 2 ;;
esac

log() { printf '[run_layer %s] %s\n' "$layer" "$*"; }

rand() {
  if command -v openssl >/dev/null 2>&1; then openssl rand -hex 16
  else python3 -c 'import secrets; print(secrets.token_hex(16))'
  fi
}

if [ ! -f .env ]; then
  log "creating .env with random local secrets (git-ignored)"
  umask 077
  sed -e "s/^POSTGRES_PASS=.*/POSTGRES_PASS=$(rand)/" \
      -e "s/^MINIO_ROOT_PASSWORD=.*/MINIO_ROOT_PASSWORD=$(rand)/" .env.example > .env
fi

export LAKE_STATE_DIR="${LAKE_STATE_DIR:-./.lake-state}"
mkdir -p "$LAKE_STATE_DIR" site data/generated/learning_activity

if [ "${SKIP_BUILD:-0}" != "1" ]; then
  log "building images"
  docker compose --profile etl build etl reports
fi

if [ "$layer" = "bronze" ] || [ "$layer" = "all" ]; then
  if [ ! -f data/generated/manifest.json ]; then
    log "generating synthetic data"
    docker compose run --rm datagen
  fi
  log "starting postgres + minio"
  docker compose up -d --wait postgres minio
  run_deps=()
else
  log "starting minio"
  docker compose up -d --wait minio
  run_deps=(--no-deps)
fi

if [ "${IMPORT_STATE:-0}" = "1" ]; then
  log "importing lake state from $LAKE_STATE_DIR"
  docker compose run --rm --no-deps etl --import-lake /lake-state
fi

log "running layer"
docker compose run --rm "${run_deps[@]}" -e LAYER="$layer" etl

if [ "${EXPORT_STATE:-0}" = "1" ]; then
  log "exporting lake state to $LAKE_STATE_DIR"
  find "$LAKE_STATE_DIR" -mindepth 1 -delete
  docker compose run --rm --no-deps etl --export-lake /lake-state
fi

if { [ "$layer" = "gold" ] || [ "$layer" = "all" ]; } && [ "${SKIP_SITE:-0}" != "1" ]; then
  log "building site/"
  docker compose run --rm reports
fi

log "done"
