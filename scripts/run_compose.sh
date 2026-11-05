#!/usr/bin/env bash
# Run the full pipeline locally: Postgres (generated data) + MinIO + etl job.
#
# Options (env vars):
#   STEPS=extract,bronze,silver,gold   stages to run (default: all)
#   RUNS=2                             run the job N times (idempotency check)
#   FINDINGS=1                         print run summary + headline findings after the run
#   SKIP_BUILD=1                       reuse the existing images
#   SKIP_SITE=1                        don't build site/ after a successful run
#   SKIP_NOTEBOOK=1                    build site/ without executing the notebook
#   RESET=1                            drop volumes first (fresh Postgres + empty lake); needed after regenerating data
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=scripts/_env.sh
. scripts/_env.sh
export_host_ids

log() { printf '[run_compose] %s\n' "$*"; }

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

if [ "${RESET:-0}" = "1" ]; then
  log "RESET=1: removing containers and volumes"
  docker compose --profile etl down -v --remove-orphans
fi

if [ "${SKIP_BUILD:-0}" != "1" ]; then
  log "building etl + reports images"
  docker compose --profile etl build etl reports
fi

mkdir -p data/generated/learning_activity
if [ ! -f data/generated/manifest.json ]; then
  log "generating synthetic data"
  docker compose run --rm datagen
fi

log "starting postgres + minio"
docker compose up -d --wait postgres minio

status=0
for i in $(seq 1 "${RUNS:-1}"); do
  log "etl run $i/${RUNS:-1} (STEPS=${STEPS:-extract,bronze,silver,gold})"
  if ! docker compose run --rm etl; then status=1; break; fi
done

if [ "${FINDINGS:-0}" = "1" ] || [ "$status" != "0" ]; then
  docker compose run --rm --no-deps --entrypoint python etl scripts/findings.py || true
fi

if [ "$status" = "0" ] && [ "${SKIP_SITE:-0}" != "1" ]; then
  log "building site/ (dashboard + notebook)"
  mkdir -p site
  if docker compose run --rm reports; then
    log "site ready: site/index.html"
  else
    log "site build FAILED"; status=1
  fi
fi

if [ "$status" = "0" ]; then log "pipeline SUCCEEDED"; else log "pipeline FAILED"; fi
exit "$status"
