# shellcheck shell=bash
# Shared helpers for the shell scripts. Source this file; don't run it.

# Export KEY=VALUE lines from a dotenv file. Variables already set in the shell win.
load_dotenv() {
  local file="${1:-.env}" line key
  [ -f "$file" ] || return 0
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"
    case "$line" in '' | \#*) continue ;; esac
    key="${line%%=*}"
    if [ -z "${!key+x}" ]; then export "$key=${line#*=}"; fi
  done <"$file"
}

# Connection settings for tests running on the host, against the compose services.
# Anything already set keeps its value.
export_test_connection() {
  export INTEGRATION=1
  export POSTGRES_HOST="${POSTGRES_HOST:-127.0.0.1}"
  export POSTGRES_PORT="${POSTGRES_PORT:-${PG_HOST_PORT:-5544}}"
  export S3_ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:${MINIO_HOST_PORT:-9000}}"
  export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-${MINIO_ROOT_USER:-}}"
  export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-${MINIO_ROOT_PASSWORD:-}}"
  export AWS_REGION="${AWS_REGION:-us-east-1}"
}

rand_hex() {
  if command -v openssl >/dev/null 2>&1; then openssl rand -hex 16
  else python3 -c 'import secrets; print(secrets.token_hex(16))'
  fi
}

# Write .env from .env.example with random passwords, readable by the owner only.
# The umask is set in a subshell: a script-wide `umask 077` would also make every directory
# created later (e.g. data/generated) owner-only, and the Postgres container could not read it.
create_dotenv() {
  local example="${1:-.env.example}" out="${2:-.env}" pg minio
  pg="$(rand_hex)" minio="$(rand_hex)"
  (
    umask 077
    sed -e "s/^POSTGRES_PASS=.*/POSTGRES_PASS=${pg}/" \
        -e "s/^MINIO_ROOT_PASSWORD=.*/MINIO_ROOT_PASSWORD=${minio}/" "$example" >"$out"
  )
}

# Containers that write to bind mounts run as the host user, so it can read and delete the files.
export_host_ids() {
  export ETL_UID="${ETL_UID:-$(id -u)}" ETL_GID="${ETL_GID:-$(id -g)}"
  export SITE_UID="${SITE_UID:-$(id -u)}" SITE_GID="${SITE_GID:-$(id -g)}"
}
