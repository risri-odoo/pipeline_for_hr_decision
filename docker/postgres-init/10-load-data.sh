#!/usr/bin/env bash
# Runs once, on an empty data volume, after 01-schema.sql: loads the generated CSVs in foreign-key order.
set -euo pipefail

if [ ! -r /data/generated/teams.csv ]; then
  echo "ERROR: /data/generated/teams.csv is missing or unreadable for user $(id -un) (uid $(id -u))." >&2
  echo "Generate the data first, and make sure data/generated is readable by others (ls -ld data/generated)." >&2
  ls -ld /data/generated /data/generated/teams.csv >&2 || true
  exit 1
fi

for table in teams competencies employees projects performance_reviews competency_assessments talent_review; do
  echo "loading ${table}.csv"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c "\copy ${table} FROM '/data/generated/${table}.csv' WITH (FORMAT csv, HEADER true)"
done
