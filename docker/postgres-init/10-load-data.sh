#!/usr/bin/env bash
# Runs once, on an empty data volume, after 01-schema.sql: loads the generated CSVs in foreign-key order.
set -euo pipefail

for table in teams competencies employees projects performance_reviews competency_assessments talent_review; do
  echo "loading ${table}.csv"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c "\copy ${table} FROM '/data/generated/${table}.csv' WITH (FORMAT csv, HEADER true)"
done
