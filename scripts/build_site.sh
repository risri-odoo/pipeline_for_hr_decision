#!/usr/bin/env bash
# Build site/ from the lake: execute the notebook -> insights.html, then the dashboard -> index.html + lineage.json.
# Lake settings come from the environment (LAKE_BUCKET, S3_ENDPOINT, AWS_*), as for job.py.
#   SITE_DIR=site      output directory
#   SKIP_NOTEBOOK=1    dashboard only
set -euo pipefail

cd "$(dirname "$0")/.."
out="${SITE_DIR:-site}"
py="${PYTHON:-python}"
mkdir -p "$out"

if [ "${SKIP_NOTEBOOK:-0}" != "1" ]; then
  echo "[build_site] executing notebooks/insights.ipynb"
  "$py" -m jupyter nbconvert --to html --execute notebooks/insights.ipynb \
    --ExecutePreprocessor.timeout=600 --output insights --output-dir "$out"
fi

echo "[build_site] building dashboard"
"$py" scripts/build_dashboard.py --out "$out"
