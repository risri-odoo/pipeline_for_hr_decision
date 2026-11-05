#!/usr/bin/env bash
# CI helper: decide where a layer job gets its input lake state from.
#   scripts/ci_find_state.sh <upstream job result> <artifact name from this run>
# - upstream job succeeded in this run  -> that job's artifact (this run)
# - upstream job skipped (layer run alone) -> newest unexpired "lake-after-*" artifact in the repo,
#   i.e. the lake as the last successful layer job left it
# Writes run_id=... and name=... to $GITHUB_OUTPUT. Needs GH_TOKEN and GITHUB_REPOSITORY.
set -euo pipefail

upstream_result="$1"
this_run_name="$2"

if [ "$upstream_result" = "success" ]; then
  run_id="$GITHUB_RUN_ID"
  name="$this_run_name"
else
  read -r run_id name < <(
    gh api "repos/$GITHUB_REPOSITORY/actions/artifacts?per_page=100" --jq \
      '[.artifacts[] | select((.name | startswith("lake-after-")) and (.expired | not))]
       | sort_by(.created_at) | last | "\(.workflow_run.id // "") \(.name // "")"'
  ) || true
  if [ -z "${run_id:-}" ] || [ -z "${name:-}" ]; then
    echo "::error::No saved lake state found from an earlier run. Run the workflow with layer=all (or the upstream layer) first."
    exit 1
  fi
  echo "::notice::Using lake state '$name' from run $run_id"
fi

echo "run_id=$run_id" >> "$GITHUB_OUTPUT"
echo "name=$name" >> "$GITHUB_OUTPUT"
