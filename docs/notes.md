# Project notes

Implementation notes, and where the code differs from `docs/design-spec.md`.

## Decisions and deviations from the spec

### MinIO image: Chainguard build (`cgr.dev/chainguard/minio`)
- The official `minio/minio` image can no longer be pulled anonymously: Docker Hub returns "pull access denied" and `quay.io/minio/*` returns 401 (checked 2026-10-18).
- We use Chainguard's free MinIO build instead. It is the same MinIO server (`minio version RELEASE.2026-09-22...`), rebuilt by Chainguard with a minimal base and a non-root user.
- The image is pinned by digest in `docker-compose.yml`. Chainguard's free tier publishes only `:latest`, so a digest pin is the only reproducible reference. To update: `docker pull cgr.dev/chainguard/minio:latest`, then copy the new digest.
- The image includes `mc`, so it also covers the healthcheck (`mc ready local`) and bucket creation (`createbucket` service); no second image is needed.
- Fallbacks if Chainguard access changes: `rustfs/rustfs` or `chrislusf/seaweedfs` (both S3-compatible and both pull anonymously). `job.py` only needs an S3 endpoint with path-style addressing.

### Other deviations
- **JSONL upload:** the spec says the JSONL is "uploaded once" to landing but doesn't say by which step. `extract` does it when `EVENTS_SOURCE_DIR` is set (files already present with the same size are skipped).
- **Spark:** `ENGINE=spark` fails fast with a config error. Spark is optional in the spec and not implemented.
- **Local secrets:** `scripts/run_compose.sh` creates a git-ignored `.env` with random passwords on first run; `.env.example` holds placeholders only. The etl service uses the MinIO root user locally. That is fine for a throwaway local or CI stack, but a real deployment should use a scoped access key.
- **Reject categories:** the quarantine log splits event_date failures into "unparseable date" and "impossible date", matching the spec's DQ table.
- **Performance:** a full Docker run takes about 24-31 s, against about 1.3 s with an in-memory lake. The gap is the many small JSONL objects read one at a time (the bottleneck the spec already notes). It is inside the SLA; parallel reads would be the fix.

## Layer pipelines

`job.py` runs as three independent layers. `--layer` (or `LAYER=`) picks one; `STEPS` still works and is grouped into the same layers.

| Layer | Steps | Needs | Writes | Checks |
| --- | --- | --- | --- | --- |
| bronze | extract + bronze | Postgres, MinIO | `landing/`, `bronze/`, `bronze/_SUCCESS` | 1 (bronze rows == landing rows) |
| silver | silver | MinIO, `bronze/_SUCCESS` | `silver/` (+ `_quarantine/`), `silver/_SUCCESS` | none (rejects are quarantined, not failed) |
| gold | gold | MinIO, `silver/_SUCCESS` | `gold/` (7 tables), `gold/_site/findings.json`, `gold/_SUCCESS` | 8 |

- **Run ids:** each layer run gets `<run id>-<layer>` (e.g. `20261019T095910Z-46471f-gold`), its own lineage (`lineage/<layer>/runs/<id>.json`, `lineage/<layer>/latest.json`) and, when it completes, a `_SUCCESS` marker: `{layer, run_id, run_date, completed_at, row_counts, upstream}`.
- **Dependency checks:** silver fails fast (before writing anything) when `bronze/_SUCCESS` is missing, and gold when `silver/_SUCCESS` is missing. The error says which layer to run. A layer deletes its own marker when it starts, so a failed rerun can't leave a stale "success" for the next layer to consume. `STEPS=extract` alone doesn't complete bronze and writes no marker.
- **Lineage chain:** silver records the bronze run_id it read; gold records the silver run_id and, through it, the bronze run_id.
- **Staleness is about data, not run ids:** bronze writes a `data_checksum` (SHA-256 over the bronze objects' ETags, ignoring dates and run ids) into its marker, and silver and gold inherit it. A layer is stale only when its input checksum differs from the upstream marker's, so re-running a layer on unchanged input changes nothing. If bronze has changed since silver was built, gold logs a warning (also kept in its lineage `warnings`). A code change doesn't change the checksum.
- **Pipeline view:** `lineage/latest.json` is rebuilt after every invocation from the latest run of each layer, plus the markers: steps and checks are concatenated (1 bronze + 8 gold = 9), and `layers` lists each layer's run, status and upstream. Status: `FAILED` (a layer's latest run failed), `INCOMPLETE` (a layer has no completed output), `STALE` (a layer was built from different upstream data), else `SUCCEEDED`. A run stopped by its dependency check is `REFUSED`: it changed nothing, so it stays in `lineage/<layer>/runs/` but doesn't become the layer's latest run. The dashboard header shows this status, each layer's run and any warnings.
- **Site data:** gold writes the section 7 aggregates to `gold/_site/findings.json`; the site build copies it to `site/findings.json`. The findings logic moved from `scripts/findings.py` into `job.py` for this (the script re-exports it).

### CI: how the lake moves between jobs
GitHub Actions jobs run on separate, fresh runners, so the MinIO started in one job is gone in the next. Options considered:

| Option | Why not / why |
| --- | --- |
| One runner, three steps | simplest, but not three jobs, and a layer can't be rerun alone |
| Tar MinIO's data directory | couples the artifact to MinIO's on-disk format and version |
| External persistent S3 (R2, S3) | real persistence, but needs an account, a card and secrets |
| **Object-level export/import (chosen)** | `python job.py --export-lake DIR` / `--import-lake DIR` copy every object 1:1 (16 threads). Format-independent, works with any S3 endpoint, and is just files |

Each job imports the previous job's export, runs its layer and exports the whole lake as the artifact `lake-after-<layer>` (7-day retention, about 22 MB at the default size). Running one layer alone (`workflow_dispatch`, `layer=silver`) restores the newest unexpired `lake-after-*` artifact from an earlier run (`scripts/ci_find_state.sh`, via the GitHub API); the layer's dependency check then says whether that state is good enough. If the saved state has expired, run `layer=all` (or the upstream layer) first. `concurrency` keeps runs sequential so they don't race on "newest state".

Measured locally (Docker Desktop on Windows): export about 20 s, import about 60 s for 1,640 objects. Windows file sharing is the slow part; Linux runners should be faster.

### Hosting: GitHub Pages
- **Why GitHub Pages:** the repo is public, so Pages is free, and the official actions need no secrets or third-party account (the workflow's `GITHUB_TOKEN` deploys with `pages: write` + `id-token: write`).
- **One-time setup:** Settings > Pages > Build and deployment > Source = **GitHub Actions**. Until then the `deploy` job fails at its first step.
- **Gate:** `scripts/ci_gate.py` reads `site/lineage.json` in the gold job and sets `deployable=true` only for status `SUCCEEDED` with exactly 9/9 checks (so a STALE or INCOMPLETE pipeline never deploys). The gold job packages `site/` with `actions/upload-pages-artifact` only then, and only on `main`.
- **Deploy:** the `deploy` job runs `actions/deploy-pages` only when bronze, silver and gold all succeeded in the same run on `main`. A single-layer run (`workflow_dispatch`, `layer=gold`) never deploys, even if it ends SUCCEEDED.
- **The published site is public.** It holds aggregates only, plus `insights.html`, whose lookup example shows one synthetic employee's id, department and ratings (no names or emails).

### Gotchas met while building this
- In Git Bash, `/lake-state` passed to `docker compose run` gets rewritten to `C:/Program Files/Git/lake-state`. `run_layer.sh` sets `MSYS_NO_PATHCONV=1`.
- `actions/upload-artifact@v4` skips hidden files and folders, so CI uses `./lake-state` (`LAKE_STATE_DIR`), not the local default `./.lake-state`.
- Containers write to bind mounts as `ETL_UID`/`SITE_UID` (default 10001). The shell scripts and CI set them to the host user so it can read and delete the files.

## Generated data
- `scripts/generate_data.py` writes `data/generated/` (git-ignored). The run scripts generate it through the `datagen` compose service if it's missing, so Docker is enough; `run_tests.sh` uses local Python.
- It uses only the standard library and a seeded `random.Random`, with fixed dates and no clock, so the same seed and size give byte-identical files.
- Planted issues are chosen so each bad line fails exactly one rule first. Duplicates go after the original in the same day's file, so the first copy is kept.
- Postgres loads the CSVs once, on an empty volume. After regenerating with another seed or size, reset the volumes (`-Reset` / `RESET=1`), or Postgres keeps the old rows.

## Running it on Windows (PowerShell)

Use the PowerShell script. In PowerShell, plain `bash` resolves to WSL's bash (`C:\Windows\System32\bash.exe`), not Git Bash, so `bash scripts/run_compose.sh` runs in the wrong environment.

```powershell
cd C:\dvt\Marksheets\data_pipleline_project
.\scripts\run_compose.ps1                # pipeline + site\ (first run creates .env with random secrets)
.\scripts\run_compose.ps1 -Runs 2        # idempotency check
.\scripts\run_compose.ps1 -Reset         # start from empty volumes
start site\index.html                    # open the dashboard
```

If PowerShell blocks the script ("running scripts is disabled"), allow it for this window only:
`Set-ExecutionPolicy -Scope Process Bypass`.

Tests (need the venv; Git Bash runs the shell script):

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements-dev.txt
& "C:\Program Files\Git\bin\bash.exe" scripts/run_tests.sh
```

## Site
- `site/index.html`: the dashboard. Self-contained (inline CSS + inline SVG from matplotlib, no JavaScript, no external URLs), aggregate-only (enforced by tests), follows the viewer's light/dark setting. Chart colours come from a colour-blind-checked categorical palette; matplotlib draws with the light values and the SVG is rewritten to CSS variables.
- `site/insights.html`: `notebooks/insights.ipynb` executed against the lake and exported by nbconvert. nbconvert's template loads require.js, MathJax and mermaid from CDNs; `build_dashboard.sanitize_html` strips scripts, external links and URL comments so this page is self-contained too (nothing in it needs JS).
- `site/lineage.json`: the latest run's lineage report.
- **Not published:** the spec lists optional `gold/*.csv`. They are skipped because the Gold tables are per-employee (names, team, ratings), which contradicts aggregate-only.
- **Notebook lookups** (`review_history`, `training_history`) return ids and HR fields only (no names or emails), because the executed notebook is published.
- The committed notebook has no outputs; `scripts/build_site.sh` executes it. The `reports` image stage (`Dockerfile` target `reports`) holds matplotlib + nbconvert, so the `etl` image stays small. `SITE_UID`/`SITE_GID` let Linux CI own the files written to `./site`.

## Testing
- `scripts/run_tests.sh`: starts Postgres + MinIO, runs unit and integration tests with coverage.
- `UNIT_ONLY=1 scripts/run_tests.sh`: no Docker; integration tests skip.
- The acceptance gate (spec section 7) is itself a test (`tests/test_gold.py::test_sample_findings_match_manifest`). Expected values come from `data/generated/manifest.json`, and the same check runs on other seeds and sizes (`tests/test_generate_data.py`).
- `tests/test_regressions.py` holds one test per fixed bug.
- `tests/conftest.py` generates the data if it's missing.
- Integration tests use a throwaway bucket per test and remove it afterwards.
