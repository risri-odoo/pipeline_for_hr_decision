# Design spec: ACME Talent ETL

A medallion pipeline running on Docker (Postgres + MinIO), GitHub Actions for compute, and a static host for the dashboard.
An earlier version ran on AWS (section 13). Implementation notes and deviations from this spec: `docs/notes.md`.
Never commit secrets.

---

## 1. Problem
ACME HR data is scattered: 7 PostgreSQL tables + a learning-platform event feed (JSONL, partitioned by day).
Build a medallion lake (landing -> bronze -> silver -> gold), validate the data, answer 7 questions
in Gold tables, a Jupyter notebook and a static dashboard.

### Source data (`data/generated/`, created by `scripts/generate_data.py`)
Fully synthetic, deterministic for a given `--seed` and `--employees`, and never committed.
- 7 CSVs: teams, competencies, employees, projects, performance_reviews, competency_assessments, talent_review
  (`docker/postgres-init/` creates the schema and loads them into Postgres in FK order).
- `learning_activity/year=YYYY/month=MM/day=DD/events-000.jsonl`: one file per weekday, 2023-09 .. 2026-09.
- `manifest.json`: row counts, planted data-quality counts and the expected results. All exact numbers live there, not in this document.

### The 7 questions -> Gold tables
| # | Question | Gold table (grain) |
|---|---|---|
| Q1 | Ratings / review history per employee | gold_employee_review_history (1 row per review) |
| Q2 | Critical skill gaps | gold_skill_gaps (1 row per assessment) |
| Q3 | High-potential, promotion-ready | gold_promotion_readiness (1 row per employee) |
| Q4 | Training completed | gold_training_summary (1 row per employee) |
| Q5 | Attrition risk from performance trends | gold_attrition_risk (1 row per employee) |
| Q6 | Performance vs team/project outcomes | gold_team_performance (1 row per team) |
| Q7 | Skill distribution | gold_skill_distribution (dept x category x competency) |

---

## 2. Target architecture (no AWS)
```
docker-compose: postgres:16 (schema + generated CSVs on init) + minio (+ create bucket) + etl (python:3.12-slim)
etl: job.py STEPS=extract,bronze,silver,gold
  Postgres --COPY--> s3://<bucket>/landing/postgres/...
  generated JSONL --upload once--> landing/learning_activity/...
  landing -> bronze -> silver (+ _quarantine) -> gold + lineage
scripts/build_dashboard.py: gold + lineage -> site/index.html (+ insights.html, lineage.json)
GitHub Actions: on push/manual/schedule -> compose up -> run -> publish site/ to Pages
```
Optional later: kind + Helm chart (Kubernetes Job), Spark engine, Cloudflare R2 for a persistent lake.

### Lake layout (bucket from env, e.g. `acme-datalake`)
| Prefix | Example | Mode |
|---|---|---|
| landing/postgres/ | landing/postgres/teams/ingest_date=2026-10-15/teams.csv | immutable, skip if exists |
| landing/learning_activity/ | landing/learning_activity/year=2023/month=10/day=02/events-000.jsonl | uploaded once |
| bronze/postgres/ | bronze/postgres/employees/ingest_date=2026-10-15/employees.csv | byte copy, never overwritten |
| bronze/s3/learning_activity/ | same partitions as landing | copy, skip if same size exists |
| silver/<table>/ | silver/performance_reviews/performance_reviews.parquet | full refresh |
| silver/learning_activity/ | silver/learning_activity/year=2024/month=03/part.parquet (37 monthly) | prefix deleted then rewritten |
| silver/_quarantine/ | silver/_quarantine/learning_activity/run_date=2026-10-15/rejects.parquet | replaced per run_date |
| gold/ | gold/gold_attrition_risk/gold_attrition_risk.parquet | overwrite |
| lineage/ | lineage/runs/<run_id>.json, lineage/latest.json | new file per run |

---

## 3. Tech stack
Python 3.12, pandas, pyarrow (Parquet), Pydantic v2, boto3 (S3 API to MinIO, path-style, retries 5 standard),
psycopg2 (COPY TO STDOUT), PostgreSQL 16, MinIO, Docker Compose, matplotlib (dashboard/notebook),
Jupyter + nbconvert, pytest + pytest-cov, bash (+ shellcheck), GitHub Actions, GitHub Pages.
Optional: PySpark (parity-tested alternative Gold engine), kind + Helm.

---

## 4. job.py design (single file, execution order)

### 4.1 Settings (all from env; fail fast; no hardcoded buckets/IDs)
| Var | Default | Purpose |
|---|---|---|
| POSTGRES_HOST/PORT/NAME/USER/PASS | localhost/5432/postgres/postgres/"" | source DB (required only if `extract` in STEPS) |
| S3_ENDPOINT | http://localhost:9000 | MinIO endpoint |
| AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY | from env | MinIO keys |
| AWS_REGION | us-east-1 | region |
| LAKE_BUCKET | required | lake bucket |
| LANDING_PREFIX | landing/learning_activity/ | JSONL landing |
| LANDING_PG_PREFIX | landing/postgres/ | extract target |
| RUN_DATE | today (UTC) | snapshot / quarantine partition date |
| RUN_ID | YYYYMMDDTHHMMSSZ-<6 hex> | run id in lineage + audit cols |
| STEPS | extract,bronze,silver,gold | stages to run |
| ENGINE | pandas | pandas or spark (else exit 1) |
| EXECUTION_ENV | local | docker / github-actions / kind; recorded in lineage |
| LOG_LEVEL | INFO | logging |
- `ConfigError` raised before any I/O for missing/invalid config.
- Logging format: `%(asctime)s level=%(levelname)s fn=%(funcName)s msg=%(message)s`.
- `retry(times=3, base_delay=1.0, exceptions=(Exception,))`: exponential backoff (1 s, 2 s), WARNING per retry, ERROR on give-up.

### 4.2 Pydantic models (`Base`: str_strip_whitespace=True, extra="ignore")
| Model | Rules |
|---|---|
| Team | team_id int, name str(min 1), department str(min 1) |
| Competency | competency_id int, name str(min1), category Literal[Technical, Leadership, Domain] |
| Employee | employee_id int, first_name/last_name str(min1), email (regex `^[^@\s]+@[^@\s]+\.[^@\s]+$`, lowercased), team_id int, job_level str(min1), hire_date date (not in future) |
| Project | project_id int, name, team_id int, outcome_status Literal[on-time, delayed, at-risk, cancelled], outcome_score float 0..100 |
| PerformanceReview | review_id, employee_id int, review_period `^\d{4}-H[12]$`, rating int 1..5, manager_comment Optional[str], submitted_date date |
| CompetencyAssessment | assessment_id, employee_id, competency_id int, proficiency_level/required_level int 1..5, assessed_date date |
| TalentReview | employee_id int, review_cycle `^\d{4}-H[12]$`, potential_rating Literal[Low,Medium,High], promotion_readiness Literal[Ready Now, 1-2 Years, Not Ready], attrition_risk Literal[Low,Medium,High] |
| LearningEvent | event_id `^EV\d{10}$`, employee_id int, event_date datetime, event_type Literal[enrollment, module_started, module_completed, quiz_attempted, quiz_passed, course_completed, certification_earned], course_name str(min1), text Optional[str], score Optional[int] 0..100; model validator: score only on quiz events |

- TABLES (load order -> model, PK): teams/team_id, competencies/competency_id, employees/employee_id,
  projects/project_id, performance_reviews/review_id, competency_assessments/assessment_id, talent_review/employee_id.
- FOREIGN_KEYS: employees.team_id->teams; projects.team_id->teams; performance_reviews.employee_id->employees;
  competency_assessments.employee_id->employees and .competency_id->competencies; talent_review.employee_id->employees.

### 4.3 Validation helpers
- `format_errors(exc)` -> "field: msg; ...". `make_reject(dataset, record, error, source)` -> {dataset, error, record(JSON str), source}.
- `read_raw_csv(raw)`: all columns as str, empty -> None (Pydantic decides types).
- `validate_table(df, model, pk, dataset, source)`: validate each row (source `file#L<line>`), then drop duplicate PKs (keep first) -> (valid_df, rejects).
- `split_orphans(df, col, parent_ids, ...)`: FK without parent -> reject `orphan <col>`.
- `flag_before_hire(df, date_col, employees)`: add `dq_before_hire` (record date < hire_date); rows KEPT.
- `partition_date(key)` from `year=/month=/day=`.
- `clean_events(lines, valid_employee_ids)`, in order: JSON parse (malformed -> reject) -> schema drift (`notes` and no `text` -> map to text, `schema_drift=True`) -> LearningEvent -> event_date must equal partition date -> duplicate event_id (keep first) -> orphan employee_id. Returns (events_df, rejects).

### 4.4 Lake (boto3 S3 wrapper)
endpoint_url + path-style addressing; `ensure_bucket`, `put`, `get`, `copy` (all @retry), `size(key)` (None on 404),
`list(prefix)` (paginated), `delete_prefix` (batches of 1,000), `write_parquet`, `read_parquet`, `read_parquet_prefix`, `write_json`.

### 4.5 Postgres export
`pg_export_csv(table)`: whitelist table name, `COPY <t> TO STDOUT WITH (FORMAT csv, HEADER true)` via copy_expert,
connect_timeout=10, @retry on psycopg2.OperationalError.

### 4.6 Lineage
`record(layer, dataset, source, target, rows_in, rows_out, rows_rejected, duration, note)`, rows_per_min = rows_in/duration*60,
WARNING when rows_out == 0. Report: run_id, run_date, status, error, started_at, ended_at, duration_s, bucket,
execution {environment, host, ...}, steps[], reconciliation[]. Written to lineage/runs/<run_id>.json + latest.json in `finally`.

### 4.7 Steps
- **extract**: per table, if landing key for RUN_DATE exists -> read it, note "already extracted today, skipped"; else COPY -> put -> verify_size (stored size == source, else IOError) -> note "exported". Count rows; warn on empty table.
- **bronze** (lake only, no Postgres): per table, `snapshot_date` = landing ingest_date for RUN_DATE else newest; if bronze key exists -> skip ("already in bronze, skipped", never overwrite); else copy + verify_size. Reconciliation check 1: bronze rows == landing rows. Mismatch -> RuntimeError. JSONL: copy landing -> bronze/s3/learning_activity/, skip same-size.
- **silver** (full refresh): per table in TABLES order: read bronze -> validate_table -> split_orphans per FK -> flag_before_hire (reviews on submitted_date, assessments on assessed_date) -> add audit cols (`_source`, `_run_id`, `_processed_at`) -> write parquet; rejects -> `write_quarantine` (delete run_date partition, write rejects.parquet + run_id, log top 5 reasons). Events: read all bronze lines (key, line_no, text) -> clean_events -> delete silver/learning_activity/ -> write one parquet per year/month.
- **gold**: build 7 tables (section 6), write each, run 8 Gold checks; any failure -> RuntimeError.
- **main**: Settings (ConfigError -> exit 1, no lineage), run STEPS in order; any exception -> FAILED + error repr; finally write lineage; exit 0 if SUCCEEDED else 1.

---

## 5. Data quality (issues planted by the generator)
| Issue | Handling | Manifest key |
|---|---|---|
| Duplicate event_id | quarantine, keep first | `planted.duplicate_event_id` |
| employee_id null / not int | quarantine | `planted.null_employee_id` |
| Unparseable event_date ("yesterday", "N/A", "") | quarantine | `planted.unparseable_date` |
| Malformed JSON | quarantine | `planted.malformed_json` |
| Impossible date (2026-13-45) | quarantine | `planted.impossible_date` |
| **Total quarantined (learning_activity)** | | `events.quarantined` |
| `notes` instead of `text` | mapped + kept, schema_drift=True (one drifted line also has a bad date, so it is quarantined) | `planted.schema_drift_kept` |
| Review before hire_date | flagged dq_before_hire, kept | `planted.reviews_before_hire` |
| Assessment before hire_date | flagged, kept | `planted.assessments_before_hire` |
| Postgres tables: PK dupes, orphans, bad enums/emails | quarantine per table | none planted |
Each rejected row counted once, under the first rule it fails.

---

## 6. Silver and Gold logic

### Silver row counts
Same as `manifest.json` `rows`; learning_activity = `events.kept`, one partition per month (`events.months`).

### Gold
- `employee_dim`: employees + `employee_name` (first + " " + last) + teams (`team_name`, `department`).
1. **gold_employee_review_history**: reviews + dim, sorted by employee, period; `prev_rating` = groupby(employee).rating.shift(1); `rating_change` = rating - prev_rating.
2. **gold_skill_gaps**: assessments + competencies (`competency_name`, `category`) + dim; `gap = required_level - proficiency_level`; `gap_severity` = critical if gap >= 2, minor if gap == 1, else none.
3. **gold_promotion_readiness**: per employee: latest_period, latest_rating, avg_rating (2 dp), rating_change (last); `critical_gaps` count (0 if none); merged with talent review; `is_high_potential = potential_rating == "High"`; **`is_promotion_ready = High AND promotion_readiness == "Ready Now" AND latest_rating >= 4`**.
4. **gold_training_summary**: pivot event counts per type -> `n_<type>` (all 7, 0-filled); `courses_completed` = distinct course_name among course_completed; `completed_course_list` (sorted, "; "-joined); `avg_quiz_score` (1 dp, non-null scores); `total_events`; `last_activity` = max event_date. Employees with no events -> zeros.
5. **gold_attrition_risk**: `as_of` = newest event_date (deterministic); `recent_learning_events` = events in 180 days before as_of. Drivers: hr_flag_high, hr_flag_medium, declining_rating (rating_change < 0), low_rating (latest <= 2), no_recent_learning. **`risk_score = 2*hr_flag_high + hr_flag_medium + declining_rating + low_rating + no_recent_learning`**; `computed_risk_tier` High >= 4, Medium 2-3, Low 0-1; `risk_drivers` comma-joined; `as_of_date`.
6. **gold_team_performance**: headcount; over the latest review_period: avg_rating, pct_high_performers (rating >= 4), pct_low_performers (rating <= 2); from projects: project_count, avg_outcome_score, pct_on_time, n_delayed_or_at_risk, has_projects; 2 dp. Pearson r(avg_rating, avg_outcome_score) over teams with projects, computed in findings, the notebook and the dashboard.
7. **gold_skill_distribution**: gold_skill_gaps grouped by department x category x competency_name: n_assessments, employees_assessed (nunique), avg_proficiency, avg_required, pct_meeting_required (% with gap <= 0), critical_gap_count; 2 dp.

### Reconciliation (9 checks; run fails on any mismatch)
| # | Layer | Check |
|---|---|---|
| 1 | bronze | bronze rows == landing rows (postgres) |
| 2 | gold | review_history rows == silver reviews |
| 3 | gold | skill_gaps rows == silver assessments |
| 4 | gold | promotion rows == employees |
| 5 | gold | attrition rows == employees |
| 6 | gold | training rows == employees |
| 7 | gold | training event total == silver events |
| 8 | gold | team rows == silver teams |
| 9 | gold | skill_distribution assessments == silver assessments |

---

## 7. Acceptance gate
The pipeline must reproduce `manifest.json` for any seed and size:
- Run: SUCCEEDED, 9/9; events in/kept/quarantined == `events.lines` / `events.kept` / `events.quarantined`; quarantine split == `quarantine_by_reason`. Rerun -> identical counts (idempotent).
- Findings == `expected` (ratings, skill gaps, promotion, training, attrition, teams, skills). The generator computes `expected` from its clean records in plain Python, independently of `job.py`.
If a number differs, fix the logic (not the expectation) and explain why.

---

## 8. Dashboard (`scripts/build_dashboard.py` -> `site/index.html`)
- Reads 7 Gold tables + lineage/latest.json via the same Lake/Settings.
- Single self-contained HTML, inline SVG charts (matplotlib), no JavaScript, no external links.
- **Aggregate only**: no employee_id, names, emails or per-employee rows (enforced by a test).
- Header: run_id, status, reconciliation passed/total, duration, execution environment.
- Sections (KPIs + charts): 1 Ratings (avg by period, change counts), 2 Skill gaps (severity, top 10 competencies), 3 Promotion (by department), 4 Training (events by type, dept table), 5 Attrition (computed tier vs HR flag, driver frequency), 6 Teams (avg rating by on-time band 0% / 1-50% / 51-100%), 7 Skills (lowest 8 % meeting required, dept x category table). Footer: DQ summary + architecture.
- Also publish: insights.html (notebook export), lineage.json, optional gold/*.csv.

## 9. Notebook (`notebooks/insights.ipynb`)
12 charts: rows out per dataset (bronze vs silver); rating distribution by period; avg rating by dept and period;
top 15 competencies by critical gaps; potential vs readiness; promotion-ready by job level; events by type;
courses completed distribution; computed risk tiers; team rating vs project outcome scatter (r); proficiency heatmap
dept x category (weighted); 15 lowest % meeting required. Lookups: `review_history(employee_id)`,
`training_history(employee_id)`. Final section: assumptions and limitations.

## 10. Tests (target ~57, ~95%+ coverage)
- Schema: bad email + future hire; rating range 1..5 (parametrized); enums.
- Helpers: validate_table quarantines + dedupes; split_orphans; flag_before_hire; score_only_on_quiz.
- Events: clean_events handles every planted issue; idempotent.
- Gold: one test per Gold table rule; reconciliation passes; timings reported.
- Lineage, retry (recovers then gives up; no retry on unlisted errors), main returns 1 and writes FAILED lineage.
- Config: fail fast on missing config; cloud/no-extract needs only bucket; default STEPS start with extract.
- Extract/Bronze with an in-memory fake lake: immutable landing; bronze copies without Postgres; never overwrites; fails on row mismatch; missing snapshot raises.
- Integration (Postgres + MinIO): COPY export reads rows; parquet roundtrip; end-to-end twice is idempotent (9/9, identical counts).
- Dashboard: shows run metadata + all 7 questions; aggregate-only and self-contained; failed run flagged.
- Optional Spark: Gold parity with pandas; timezone regression (use UTC session tz, disable Arrow fallback, localize naive timestamps to UTC: original bug shifted timestamps by 5h30m).

## 11. Performance
SLA: full run < 5 min and < 2 GB at 10x the default size. Known bottleneck on real S3: one small JSONL file per day, read sequentially (fix: parallel reads / compaction).

## 12. Design decisions (keep)
- Immutable landing + bronze; full-refresh silver + gold -> idempotent, rebuildable from raw.
- Flag, don't drop, pre-hire records (dropping them would bias results).
- Quarantine with reason + source line; nothing silently lost.
- Rule-based, explainable attrition score; "recent" measured from newest event (deterministic).
- pandas default, Spark optional with parity tests.
- One job.py, STEPS selects stages; lineage per run with execution environment.

## 13. History
An earlier version ran on AWS: extract on a cloud desktop, Bronze/Silver/Gold as a Kubernetes Job on Amazon EKS
(Terraform + Helm, hostNetwork for node-role credentials, S3 bucket policy scoped to the node role), lake in Amazon S3,
dashboard on CloudFront. Lessons: the IAM simulator returned implicitDeny for eks:CreateCluster but the real call succeeded;
the first EKS run hit S3 AccessDenied (fixed with a scoped bucket policy, no IAM changes); pinning
numpy<2 broke pyarrow 26 (reverted). This repo is the portable version: no cloud account needed.
