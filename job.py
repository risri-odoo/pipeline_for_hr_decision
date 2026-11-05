"""ACME Talent ETL: Postgres + learning events -> medallion lake (landing/bronze/silver/gold).

One file, four stages selected with STEPS (extract, bronze, silver, gold).
Storage is any S3-compatible endpoint (MinIO locally and in CI).
Every run writes a lineage report (lineage/runs/<run_id>.json + lineage/latest.json).
"""

from __future__ import annotations

import argparse
import csv
import functools
import hashlib
import io
import json
import logging
import os
import platform
import re
import secrets
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, Optional

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

log = logging.getLogger("acme_etl")

ALL_STEPS = ("extract", "bronze", "silver", "gold")
ENGINES = ("pandas",)

# Three independently runnable layers; each groups one or more steps.
LAYERS: dict[str, tuple[str, ...]] = {"bronze": ("extract", "bronze"), "silver": ("silver",), "gold": ("gold",)}
LAYER_OF = {step: layer for layer, steps in LAYERS.items() for step in steps}
LAYER_CHOICES = (*LAYERS, "all")
REQUIRES = {"silver": "bronze", "gold": "silver"}  # layer -> upstream layer whose _SUCCESS marker it needs


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


class ConfigError(Exception):
    """Missing or invalid configuration; raised before any I/O."""


def new_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"{now:%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


@dataclass(frozen=True)
class Settings:
    lake_bucket: str
    s3_endpoint: str = "http://localhost:9000"
    aws_region: str = "us-east-1"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_name: str = "postgres"
    postgres_user: str = "postgres"
    postgres_pass: str = field(default="", repr=False)
    landing_prefix: str = "landing/learning_activity/"
    landing_pg_prefix: str = "landing/postgres/"
    events_source_dir: str = ""
    run_date: str = ""
    run_id: str = ""
    steps: tuple[str, ...] = ALL_STEPS
    layer: str = ""
    engine: str = "pandas"
    execution_env: str = "local"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Settings":
        env = dict(os.environ if env is None else env)
        get = lambda k, d="": (env.get(k) or d).strip()  # noqa: E731

        bucket = get("LAKE_BUCKET")
        if not bucket:
            raise ConfigError("LAKE_BUCKET is required")

        layer = get("LAYER").lower()
        if layer:
            if layer not in LAYER_CHOICES:
                raise ConfigError(f"LAYER must be one of {', '.join(LAYER_CHOICES)}; got {layer!r}")
            steps = ALL_STEPS if layer == "all" else LAYERS[layer]
        else:
            steps = tuple(s.strip().lower() for s in get("STEPS", ",".join(ALL_STEPS)).split(",") if s.strip())
            bad = [s for s in steps if s not in ALL_STEPS]
            if not steps or bad:
                raise ConfigError(f"STEPS must be a subset of {','.join(ALL_STEPS)}; got {get('STEPS')!r}")
            steps = tuple(sorted(set(steps), key=ALL_STEPS.index))  # always run in pipeline order

        engine = get("ENGINE", "pandas").lower()
        if engine not in ENGINES:
            raise ConfigError(f"ENGINE must be one of {ENGINES} (spark is not implemented); got {engine!r}")

        run_date = get("RUN_DATE", datetime.now(timezone.utc).date().isoformat())
        try:
            date.fromisoformat(run_date)
        except ValueError as exc:
            raise ConfigError(f"RUN_DATE must be YYYY-MM-DD; got {run_date!r}") from exc

        try:
            port = int(get("POSTGRES_PORT", "5432"))
        except ValueError as exc:
            raise ConfigError("POSTGRES_PORT must be an integer") from exc

        level = get("LOG_LEVEL", "INFO").upper()
        if level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
            raise ConfigError(f"LOG_LEVEL invalid: {level!r}")

        settings = cls(
            lake_bucket=bucket,
            s3_endpoint=get("S3_ENDPOINT", "http://localhost:9000"),
            aws_region=get("AWS_REGION", "us-east-1"),
            postgres_host=get("POSTGRES_HOST", "localhost"),
            postgres_port=port,
            postgres_name=get("POSTGRES_NAME", "postgres"),
            postgres_user=get("POSTGRES_USER", "postgres"),
            postgres_pass=env.get("POSTGRES_PASS", ""),
            landing_prefix=_prefix(get("LANDING_PREFIX", "landing/learning_activity/")),
            landing_pg_prefix=_prefix(get("LANDING_PG_PREFIX", "landing/postgres/")),
            events_source_dir=get("EVENTS_SOURCE_DIR"),
            run_date=run_date,
            run_id=get("RUN_ID") or new_run_id(),
            steps=steps,
            layer=layer,
            engine=engine,
            execution_env=get("EXECUTION_ENV", "local"),
            log_level=level,
        )
        if "extract" in steps and not (settings.postgres_host and settings.postgres_name and settings.postgres_user):
            raise ConfigError("POSTGRES_HOST, POSTGRES_NAME and POSTGRES_USER are required for extract")
        return settings


def _prefix(p: str) -> str:
    return p if p.endswith("/") else p + "/"


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(format="%(asctime)s level=%(levelname)s fn=%(funcName)s msg=%(message)s")
    logging.getLogger().setLevel(getattr(logging, level, logging.INFO))


def retry(times: int = 3, base_delay: float = 1.0, exceptions: tuple[type[BaseException], ...] = (Exception,)):
    """Retry with exponential backoff (base_delay, 2*base_delay, ...)."""

    def deco(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for attempt in range(1, times + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    if attempt == times:
                        log.error("%s failed after %d attempts: %r", fn.__name__, times, exc)
                        raise
                    delay = base_delay * 2 ** (attempt - 1)
                    log.warning("%s attempt %d/%d failed (%r); retrying in %.1fs", fn.__name__, attempt, times, exc, delay)
                    time.sleep(delay)

        return wrapper

    return deco


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PERIOD = r"^\d{4}-H[12]$"
QUIZ_EVENTS = {"quiz_attempted", "quiz_passed"}
EVENT_TYPES = (
    "enrollment",
    "module_started",
    "module_completed",
    "quiz_attempted",
    "quiz_passed",
    "course_completed",
    "certification_earned",
)


class Base(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="ignore")


class Team(Base):
    team_id: int
    name: str = Field(min_length=1)
    department: str = Field(min_length=1)


class Competency(Base):
    competency_id: int
    name: str = Field(min_length=1)
    category: Literal["Technical", "Leadership", "Domain"]


class Employee(Base):
    employee_id: int
    first_name: str = Field(min_length=1)
    last_name: str = Field(min_length=1)
    email: str
    team_id: int
    job_level: str = Field(min_length=1)
    hire_date: date

    @field_validator("email")
    @classmethod
    def _email(cls, v: str) -> str:
        if not EMAIL_RE.match(v):
            raise ValueError("invalid email")
        return v.lower()

    @field_validator("hire_date")
    @classmethod
    def _not_future(cls, v: date) -> date:
        if v > date.today():
            raise ValueError("hire_date is in the future")
        return v


class Project(Base):
    project_id: int
    name: str = Field(min_length=1)
    team_id: int
    outcome_status: Literal["on-time", "delayed", "at-risk", "cancelled"]
    outcome_score: float = Field(ge=0, le=100)


class PerformanceReview(Base):
    review_id: int
    employee_id: int
    review_period: str = Field(pattern=PERIOD)
    rating: int = Field(ge=1, le=5)
    manager_comment: Optional[str] = None
    submitted_date: date


class CompetencyAssessment(Base):
    assessment_id: int
    employee_id: int
    competency_id: int
    proficiency_level: int = Field(ge=1, le=5)
    required_level: int = Field(ge=1, le=5)
    assessed_date: date


class TalentReview(Base):
    employee_id: int
    review_cycle: str = Field(pattern=PERIOD)
    potential_rating: Literal["Low", "Medium", "High"]
    promotion_readiness: Literal["Ready Now", "1-2 Years", "Not Ready"]
    attrition_risk: Literal["Low", "Medium", "High"]


class LearningEvent(Base):
    event_id: str = Field(pattern=r"^EV\d{10}$")
    employee_id: int
    event_date: datetime
    event_type: Literal[EVENT_TYPES]  # type: ignore[valid-type]
    course_name: str = Field(min_length=1)
    text: Optional[str] = None
    score: Optional[int] = Field(default=None, ge=0, le=100)

    @model_validator(mode="after")
    def _score_only_on_quiz(self) -> "LearningEvent":
        if self.score is not None and self.event_type not in QUIZ_EVENTS:
            raise ValueError("score only allowed on quiz events")
        return self


# load order -> (model, primary key)
TABLES: dict[str, tuple[type[Base], str]] = {
    "teams": (Team, "team_id"),
    "competencies": (Competency, "competency_id"),
    "employees": (Employee, "employee_id"),
    "projects": (Project, "project_id"),
    "performance_reviews": (PerformanceReview, "review_id"),
    "competency_assessments": (CompetencyAssessment, "assessment_id"),
    "talent_review": (TalentReview, "employee_id"),
}

# child table -> [(column, parent table, parent pk)]
FOREIGN_KEYS: dict[str, list[tuple[str, str, str]]] = {
    "employees": [("team_id", "teams", "team_id")],
    "projects": [("team_id", "teams", "team_id")],
    "performance_reviews": [("employee_id", "employees", "employee_id")],
    "competency_assessments": [
        ("employee_id", "employees", "employee_id"),
        ("competency_id", "competencies", "competency_id"),
    ],
    "talent_review": [("employee_id", "employees", "employee_id")],
}

BEFORE_HIRE_COLS = {"performance_reviews": "submitted_date", "competency_assessments": "assessed_date"}

# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

REJECT_COLUMNS = ["dataset", "error", "record", "source"]


def format_errors(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"]) or "record"
        msg = err["msg"].removeprefix("Value error, ")
        parts.append(f"{loc}: {msg}")
    return "; ".join(parts)


def make_reject(dataset: str, record: Any, error: str, source: str) -> dict[str, str]:
    if not isinstance(record, str):
        record = json.dumps(record, default=str, sort_keys=True)
    return {"dataset": dataset, "error": error, "record": record, "source": source}


def read_raw_csv(raw: bytes) -> pd.DataFrame:
    """All columns as text, empty -> None; Pydantic decides the types."""
    df = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False)
    return df.astype(object).where(df != "", None)


def _date_fields(model: type[BaseModel]) -> list[str]:
    return [n for n, f in model.model_fields.items() if f.annotation in (date, datetime)]


def models_to_frame(rows: list[dict], model: type[BaseModel]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=list(model.model_fields) + [c for c in (rows[0] if rows else {}) if c not in model.model_fields])
    for col in _date_fields(model):
        df[col] = pd.to_datetime(df[col])
    return df


def validate_table(df: pd.DataFrame, model: type[BaseModel], pk: str, dataset: str, source: str):
    """Validate each row, then drop duplicate PKs (keep first). Returns (valid_df, rejects)."""
    good: list[dict] = []
    rejects: list[dict] = []
    seen: set = set()
    for i, rec in enumerate(df.to_dict(orient="records")):
        src = f"{source}#L{i + 2}"  # +1 header, +1 one-based
        try:
            obj = model.model_validate(rec)
        except ValidationError as exc:
            rejects.append(make_reject(dataset, rec, format_errors(exc), src))
            continue
        key = getattr(obj, pk)
        if key in seen:
            rejects.append(make_reject(dataset, rec, f"duplicate {pk}", src))
            continue
        seen.add(key)
        good.append(obj.model_dump())
    return models_to_frame(good, model), rejects


def split_orphans(df: pd.DataFrame, col: str, parent_ids: Iterable, dataset: str, source: str):
    """Rows whose FK has no parent -> rejects (`orphan <col>`)."""
    mask = df[col].isin(set(parent_ids))
    rejects = [make_reject(dataset, rec, f"orphan {col}", source) for rec in df.loc[~mask].to_dict(orient="records")]
    return df.loc[mask].reset_index(drop=True), rejects


def flag_before_hire(df: pd.DataFrame, date_col: str, employees: pd.DataFrame) -> pd.DataFrame:
    """Add dq_before_hire (record date earlier than hire_date). Rows are kept."""
    hire = employees.set_index("employee_id")["hire_date"]
    out = df.copy()
    out["dq_before_hire"] = (out[date_col] < out["employee_id"].map(hire)).fillna(False).astype(bool)
    return out


_PART_RE = re.compile(r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})")


def partition_date(key: str) -> Optional[date]:
    m = _PART_RE.search(key)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def clean_events(lines: Iterable[tuple[str, int, str]], valid_employee_ids: Iterable[int]):
    """Parse + validate JSONL lines. Each reject is counted once, under the first rule it fails:
    malformed JSON -> schema (Pydantic) -> partition date mismatch -> duplicate event_id -> orphan employee_id.
    Schema drift (`notes` instead of `text`) is mapped and kept with schema_drift=True.
    """
    valid_ids = set(valid_employee_ids)
    seen: set[str] = set()
    rows: list[dict] = []
    rejects: list[dict] = []
    for key, line_no, text in lines:
        src = f"{key}#L{line_no}"
        try:
            obj = json.loads(text)
        except json.JSONDecodeError as exc:
            rejects.append(make_reject("learning_activity", text, f"malformed json: {exc.msg}", src))
            continue
        if not isinstance(obj, dict):
            rejects.append(make_reject("learning_activity", text, "malformed json: not an object", src))
            continue
        drift = "notes" in obj and "text" not in obj
        if drift:
            obj["text"] = obj.pop("notes")
        try:
            ev = LearningEvent.model_validate(obj)
        except ValidationError as exc:
            rejects.append(make_reject("learning_activity", obj, format_errors(exc), src))
            continue
        pdate = partition_date(key)
        if pdate is not None and ev.event_date.date() != pdate:
            rejects.append(make_reject("learning_activity", obj, f"event_date {ev.event_date.date()} != partition {pdate}", src))
            continue
        if ev.event_id in seen:
            rejects.append(make_reject("learning_activity", obj, "duplicate event_id", src))
            continue
        seen.add(ev.event_id)
        if ev.employee_id not in valid_ids:
            rejects.append(make_reject("learning_activity", obj, "orphan employee_id", src))
            continue
        rows.append({**ev.model_dump(), "schema_drift": drift, "_source": src})
    cols = list(LearningEvent.model_fields) + ["schema_drift", "_source"]
    df = pd.DataFrame(rows, columns=cols)
    df["event_date"] = pd.to_datetime(df["event_date"])
    df["employee_id"] = df["employee_id"].astype("int64")
    df["score"] = df["score"].astype("Int64")
    df["schema_drift"] = df["schema_drift"].astype(bool)
    return df, rejects


def reject_reason(error: str) -> str:
    """Short category for a reject message (for logs and the DQ summary)."""
    if error.startswith("malformed json"):
        return "malformed json"
    if error.startswith("duplicate"):
        return error
    if error.startswith("orphan"):
        return error
    if error.startswith("event_date ") and "partition" in error:
        return "event_date != partition"
    first = error.split(";")[0]
    field = first.split(":")[0]
    if field == "event_date":
        return "impossible date" if "range" in first else "unparseable date"
    if field == "employee_id":
        return "employee_id null / not int"
    return f"{field}: invalid"


def add_audit_cols(df: pd.DataFrame, source: str, run_id: str) -> pd.DataFrame:
    out = df.copy()
    if "_source" not in out.columns:
        out["_source"] = source
    out["_run_id"] = run_id
    out["_processed_at"] = pd.Timestamp.now(tz="UTC")
    return out


# ---------------------------------------------------------------------------
# Lake (S3 API)
# ---------------------------------------------------------------------------


class Lake:
    """Thin boto3 wrapper; works with MinIO (path-style) or AWS S3."""

    def __init__(self, bucket: str, endpoint: str | None = None, region: str = "us-east-1", client: Any = None):
        self.bucket = bucket
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                endpoint_url=endpoint or None,
                region_name=region,
                config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"}),
            )
        self.s3 = client
        self.region = region

    @classmethod
    def from_settings(cls, s: Settings) -> "Lake":
        return cls(s.lake_bucket, s.s3_endpoint, s.aws_region)

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    @retry()
    def ensure_bucket(self) -> None:
        from botocore.exceptions import ClientError

        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError:
            kwargs: dict = {"Bucket": self.bucket}
            if self.region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
            self.s3.create_bucket(**kwargs)
            log.info("created bucket %s", self.bucket)

    @retry()
    def put(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    @retry()
    def get(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    @retry()
    def copy(self, src: str, dst: str) -> None:
        self.s3.copy_object(Bucket=self.bucket, Key=dst, CopySource={"Bucket": self.bucket, "Key": src})

    def size(self, key: str) -> Optional[int]:
        from botocore.exceptions import ClientError

        try:
            return int(self.s3.head_object(Bucket=self.bucket, Key=key)["ContentLength"])
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    def list(self, prefix: str) -> list[str]:
        keys: list[str] = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(o["Key"] for o in page.get("Contents", []))
        return sorted(keys)

    def fingerprints(self, prefix: str) -> dict[str, str]:
        """key -> content fingerprint (the object's ETag, an MD5 for these single-part objects)."""
        out: dict[str, str] = {}
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            out.update({o["Key"]: o["ETag"].strip('"') for o in page.get("Contents", [])})
        return out

    def delete_prefix(self, prefix: str) -> int:
        keys = self.list(prefix)
        for i in range(0, len(keys), 1000):
            batch = [{"Key": k} for k in keys[i : i + 1000]]
            self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": batch, "Quiet": True})
        return len(keys)

    # helpers built on the primitives above (shared with the in-memory test lake)
    def exists(self, key: str) -> bool:
        return self.size(key) is not None

    def write_parquet(self, df: pd.DataFrame, key: str) -> None:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        self.put(key, buf.getvalue())

    def read_parquet(self, key: str) -> pd.DataFrame:
        return pd.read_parquet(io.BytesIO(self.get(key)))

    def read_parquet_prefix(self, prefix: str) -> pd.DataFrame:
        frames = [self.read_parquet(k) for k in self.list(prefix) if k.endswith(".parquet")]
        if not frames:
            raise FileNotFoundError(f"no parquet under {self.uri(prefix)}")
        return pd.concat(frames, ignore_index=True)

    def write_json(self, key: str, obj: Any) -> None:
        self.put(key, json.dumps(obj, indent=2, default=str).encode())

    def read_json(self, key: str) -> Any:
        return json.loads(self.get(key))


def verify_size(lake: Lake, key: str, expected: int) -> None:
    actual = lake.size(key)
    if actual != expected:
        raise IOError(f"size mismatch for {lake.uri(key)}: stored {actual}, expected {expected}")


# ---------------------------------------------------------------------------
# Postgres export
# ---------------------------------------------------------------------------

_PG_ERRORS: tuple[type[BaseException], ...]
try:  # psycopg2 is only needed for extract
    import psycopg2

    _PG_ERRORS = (psycopg2.OperationalError,)
except ImportError:  # pragma: no cover
    psycopg2 = None
    _PG_ERRORS = (ConnectionError,)


@retry(exceptions=_PG_ERRORS)
def pg_export_csv(settings: Settings, table: str) -> bytes:
    """COPY one whitelisted table to CSV bytes (with header)."""
    if table not in TABLES:
        raise ValueError(f"table not allowed: {table!r}")
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_name,
        user=settings.postgres_user,
        password=settings.postgres_pass,
        connect_timeout=10,
    )
    try:
        buf = io.BytesIO()
        with conn.cursor() as cur:
            cur.copy_expert(f"COPY {table} TO STDOUT WITH (FORMAT csv, HEADER true)", buf)
        return buf.getvalue()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


REFUSED = "REFUSED"  # a layer run that stopped at its dependency check, before touching any data


class Lineage:
    """Lineage of one layer run (or of a whole run when layer is None)."""

    def __init__(self, settings: Settings, layer: str | None = None):
        self.settings = settings
        self.layer = layer
        self.upstream: dict | None = None
        self.data_checksum: str | None = None  # identifies the input data this layer's output came from
        self.warnings: list[str] = []
        self.started = datetime.now(timezone.utc)
        self.steps: list[dict] = []
        self.reconciliation: list[dict] = []

    def record(self, layer, dataset, source, target, rows_in, rows_out, rows_rejected=0, duration=0.0, note=""):
        duration = round(float(duration), 3)
        rpm = round(rows_in / duration * 60, 1) if duration > 0 else None
        if rows_out == 0:
            log.warning("%s/%s produced 0 rows", layer, dataset)
        entry = {
            "layer": layer,
            "dataset": dataset,
            "source": source,
            "target": target,
            "rows_in": int(rows_in),
            "rows_out": int(rows_out),
            "rows_rejected": int(rows_rejected),
            "duration_s": duration,
            "rows_per_min": rpm,
            "note": note,
        }
        self.steps.append(entry)
        log.info("%s %s in=%d out=%d rejected=%d %.2fs %s", layer, dataset, rows_in, rows_out, rows_rejected, duration, note)
        return entry

    def check(self, layer: str, name: str, expected, actual) -> bool:
        passed = expected == actual
        self.reconciliation.append({"layer": layer, "check": name, "expected": expected, "actual": actual, "passed": passed})
        (log.info if passed else log.error)("check %s: expected=%s actual=%s %s", name, expected, actual, "PASS" if passed else "FAIL")
        return passed

    def report(self, status: str, error: str | None = None) -> dict:
        ended = datetime.now(timezone.utc)
        s = self.settings
        return {
            "run_id": s.run_id,
            "layer": self.layer,
            "upstream": self.upstream,
            "data_checksum": self.data_checksum,
            "run_date": s.run_date,
            "status": status,
            "error": error,
            "warnings": self.warnings,
            "started_at": self.started.isoformat(),
            "ended_at": ended.isoformat(),
            "duration_s": round((ended - self.started).total_seconds(), 2),
            "bucket": s.lake_bucket,
            "steps_requested": list(s.steps),
            "execution": {
                "environment": s.execution_env,
                "host": socket.gethostname(),
                "engine": s.engine,
                "python": platform.python_version(),
                "pandas": pd.__version__,
                "pid": os.getpid(),
            },
            "steps": self.steps,
            "reconciliation": self.reconciliation,
        }

    def write(self, lake: Lake, status: str, error: str | None = None) -> dict:
        rep = self.report(status, error)
        base = f"lineage/{self.layer}/" if self.layer else "lineage/"
        lake.write_json(f"{base}runs/{self.settings.run_id}.json", rep)
        if status != REFUSED:  # a refused run changed nothing, so it isn't the layer's latest run
            lake.write_json(f"{base}latest.json", rep)
        return rep


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

BRONZE_PG = "bronze/postgres/"
BRONZE_EVENTS = "bronze/s3/learning_activity/"
SILVER = "silver/"
SILVER_EVENTS = "silver/learning_activity/"
QUARANTINE = "silver/_quarantine/"
GOLD = "gold/"
GOLD_SITE_DATA = "gold/_site/findings.json"


def count_csv_rows(raw: bytes) -> int:
    return sum(1 for _ in csv.reader(io.StringIO(raw.decode("utf-8")))) - 1 if raw else 0


def snapshot_date(lake: Lake, table_prefix: str, run_date: str) -> str:
    """ingest_date for run_date if present, else the newest one."""
    dates = sorted({m.group(1) for k in lake.list(table_prefix) if (m := re.search(r"ingest_date=(\d{4}-\d{2}-\d{2})/", k))})
    if not dates:
        raise RuntimeError(f"no snapshot under {lake.uri(table_prefix)}")
    return run_date if run_date in dates else dates[-1]


def pg_key(prefix: str, table: str, day: str) -> str:
    return f"{prefix}{table}/ingest_date={day}/{table}.csv"


def step_extract(s: Settings, lake: Lake, lin: Lineage, exporter: Callable[[Settings, str], bytes] = pg_export_csv) -> None:
    for table in TABLES:
        t0 = time.perf_counter()
        key = pg_key(s.landing_pg_prefix, table, s.run_date)
        if lake.exists(key):
            data, note = lake.get(key), "already extracted today, skipped"
        else:
            data = exporter(s, table)
            lake.put(key, data)
            verify_size(lake, key, len(data))
            note = "exported"
        rows = count_csv_rows(data)
        if rows == 0:
            log.warning("table %s is empty", table)
        lin.record("landing", table, f"postgres:{table}", lake.uri(key), rows, rows, 0, time.perf_counter() - t0, note)
    if s.events_source_dir:
        upload_events(s, lake, lin)


def upload_events(s: Settings, lake: Lake, lin: Lineage) -> None:
    """Upload the JSONL to landing once (files already present are skipped)."""
    t0 = time.perf_counter()
    root = Path(s.events_source_dir)
    files = sorted(root.rglob("*.jsonl"))
    if not files:
        raise RuntimeError(f"no .jsonl files under {root}")
    uploaded = lines = 0
    for f in files:
        key = s.landing_prefix + f.relative_to(root).as_posix()
        data = f.read_bytes()
        lines += data.count(b"\n") + (0 if data.endswith(b"\n") or not data else 1)
        if lake.size(key) == len(data):
            continue
        lake.put(key, data)
        uploaded += 1
    note = f"{len(files)} files, {uploaded} uploaded, {len(files) - uploaded} already present"
    lin.record("landing", "learning_activity", f"file:{root}", lake.uri(s.landing_prefix), lines, lines, 0, time.perf_counter() - t0, note)


def bronze_checksum(lake: Lake, days: dict[str, str]) -> str:
    """Hash of the bronze content a layer reads. Ignores run ids, dates and timestamps:
    the same input re-landed on another day gives the same checksum."""
    parts = []
    for table, day in days.items():
        parts += [f"{table}:{fp}" for fp in lake.fingerprints(pg_key(BRONZE_PG, table, day)).values()]
    parts += [f"{key[len(BRONZE_EVENTS):]}:{fp}" for key, fp in lake.fingerprints(BRONZE_EVENTS).items()]
    return hashlib.sha256("\n".join(sorted(parts)).encode()).hexdigest()


def step_bronze(s: Settings, lake: Lake, lin: Lineage) -> None:
    landing_total = bronze_total = 0
    days: dict[str, str] = {}
    for table in TABLES:
        t0 = time.perf_counter()
        day = days[table] = snapshot_date(lake, f"{s.landing_pg_prefix}{table}/", s.run_date)
        src, dst = pg_key(s.landing_pg_prefix, table, day), pg_key(BRONZE_PG, table, day)
        landing_rows = count_csv_rows(lake.get(src))
        if lake.exists(dst):
            note = "already in bronze, skipped"
        else:
            lake.copy(src, dst)
            verify_size(lake, dst, lake.size(src))
            note = f"copied snapshot {day}"
        bronze_rows = count_csv_rows(lake.get(dst))
        landing_total += landing_rows
        bronze_total += bronze_rows
        lin.record("bronze", table, lake.uri(src), lake.uri(dst), landing_rows, bronze_rows, 0, time.perf_counter() - t0, note)
    if not lin.check("bronze", "bronze rows == landing rows (postgres)", landing_total, bronze_total):
        raise RuntimeError(f"bronze reconciliation failed: landing={landing_total} bronze={bronze_total}")

    t0 = time.perf_counter()
    keys = [k for k in lake.list(s.landing_prefix) if k.endswith(".jsonl")]
    copied = 0
    for src in keys:
        dst = BRONZE_EVENTS + src[len(s.landing_prefix) :]
        src_size = lake.size(src)
        if lake.size(dst) == src_size:
            continue
        lake.copy(src, dst)
        verify_size(lake, dst, src_size)
        copied += 1
    note = f"{copied} copied, {len(keys) - copied} already present (same size)"
    lin.record("bronze", "learning_activity", lake.uri(s.landing_prefix), lake.uri(BRONZE_EVENTS), len(keys), len(keys), 0, time.perf_counter() - t0, note)
    lin.data_checksum = bronze_checksum(lake, days)


def write_quarantine(s: Settings, lake: Lake, dataset: str, rejects: list[dict]) -> None:
    prefix = f"{QUARANTINE}{dataset}/run_date={s.run_date}/"
    lake.delete_prefix(prefix)
    if not rejects:
        return
    df = pd.DataFrame(rejects, columns=REJECT_COLUMNS)
    df["run_id"] = s.run_id
    lake.write_parquet(df, prefix + "rejects.parquet")
    top = pd.Series([reject_reason(e) for e in df["error"]]).value_counts().head(5)
    log.warning("%s: %d rejects quarantined; top reasons: %s", dataset, len(df), "; ".join(f"{k}={v}" for k, v in top.items()))


def read_bronze_lines(lake: Lake) -> list[tuple[str, int, str]]:
    lines: list[tuple[str, int, str]] = []
    for key in (k for k in lake.list(BRONZE_EVENTS) if k.endswith(".jsonl")):
        for i, text in enumerate(lake.get(key).decode("utf-8").splitlines(), 1):
            if text.strip():
                lines.append((key, i, text))
    return lines


def step_silver(s: Settings, lake: Lake, lin: Lineage) -> None:
    valid: dict[str, pd.DataFrame] = {}
    for table, (model, pk) in TABLES.items():
        t0 = time.perf_counter()
        day = snapshot_date(lake, f"{BRONZE_PG}{table}/", s.run_date)
        key = pg_key(BRONZE_PG, table, day)
        raw = read_raw_csv(lake.get(key))
        good, rejects = validate_table(raw, model, pk, table, key)
        for col, parent, parent_pk in FOREIGN_KEYS.get(table, []):
            good, orphans = split_orphans(good, col, valid[parent][parent_pk], table, key)
            rejects += orphans
        note = ""
        if table in BEFORE_HIRE_COLS:
            good = flag_before_hire(good, BEFORE_HIRE_COLS[table], valid["employees"])
            note = f"{int(good['dq_before_hire'].sum())} flagged dq_before_hire (kept)"
        good = add_audit_cols(good, lake.uri(key), s.run_id)
        target = f"{SILVER}{table}/{table}.parquet"
        lake.write_parquet(good, target)
        write_quarantine(s, lake, table, rejects)
        valid[table] = good
        lin.record("silver", table, lake.uri(key), lake.uri(target), len(raw), len(good), len(rejects), time.perf_counter() - t0, note)

    t0 = time.perf_counter()
    lines = read_bronze_lines(lake)
    events, rejects = clean_events(lines, valid["employees"]["employee_id"])
    events = add_audit_cols(events, "", s.run_id)
    lake.delete_prefix(SILVER_EVENTS)
    parts = events.groupby([events["event_date"].dt.year, events["event_date"].dt.month], sort=True)
    for (y, m), part in parts:
        lake.write_parquet(part.reset_index(drop=True), f"{SILVER_EVENTS}year={y:04d}/month={m:02d}/part.parquet")
    write_quarantine(s, lake, "learning_activity", rejects)
    note = f"{parts.ngroups} monthly partitions; {int(events['schema_drift'].sum())} schema-drift rows mapped (notes->text)"
    lin.record("silver", "learning_activity", lake.uri(BRONZE_EVENTS), lake.uri(SILVER_EVENTS), len(lines), len(events), len(rejects), time.perf_counter() - t0, note)


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------

GOLD_TABLES = (
    "gold_employee_review_history",
    "gold_skill_gaps",
    "gold_promotion_readiness",
    "gold_training_summary",
    "gold_attrition_risk",
    "gold_team_performance",
    "gold_skill_distribution",
)
RECENT_DAYS = 180


def employee_dim(employees: pd.DataFrame, teams: pd.DataFrame) -> pd.DataFrame:
    dim = employees[["employee_id", "first_name", "last_name", "team_id", "job_level", "hire_date"]].copy()
    dim["employee_name"] = dim["first_name"] + " " + dim["last_name"]
    t = teams[["team_id", "name", "department"]].rename(columns={"name": "team_name"})
    return dim.drop(columns=["first_name", "last_name"]).merge(t, on="team_id", how="left")


def gold_employee_review_history(reviews: pd.DataFrame, dim: pd.DataFrame) -> pd.DataFrame:
    cols = ["review_id", "employee_id", "review_period", "rating", "manager_comment", "submitted_date", "dq_before_hire"]
    df = reviews[cols].merge(dim, on="employee_id", how="left")
    df = df.sort_values(["employee_id", "review_period", "review_id"]).reset_index(drop=True)
    df["prev_rating"] = df.groupby("employee_id")["rating"].shift(1)
    df["rating_change"] = df["rating"] - df["prev_rating"]
    return df


def gold_skill_gaps(assessments: pd.DataFrame, competencies: pd.DataFrame, dim: pd.DataFrame) -> pd.DataFrame:
    comp = competencies[["competency_id", "name", "category"]].rename(columns={"name": "competency_name"})
    cols = ["assessment_id", "employee_id", "competency_id", "proficiency_level", "required_level", "assessed_date", "dq_before_hire"]
    df = assessments[cols].merge(comp, on="competency_id", how="left").merge(dim, on="employee_id", how="left")
    df["gap"] = df["required_level"] - df["proficiency_level"]
    df["gap_severity"] = "none"
    df.loc[df["gap"] == 1, "gap_severity"] = "minor"
    df.loc[df["gap"] >= 2, "gap_severity"] = "critical"
    return df.sort_values("assessment_id").reset_index(drop=True)


def latest_review_stats(history: pd.DataFrame) -> pd.DataFrame:
    g = history.groupby("employee_id")
    last = g.tail(1).set_index("employee_id")
    return pd.DataFrame(
        {
            "latest_period": last["review_period"],
            "latest_rating": last["rating"],
            "avg_rating": g["rating"].mean().round(2),
            "rating_change": last["rating_change"],
        }
    ).reset_index()


def gold_promotion_readiness(dim, history, gaps, talent) -> pd.DataFrame:
    crit = gaps[gaps["gap_severity"] == "critical"].groupby("employee_id").size().rename("critical_gaps").reset_index()
    tr = talent[["employee_id", "review_cycle", "potential_rating", "promotion_readiness", "attrition_risk"]]
    df = dim.merge(latest_review_stats(history), on="employee_id", how="left").merge(crit, on="employee_id", how="left").merge(tr, on="employee_id", how="left")
    df["critical_gaps"] = df["critical_gaps"].fillna(0).astype("int64")
    df["is_high_potential"] = df["potential_rating"].eq("High")
    df["is_promotion_ready"] = df["is_high_potential"] & df["promotion_readiness"].eq("Ready Now") & df["latest_rating"].ge(4).fillna(False)
    df["is_promotion_ready"] = df["is_promotion_ready"].astype(bool)
    return df.sort_values("employee_id").reset_index(drop=True)


def gold_training_summary(dim: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    counts = events.pivot_table(index="employee_id", columns="event_type", values="event_id", aggfunc="count", fill_value=0)
    counts = counts.reindex(columns=list(EVENT_TYPES), fill_value=0)
    counts.columns = [f"n_{c}" for c in counts.columns]
    done = events[events["event_type"] == "course_completed"].groupby("employee_id")["course_name"]
    quiz = events.dropna(subset=["score"]).groupby("employee_id")["score"]
    agg = pd.DataFrame(
        {
            "courses_completed": done.nunique(),
            "completed_course_list": done.agg(lambda s: "; ".join(sorted(set(s)))),
            "avg_quiz_score": quiz.mean().astype("float64").round(1),
            "total_events": events.groupby("employee_id").size(),
            "last_activity": events.groupby("employee_id")["event_date"].max(),
        }
    )
    df = dim[["employee_id", "employee_name", "team_id", "team_name", "department", "job_level"]].merge(
        counts.join(agg, how="outer").reset_index(), on="employee_id", how="left"
    )
    for c in [f"n_{t}" for t in EVENT_TYPES] + ["courses_completed", "total_events"]:
        df[c] = df[c].fillna(0).astype("int64")
    df["completed_course_list"] = df["completed_course_list"].fillna("")
    return df.sort_values("employee_id").reset_index(drop=True)


def gold_attrition_risk(promo: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    as_of = events["event_date"].max()
    recent = events[(events["event_date"] > as_of - pd.Timedelta(days=RECENT_DAYS)) & (events["event_date"] <= as_of)]
    recent_n = recent.groupby("employee_id").size().rename("recent_learning_events")
    cols = ["employee_id", "employee_name", "team_id", "team_name", "department", "job_level", "latest_rating", "rating_change", "potential_rating", "attrition_risk"]
    df = promo[cols].merge(recent_n.reset_index(), on="employee_id", how="left")
    df["recent_learning_events"] = df["recent_learning_events"].fillna(0).astype("int64")
    drivers = {
        "hr_flag_high": df["attrition_risk"].eq("High"),
        "hr_flag_medium": df["attrition_risk"].eq("Medium"),
        "declining_rating": df["rating_change"].lt(0).fillna(False),
        "low_rating": df["latest_rating"].le(2).fillna(False),
        "no_recent_learning": df["recent_learning_events"].eq(0),
    }
    for name, flag in drivers.items():
        df[name] = flag.astype(bool)
    df["risk_score"] = (2 * df["hr_flag_high"].astype(int) + df["hr_flag_medium"].astype(int) + df["declining_rating"].astype(int) + df["low_rating"].astype(int) + df["no_recent_learning"].astype(int)).astype("int64")
    df["computed_risk_tier"] = pd.cut(df["risk_score"], bins=[-1, 1, 3, 99], labels=["Low", "Medium", "High"]).astype(str)
    df["risk_drivers"] = df[list(drivers)].apply(lambda r: ",".join(n for n in drivers if r[n]), axis=1)
    df["as_of_date"] = as_of.date().isoformat()
    return df.sort_values("employee_id").reset_index(drop=True)


def gold_team_performance(teams, employees, reviews, projects) -> pd.DataFrame:
    base = teams[["team_id", "name", "department"]].rename(columns={"name": "team_name"})
    headcount = employees.groupby("team_id").size().rename("headcount")
    latest = reviews["review_period"].max()
    r = reviews[reviews["review_period"] == latest].merge(employees[["employee_id", "team_id"]], on="employee_id")
    rg = r.groupby("team_id")["rating"]
    perf = pd.DataFrame(
        {
            "avg_rating": rg.mean(),
            "pct_high_performers": rg.apply(lambda x: (x >= 4).mean() * 100),
            "pct_low_performers": rg.apply(lambda x: (x <= 2).mean() * 100),
        }
    )
    pg = projects.groupby("team_id")
    proj = pd.DataFrame(
        {
            "project_count": pg.size(),
            "avg_outcome_score": pg["outcome_score"].mean(),
            "pct_on_time": pg["outcome_status"].apply(lambda x: (x == "on-time").mean() * 100),
            "n_delayed_or_at_risk": pg["outcome_status"].apply(lambda x: x.isin(["delayed", "at-risk"]).sum()),
        }
    )
    df = base.merge(headcount.reset_index(), on="team_id", how="left").merge(perf.reset_index(), on="team_id", how="left").merge(proj.reset_index(), on="team_id", how="left")
    df["review_period"] = latest
    df["headcount"] = df["headcount"].fillna(0).astype("int64")
    df["project_count"] = df["project_count"].fillna(0).astype("int64")
    df["n_delayed_or_at_risk"] = df["n_delayed_or_at_risk"].fillna(0).astype("int64")
    df["has_projects"] = df["project_count"] > 0
    for c in ("avg_rating", "pct_high_performers", "pct_low_performers", "avg_outcome_score", "pct_on_time"):
        df[c] = df[c].astype("float64").round(2)
    return df.sort_values("team_id").reset_index(drop=True)


def gold_skill_distribution(gaps: pd.DataFrame) -> pd.DataFrame:
    g = gaps.groupby(["department", "category", "competency_name"])
    df = pd.DataFrame(
        {
            "n_assessments": g.size(),
            "employees_assessed": g["employee_id"].nunique(),
            "avg_proficiency": g["proficiency_level"].mean(),
            "avg_required": g["required_level"].mean(),
            "pct_meeting_required": g["gap"].apply(lambda x: (x <= 0).mean() * 100),
            "critical_gap_count": g["gap_severity"].apply(lambda x: (x == "critical").sum()),
        }
    ).reset_index()
    for c in ("avg_proficiency", "avg_required", "pct_meeting_required"):
        df[c] = df[c].round(2)
    df["critical_gap_count"] = df["critical_gap_count"].astype("int64")
    return df


def build_gold(silver: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    dim = employee_dim(silver["employees"], silver["teams"])
    history = gold_employee_review_history(silver["performance_reviews"], dim)
    gaps = gold_skill_gaps(silver["competency_assessments"], silver["competencies"], dim)
    promo = gold_promotion_readiness(dim, history, gaps, silver["talent_review"])
    return {
        "gold_employee_review_history": history,
        "gold_skill_gaps": gaps,
        "gold_promotion_readiness": promo,
        "gold_training_summary": gold_training_summary(dim, silver["learning_activity"]),
        "gold_attrition_risk": gold_attrition_risk(promo, silver["learning_activity"]),
        "gold_team_performance": gold_team_performance(silver["teams"], silver["employees"], silver["performance_reviews"], silver["projects"]),
        "gold_skill_distribution": gold_skill_distribution(gaps),
    }


def gold_checks(lin: Lineage, silver: dict[str, pd.DataFrame], gold: dict[str, pd.DataFrame]) -> bool:
    n_emp = len(silver["employees"])
    t = gold["gold_training_summary"]
    n_type = [f"n_{e}" for e in EVENT_TYPES]
    results = [
        lin.check("gold", "review_history rows == silver reviews", len(silver["performance_reviews"]), len(gold["gold_employee_review_history"])),
        lin.check("gold", "skill_gaps rows == silver assessments", len(silver["competency_assessments"]), len(gold["gold_skill_gaps"])),
        lin.check("gold", "promotion rows == employees", n_emp, len(gold["gold_promotion_readiness"])),
        lin.check("gold", "attrition rows == employees", n_emp, len(gold["gold_attrition_risk"])),
        lin.check("gold", "training rows == employees", n_emp, len(t)),
        lin.check("gold", "training event total == silver events", len(silver["learning_activity"]), int(t[n_type].to_numpy().sum())),
        lin.check("gold", "team rows == silver teams", len(silver["teams"]), len(gold["gold_team_performance"])),
        lin.check("gold", "skill_distribution assessments == silver assessments", len(silver["competency_assessments"]), int(gold["gold_skill_distribution"]["n_assessments"].sum())),
    ]
    return all(results)


def read_silver(lake: Lake) -> dict[str, pd.DataFrame]:
    silver = {t: lake.read_parquet(f"{SILVER}{t}/{t}.parquet") for t in TABLES}
    silver["learning_activity"] = lake.read_parquet_prefix(SILVER_EVENTS)
    return silver


def step_gold(s: Settings, lake: Lake, lin: Lineage) -> dict[str, pd.DataFrame]:
    silver = read_silver(lake)
    t0 = time.perf_counter()
    gold = build_gold(silver)
    build_s = time.perf_counter() - t0
    for name, df in gold.items():
        t1 = time.perf_counter()
        key = f"{GOLD}{name}/{name}.parquet"
        lake.write_parquet(df, key)
        lin.record("gold", name, lake.uri(SILVER), lake.uri(key), len(df), len(df), 0, time.perf_counter() - t1 + build_s / len(gold))
    if not gold_checks(lin, silver, gold):
        raise RuntimeError("gold reconciliation failed")
    # aggregates only; published with the site. NaN (e.g. r over < 2 teams) -> null so browsers can parse it
    lake.write_json(GOLD_SITE_DATA, json.loads(json.dumps(compute_findings(gold), default=str), parse_constant=lambda _: None))
    return gold


def read_gold(lake: Lake) -> dict[str, pd.DataFrame]:
    return {t: lake.read_parquet(f"{GOLD}{t}/{t}.parquet") for t in GOLD_TABLES}


# ---------------------------------------------------------------------------
# Findings (spec section 7): aggregates for the site, the notebook and the acceptance gate
# ---------------------------------------------------------------------------

ON_TIME_BANDS = ("0%", "1-50%", "51-100%")


def on_time_band(pct: float) -> str:
    if pct == 0:
        return "0%"
    return "1-50%" if pct <= 50 else "51-100%"


def weighted(df: pd.DataFrame, by: list[str], col: str, weight: str = "n_assessments") -> pd.Series:
    w = df[col] * df[weight]
    return w.groupby([df[b] for b in by]).sum() / df[weight].groupby([df[b] for b in by]).sum()


def compute_findings(gold: dict[str, pd.DataFrame]) -> dict:
    hist = gold["gold_employee_review_history"]
    gaps = gold["gold_skill_gaps"]
    promo = gold["gold_promotion_readiness"]
    train = gold["gold_training_summary"]
    attr = gold["gold_attrition_risk"]
    teams = gold["gold_team_performance"]
    dist = gold["gold_skill_distribution"]

    change = hist["rating_change"].dropna()
    ratings = {
        "avg_by_period": hist.groupby("review_period")["rating"].mean().round(2).to_dict(),
        "employees_compared": int(len(change)),
        "pct_improved": round((change > 0).mean() * 100, 1),
        "pct_same": round((change == 0).mean() * 100, 1),
        "pct_declined": round((change < 0).mean() * 100, 1),
        "n_improved": int((change > 0).sum()),
        "n_same": int((change == 0).sum()),
        "n_declined": int((change < 0).sum()),
    }

    crit = gaps[gaps["gap_severity"] == "critical"]
    top = crit["competency_name"].value_counts()
    skill_gaps = {
        "assessments": int(len(gaps)),
        "critical": int(len(crit)),
        "pct_critical": round(len(crit) / len(gaps) * 100, 1),
        "employees_with_critical": int(crit["employee_id"].nunique()),
        "severity_counts": gaps["gap_severity"].value_counts().to_dict(),
        "top_competencies": [[k, int(v)] for k, v in top.head(10).items()],
    }

    ready = promo[promo["is_promotion_ready"]]
    promotion = {
        "high_potential": int(promo["is_high_potential"].sum()),
        "promotion_ready": int(len(ready)),
        "ready_with_critical_gap": int((ready["critical_gaps"] >= 1).sum()),
    }

    training = {
        "events": int(train["total_events"].sum()),
        "employees_with_activity": int((train["total_events"] > 0).sum()),
        "employees": int(len(train)),
        "no_completed_course": int((train["courses_completed"] == 0).sum()),
        "avg_courses_per_employee": round(train["courses_completed"].mean(), 2),
    }

    tiers = attr["computed_risk_tier"].value_counts()
    attrition = {
        "computed_tiers": {t: int(tiers.get(t, 0)) for t in ("Low", "Medium", "High")},
        "hr_flag_high": int((attr["attrition_risk"] == "High").sum()),
        "high_risk_and_high_potential": int(((attr["computed_risk_tier"] == "High") & (attr["potential_rating"] == "High")).sum()),
        "as_of_date": str(attr["as_of_date"].iloc[0]),
    }

    tp = teams[teams["has_projects"]].copy()
    tp["band"] = tp["pct_on_time"].map(on_time_band)
    team_perf = {
        "teams_with_projects": int(len(tp)),
        "pearson_r": round(float(tp["avg_rating"].corr(tp["avg_outcome_score"])), 2),
        "avg_rating_by_on_time_band": {b: round(float(tp.loc[tp["band"] == b, "avg_rating"].mean()), 2) for b in ON_TIME_BANDS if (tp["band"] == b).any()},
        "teams_by_on_time_band": {b: int((tp["band"] == b).sum()) for b in ON_TIME_BANDS},
    }

    dept_cat = weighted(dist, ["department", "category"], "avg_proficiency")
    by_comp = weighted(dist, ["competency_name"], "pct_meeting_required").sort_values()
    overall = (dist["pct_meeting_required"] * dist["n_assessments"]).sum() / dist["n_assessments"].sum()
    skills = {
        "strongest": [*dept_cat.idxmax(), round(float(dept_cat.max()), 2)],
        "weakest": [*dept_cat.idxmin(), round(float(dept_cat.min()), 2)],
        "lowest_pct_meeting": [by_comp.index[0], round(float(by_comp.iloc[0]), 1)],
        "overall_pct_meeting": round(float(overall), 1),
    }

    return {
        "ratings": ratings,
        "skill_gaps": skill_gaps,
        "promotion": promotion,
        "training": training,
        "attrition": attrition,
        "teams": team_perf,
        "skills": skills,
    }


def run_summary(lineage: dict) -> dict:
    steps = {(s["layer"], s["dataset"]): s for s in lineage.get("steps", [])}
    ev = steps.get(("silver", "learning_activity"), {})
    rec = lineage.get("reconciliation", [])
    return {
        "run_id": lineage.get("run_id"),
        "status": lineage.get("status"),
        "reconciliation": f"{sum(c['passed'] for c in rec)}/{len(rec)}",
        "events_in": ev.get("rows_in"),
        "events_kept": ev.get("rows_out"),
        "events_quarantined": ev.get("rows_rejected"),
        "duration_s": lineage.get("duration_s"),
        "environment": lineage.get("execution", {}).get("environment"),
    }


# ---------------------------------------------------------------------------
# Layers: markers, dependency checks, lineage chain
# ---------------------------------------------------------------------------

STEP_FUNCS: dict[str, Callable[[Settings, Lake, Lineage], Any]] = {
    "extract": step_extract,
    "bronze": step_bronze,
    "silver": step_silver,
    "gold": step_gold,
}


class DependencyError(RuntimeError):
    """A layer's upstream output is missing."""


def marker_key(layer: str) -> str:
    return f"{layer}/_SUCCESS"


def read_marker(lake: Lake, layer: str) -> dict | None:
    key = marker_key(layer)
    return lake.read_json(key) if lake.exists(key) else None


def write_marker(s: Settings, lake: Lake, layer: str, lin: Lineage) -> dict:
    marker = {
        "layer": layer,
        "run_id": s.run_id,
        "run_date": s.run_date,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "row_counts": {e["dataset"]: e["rows_out"] for e in lin.steps if e["layer"] == layer},
        "data_checksum": lin.data_checksum,
        "upstream": lin.upstream,
    }
    lake.write_json(marker_key(layer), marker)
    return marker


def check_upstream(s: Settings, lake: Lake, layer: str, lin: Lineage) -> dict | None:
    """Fail fast when the upstream layer has no _SUCCESS marker; return the upstream link for lineage."""
    req = REQUIRES.get(layer)
    if req is None:
        return None
    m = read_marker(lake, req)
    if m is None:
        raise DependencyError(
            f"{layer} layer needs {req} output, but {lake.uri(marker_key(req))} was not found. "
            f"Run the {req} layer first: python job.py --layer {req}"
        )
    upstream = {"layer": req, "run_id": m["run_id"], "completed_at": m["completed_at"], "data_checksum": m.get("data_checksum")}
    if layer == "gold":
        used = m.get("upstream") or {}
        upstream["bronze_run_id"] = used.get("run_id")
        bronze = read_marker(lake, "bronze")
        if bronze and used.get("data_checksum") != bronze.get("data_checksum"):
            msg = (
                f"silver run {m['run_id']} was built from bronze run {used.get('run_id')}, but bronze has changed since "
                f"(run {bronze['run_id']}, completed {bronze['completed_at']}); gold is using older silver data. "
                "Rerun the silver layer to pick up the new bronze."
            )
            log.warning(msg)
            lin.warnings.append(msg)
    return upstream


def layer_groups(steps: Iterable[str]) -> list[tuple[str, tuple[str, ...]]]:
    """Group steps into layer runs, in pipeline order: [('bronze', ('extract', 'bronze')), ...]."""
    groups: dict[str, list[str]] = {}
    for step in sorted(steps, key=ALL_STEPS.index):
        groups.setdefault(LAYER_OF[step], []).append(step)
    return [(layer, tuple(st)) for layer, st in groups.items()]


def run_layer(settings: Settings, lake: Lake, layer: str, steps: tuple[str, ...]) -> dict:
    """One layer run: own run_id + lineage; dependency check; marker only when the layer completes."""
    s = replace(settings, run_id=f"{settings.run_id}-{layer}")
    lin = Lineage(s, layer)
    completes = LAYERS[layer][-1] in steps  # e.g. extract alone does not complete bronze
    status, error = "FAILED", None
    log.info("layer %s run %s steps=%s", layer, s.run_id, ",".join(steps))
    try:
        if completes:
            lin.upstream = check_upstream(s, lake, layer, lin)
            lin.data_checksum = (lin.upstream or {}).get("data_checksum")  # derived data inherits its input's identity
            lake.delete_prefix(marker_key(layer))  # a failed rerun must not leave the old marker behind
        for step in steps:
            STEP_FUNCS[step](s, lake, lin)
        if completes:
            write_marker(s, lake, layer, lin)
        status = "SUCCEEDED"
    except DependencyError as exc:
        status, error = REFUSED, repr(exc)
        log.error("%s", exc)
    except Exception as exc:
        error = repr(exc)
        log.exception("layer %s failed", layer)
    rep = lin.write(lake, status, error)
    passed = sum(c["passed"] for c in rep["reconciliation"])
    log.info("layer %s run %s %s checks %d/%d in %.1fs", layer, s.run_id, status, passed, len(rep["reconciliation"]), rep["duration_s"])
    return rep


def pipeline_view(lake: Lake) -> dict:
    """Stitch the latest run of each layer into one report (lineage/latest.json, read by the site).

    status: FAILED if any layer's latest run failed; INCOMPLETE if a layer has no completed output;
    STALE if a layer was built from different upstream data (compared by checksum, so re-running
    a layer on unchanged input changes nothing); else SUCCEEDED. Refused runs don't count: they
    changed nothing and are kept only in lineage/<layer>/runs/.
    """
    runs = {layer: lake.read_json(f"lineage/{layer}/latest.json") for layer in LAYERS if lake.exists(f"lineage/{layer}/latest.json")}
    markers = {layer: read_marker(lake, layer) for layer in LAYERS}
    warnings = [w for r in runs.values() for w in r.get("warnings", [])]
    problems = []
    for layer, req in REQUIRES.items():
        m, up = markers[layer], markers[req]
        used = (m or {}).get("upstream") or {}
        if m and up and used.get("data_checksum") != up.get("data_checksum"):
            problems.append(f"{layer} run {m['run_id']} was built from {req} run {used.get('run_id')}, but {req} data has changed (run {up['run_id']}); rerun {layer}")

    failed = [(layer, r) for layer, r in runs.items() if r["status"] != "SUCCEEDED"]
    missing = [layer for layer in LAYERS if not markers[layer]]
    if failed:
        status, error = "FAILED", "; ".join(f"{layer}: {r.get('error')}" for layer, r in failed)
    elif missing:
        status, error = "INCOMPLETE", f"no completed run yet for: {', '.join(missing)}"
    elif problems:
        status, error = "STALE", "; ".join(problems)
    else:
        status, error = "SUCCEEDED", None

    ordered = [runs[layer] for layer in LAYERS if layer in runs]
    newest = max(ordered, key=lambda r: r["ended_at"]) if ordered else {}
    head = runs.get("gold") or newest
    return {
        "run_id": head.get("run_id"),
        "run_date": head.get("run_date"),
        "status": status,
        "error": error,
        "warnings": warnings,
        "started_at": min((r["started_at"] for r in ordered), default=None),
        "ended_at": newest.get("ended_at"),
        "duration_s": round(sum(r["duration_s"] for r in ordered), 2),
        "bucket": head.get("bucket"),
        "execution": newest.get("execution", {}),
        "layers": {
            layer: {k: r.get(k) for k in ("run_id", "status", "started_at", "ended_at", "duration_s", "upstream", "error")}
            for layer, r in runs.items()
        },
        "steps": [e for r in ordered for e in r["steps"]],
        "reconciliation": [c for r in ordered for c in r["reconciliation"]],
    }


# ---------------------------------------------------------------------------
# Lake state export / import (moves the lake between CI jobs)
# ---------------------------------------------------------------------------


TRANSFER_WORKERS = 16  # boto3 clients are thread-safe; small objects -> latency bound


def export_lake(lake: Lake, out_dir: str | Path) -> int:
    """Copy every object to out_dir/<key>. Returns the object count."""
    root = Path(out_dir)
    keys = lake.list("")

    def save(key: str) -> None:
        path = root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(lake.get(key))

    with ThreadPoolExecutor(max_workers=TRANSFER_WORKERS) as pool:
        list(pool.map(save, keys))
    log.info("exported %d objects from %s to %s", len(keys), lake.uri(""), root)
    return len(keys)


def import_lake(lake: Lake, in_dir: str | Path) -> int:
    """Upload every file under in_dir as <relative path>; objects already there with the same size are skipped."""
    root = Path(in_dir)
    if not root.is_dir():
        raise FileNotFoundError(f"lake state directory not found: {root}")
    files = sorted(p for p in root.rglob("*") if p.is_file())

    def upload(path: Path) -> None:
        key = path.relative_to(root).as_posix()
        data = path.read_bytes()
        if lake.size(key) != len(data):
            lake.put(key, data)

    with ThreadPoolExecutor(max_workers=TRANSFER_WORKERS) as pool:
        list(pool.map(upload, files))
    log.info("imported %d objects from %s into %s", len(files), root, lake.uri(""))
    return len(files)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="ACME Talent ETL. Settings come from env vars; see docs/design-spec.md.")
    ap.add_argument("--layer", choices=LAYER_CHOICES, help="run one layer (bronze = extract + bronze) or all; overrides STEPS")
    ap.add_argument("--export-lake", metavar="DIR", help="copy the whole lake to DIR and exit")
    ap.add_argument("--import-lake", metavar="DIR", help="upload DIR into the lake and exit")
    return ap.parse_args(argv)


def main(env: dict[str, str] | None = None, lake: Lake | None = None, argv: list[str] | None = None) -> int:
    args = parse_args(argv or [])
    env = dict(os.environ if env is None else env)
    if args.layer:
        env["LAYER"] = args.layer
    try:
        settings = Settings.from_env(env)
    except ConfigError as exc:
        setup_logging()
        log.error("config error: %s", exc)
        return 1
    setup_logging(settings.log_level)

    if args.export_lake or args.import_lake:
        lake = lake or Lake.from_settings(settings)
        lake.ensure_bucket()
        if args.import_lake:
            import_lake(lake, args.import_lake)
        if args.export_lake:
            export_lake(lake, args.export_lake)
        return 0

    log.info("run %s steps=%s bucket=%s env=%s", settings.run_id, ",".join(settings.steps), settings.lake_bucket, settings.execution_env)
    ok = False
    try:
        lake = lake or Lake.from_settings(settings)
        lake.ensure_bucket()
        ok = True
        for layer, steps in layer_groups(settings.steps):
            if run_layer(settings, lake, layer, steps)["status"] != "SUCCEEDED":
                ok = False
                break
        view = pipeline_view(lake)
        lake.write_json("lineage/latest.json", view)
        passed = sum(c["passed"] for c in view["reconciliation"])
        log.info("pipeline %s checks %d/%d%s", view["status"], passed, len(view["reconciliation"]), f" ({view['error']})" if view["error"] else "")
    except Exception:
        log.exception("run failed")
        ok = False
    return 0 if ok else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(argv=sys.argv[1:]))
