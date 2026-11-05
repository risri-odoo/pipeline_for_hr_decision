"""Integration tests against the docker-compose Postgres + MinIO.

Run via scripts/run_tests.sh (starts the services and exports INTEGRATION=1 plus
connection settings). Skipped otherwise.
"""

from __future__ import annotations

import os
import uuid

import pandas as pd
import psycopg2
import pytest

import job
from conftest import DATA, MANIFEST, ROOT, assert_findings_match

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("INTEGRATION") != "1", reason="set INTEGRATION=1 (scripts/run_tests.sh) to run"),
]


@pytest.fixture
def env() -> dict[str, str]:
    keys = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_NAME", "POSTGRES_USER", "POSTGRES_PASS", "S3_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"]
    return {k: os.environ[k] for k in keys if k in os.environ}


@pytest.fixture
def bucket(env):
    """A throwaway bucket per test, emptied and removed afterwards."""
    name = f"it-{uuid.uuid4().hex[:12]}"
    lake = job.Lake(name, env["S3_ENDPOINT"], env.get("AWS_REGION", "us-east-1"))
    lake.ensure_bucket()
    yield name
    lake.delete_prefix("")
    lake.s3.delete_bucket(Bucket=name)


def test_copy_export_reads_rows(env):
    s = job.Settings.from_env({**env, "LAKE_BUCKET": "unused"})
    raw = job.pg_export_csv(s, "employees")
    assert raw.startswith(b"employee_id,first_name,last_name,email,team_id,job_level,hire_date\n")
    assert job.count_csv_rows(raw) == MANIFEST["rows"]["employees"]
    assert job.count_csv_rows(job.pg_export_csv(s, "performance_reviews")) == MANIFEST["rows"]["performance_reviews"]


def test_copy_export_retries_then_fails(env, no_sleep):
    s = job.Settings.from_env({**env, "LAKE_BUCKET": "unused", "POSTGRES_PORT": "1"})
    with pytest.raises(psycopg2.OperationalError):
        job.pg_export_csv(s, "teams")
    assert no_sleep == [1.0, 2.0]


def test_parquet_roundtrip_on_minio(env, bucket):
    lake = job.Lake(bucket, env["S3_ENDPOINT"])
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "when": pd.to_datetime(["2024-01-02T03:04:05", "2025-06-07T08:09:10"]),
            "score": pd.array([7, None], dtype="Int64"),
            "flag": [True, False],
            "name": ["a", None],
        }
    )
    lake.write_parquet(df, "t/x.parquet")
    pd.testing.assert_frame_equal(lake.read_parquet("t/x.parquet"), df)
    assert lake.size("t/x.parquet") > 0 and lake.size("t/none.parquet") is None
    lake.copy("t/x.parquet", "t/y.parquet")
    assert lake.list("t/") == ["t/x.parquet", "t/y.parquet"]
    assert len(lake.read_parquet_prefix("t/")) == 4
    assert lake.delete_prefix("t/") == 2 and lake.list("t/") == []


def test_end_to_end_twice_is_idempotent(env, bucket):
    run_env = {**env, "LAKE_BUCKET": bucket, "RUN_DATE": "2026-10-15", "EVENTS_SOURCE_DIR": str(DATA / "learning_activity"), "EXECUTION_ENV": "pytest-integration"}
    lake = job.Lake(bucket, env["S3_ENDPOINT"])
    results = []
    for _ in range(2):
        assert job.main(run_env) == 0
        lin = lake.read_json("lineage/latest.json")
        silver_ev = next(s for s in lin["steps"] if (s["layer"], s["dataset"]) == ("silver", "learning_activity"))
        results.append(
            {
                "status": lin["status"],
                "passed": sum(c["passed"] for c in lin["reconciliation"]),
                "checks": len(lin["reconciliation"]),
                "events": (silver_ev["rows_in"], silver_ev["rows_out"], silver_ev["rows_rejected"]),
                "gold": {t: len(df) for t, df in job.read_gold(lake).items()},
                "notes": {(s["layer"], s["dataset"]): s["note"] for s in lin["steps"]},
            }
        )
    first, second = results
    assert first["status"] == second["status"] == "SUCCEEDED"
    assert first["passed"] == first["checks"] == second["passed"] == 9
    events = MANIFEST["events"]
    assert first["events"] == second["events"] == (events["lines"], events["kept"], events["quarantined"])
    assert first["gold"] == second["gold"]
    assert first["notes"][("landing", "teams")] == "exported"
    assert second["notes"][("landing", "teams")] == "already extracted today, skipped"
    assert second["notes"][("bronze", "teams")] == "already in bronze, skipped"
    assert len(lake.list("lineage/gold/runs/")) == 2
    rej = lake.read_parquet("silver/_quarantine/learning_activity/run_date=2026-10-15/rejects.parquet")
    assert rej["error"].map(job.reject_reason).value_counts().to_dict() == MANIFEST["quarantine_by_reason"]
    assert_findings_match(job.compute_findings(job.read_gold(lake)), MANIFEST["expected"])
    assert lake.read_json("lineage/latest.json")["status"] == "SUCCEEDED"


def test_notebook_executes_against_minio(env):
    """Runs notebooks/insights.ipynb against the compose lake (filled by scripts/run_compose.sh)."""
    nbformat = pytest.importorskip("nbformat")
    from nbclient import NotebookClient

    bucket = os.environ.get("LAKE_BUCKET", "")
    lake = job.Lake(bucket, env["S3_ENDPOINT"]) if bucket else None
    if lake is None or not lake.exists("lineage/latest.json"):
        pytest.skip("no pipeline run in the compose lake; run scripts/run_compose.sh first")
    root = ROOT
    nb = nbformat.read(root / "notebooks" / "insights.ipynb", as_version=4)
    NotebookClient(nb, timeout=600, kernel_name="python3", resources={"metadata": {"path": str(root / "notebooks")}}).execute()
    outputs = [o for c in nb.cells if c.cell_type == "code" for o in c.get("outputs", [])]
    assert not [o for o in outputs if o.get("output_type") == "error"]
    assert sum("image/svg+xml" in o.get("data", {}) for o in outputs) == 12
    text = "".join(o.get("text", "") for o in outputs)
    assert "SUCCEEDED" in text and "9/9" in text and "example: lowest employee_id" in text
