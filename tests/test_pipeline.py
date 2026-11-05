import io
import logging

import pytest
from botocore.exceptions import ClientError

import job
from conftest import addr, make_settings
from fakes import MemoryLake

TEAMS = b"team_id,name,department\n1,A,Eng\n2,B,Ops\n"


def fake_exporter(tables=None, calls=None):
    tables = tables or {}

    def export(settings, table):
        if calls is not None:
            calls.append(table)
        return tables.get(table, b"id\n1\n")

    return export


# ---------------------------------------------------------------------------
# retry
# ---------------------------------------------------------------------------


def test_retry_recovers(no_sleep, caplog):
    attempts = []

    @job.retry(times=3, base_delay=1.0)
    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionError("blip")
        return "ok"

    with caplog.at_level(logging.WARNING):
        assert flaky() == "ok"
    assert len(attempts) == 3 and no_sleep == [1.0, 2.0]
    assert sum("retrying" in r.message for r in caplog.records) == 2


def test_retry_gives_up(no_sleep, caplog):
    @job.retry(times=3, base_delay=0.5)
    def broken():
        raise TimeoutError("down")

    with caplog.at_level(logging.ERROR), pytest.raises(TimeoutError):
        broken()
    assert no_sleep == [0.5, 1.0]
    assert any("failed after 3 attempts" in r.message for r in caplog.records)


def test_retry_does_not_retry_unlisted(no_sleep):
    calls = []

    @job.retry(times=3, exceptions=(ConnectionError,))
    def wrong():
        calls.append(1)
        raise ValueError("not transient")

    with pytest.raises(ValueError):
        wrong()
    assert calls == [1] and no_sleep == []


# ---------------------------------------------------------------------------
# lineage
# ---------------------------------------------------------------------------


def test_lineage_record_and_report(settings, lake, caplog):
    lin = job.Lineage(settings)
    e = lin.record("silver", "teams", "src", "dst", 120, 100, 20, 2.0, "n")
    assert e["rows_per_min"] == 3600.0 and e["rows_rejected"] == 20
    with caplog.at_level(logging.WARNING):
        z = lin.record("gold", "empty", "s", "t", 0, 0, 0, 0)
    assert z["rows_per_min"] is None and any("produced 0 rows" in r.message for r in caplog.records)
    assert lin.check("gold", "x", 1, 1) and not lin.check("gold", "y", 1, 2)

    rep = lin.write(lake, "FAILED", "boom")
    assert rep["status"] == "FAILED" and rep["error"] == "boom" and rep["run_id"] == settings.run_id
    assert rep["execution"]["environment"] == "local" and rep["execution"]["engine"] == "pandas"
    assert [c["passed"] for c in rep["reconciliation"]] == [True, False]
    assert lake.read_json(f"lineage/runs/{settings.run_id}.json") == lake.read_json("lineage/latest.json")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_config_error_returns_1_without_lineage(lake):
    assert job.main({}, lake) == 1
    assert lake.objects == {}


def test_main_failure_writes_failed_lineage(lake):
    # bronze with an empty lake: no landing snapshot -> RuntimeError
    rc = job.main({"LAKE_BUCKET": "b", "STEPS": "bronze", "RUN_ID": "r1"}, lake)
    assert rc == 1
    rep = lake.read_json("lineage/bronze/runs/r1-bronze.json")
    assert rep["status"] == "FAILED" and "no snapshot" in rep["error"] and rep["layer"] == "bronze"
    view = lake.read_json("lineage/latest.json")
    assert view["status"] == "FAILED" and view["error"].startswith("bronze: ") and view["run_id"] == "r1-bronze"


def test_main_lineage_write_failure_is_failed(lake, monkeypatch):
    monkeypatch.setattr(job.Lineage, "write", lambda *a, **k: (_ for _ in ()).throw(IOError("disk")))
    assert job.main({"LAKE_BUCKET": "b", "STEPS": "gold"}, lake) == 1


def test_main_builds_lake_from_settings(monkeypatch):
    built = MemoryLake()
    monkeypatch.setattr(job.Lake, "from_settings", classmethod(lambda cls, s: built))
    assert job.main({"LAKE_BUCKET": "b", "STEPS": "bronze"}) == 1
    assert built.exists("lineage/latest.json")


# ---------------------------------------------------------------------------
# extract / bronze with the in-memory lake
# ---------------------------------------------------------------------------


def test_extract_writes_landing_once(settings, lake):
    calls = []
    export = fake_exporter({"teams": TEAMS}, calls)
    job.step_extract(settings, lake, job.Lineage(settings), export)
    key = "landing/postgres/teams/ingest_date=2026-10-15/teams.csv"
    assert lake.get(key) == TEAMS and len(calls) == 7

    lake.objects[key] = TEAMS + b"3,C,Ops\n"  # pretend landing was already there
    lin = job.Lineage(settings)
    job.step_extract(settings, lake, lin, export)
    assert len(calls) == 7  # exporter not called again
    assert lake.get(key).endswith(b"3,C,Ops\n")  # landing is immutable
    teams = next(s for s in lin.steps if s["dataset"] == "teams")
    assert teams["note"] == "already extracted today, skipped" and teams["rows_out"] == 3


def test_extract_warns_on_empty_table(settings, lake, caplog):
    with caplog.at_level(logging.WARNING):
        job.step_extract(settings, lake, job.Lineage(settings), fake_exporter({t: b"id\n" for t in job.TABLES}))
    assert sum("is empty" in r.message for r in caplog.records) == 7


def test_extract_verifies_size(settings, lake, monkeypatch):
    monkeypatch.setattr(lake, "put", lambda key, data: lake.objects.__setitem__(key, data[:-1]))
    with pytest.raises(IOError, match="size mismatch"):
        job.step_extract(settings, lake, job.Lineage(settings), fake_exporter())


def test_upload_events_once(tmp_path, lake):
    day = tmp_path / "year=2024" / "month=01" / "day=02"
    day.mkdir(parents=True)
    (day / "events-000.jsonl").write_bytes(b'{"a": 1}\n{"a": 2}\n')
    (day / "events-001.jsonl").write_bytes(b'{"a": 3}')  # no trailing newline
    s = make_settings(events_source_dir=str(tmp_path))
    lin = job.Lineage(s)
    job.upload_events(s, lake, lin)
    assert lake.exists("landing/learning_activity/year=2024/month=01/day=02/events-000.jsonl")
    assert lin.steps[-1]["rows_in"] == 3 and "2 uploaded" in lin.steps[-1]["note"]
    job.upload_events(s, lake, lin)
    assert "0 uploaded, 2 already present" in lin.steps[-1]["note"]


def test_upload_events_requires_files(tmp_path, lake):
    s = make_settings(events_source_dir=str(tmp_path))
    with pytest.raises(RuntimeError, match="no .jsonl"):
        job.upload_events(s, lake, job.Lineage(s))


def seed_landing(lake, day="2026-10-15", tables=None):
    for t in job.TABLES:
        lake.objects[job.pg_key("landing/postgres/", t, day)] = (tables or {}).get(t, b"id\n1\n2\n")
    lake.objects["landing/learning_activity/year=2024/month=01/day=02/events-000.jsonl"] = b"{}\n"


def test_bronze_copies_without_postgres(settings, lake, monkeypatch):
    monkeypatch.setattr(job, "pg_export_csv", lambda *a: pytest.fail("bronze must not touch Postgres"))
    seed_landing(lake)
    lin = job.Lineage(settings)
    job.step_bronze(settings, lake, lin)
    assert lake.get("bronze/postgres/teams/ingest_date=2026-10-15/teams.csv") == b"id\n1\n2\n"
    assert lake.exists("bronze/s3/learning_activity/year=2024/month=01/day=02/events-000.jsonl")
    assert lin.reconciliation[0] == {"layer": "bronze", "check": "bronze rows == landing rows (postgres)", "expected": 14, "actual": 14, "passed": True}


def test_bronze_never_overwrites(settings, lake):
    seed_landing(lake)
    job.step_bronze(settings, lake, job.Lineage(settings))
    lake.calls.clear()
    lin = job.Lineage(settings)
    job.step_bronze(settings, lake, lin)
    assert not [c for c in lake.calls if c[0] in ("put", "copy")]
    assert all(s["note"].startswith(("already in bronze", "0 copied")) for s in lin.steps)


def test_bronze_fails_on_row_mismatch(settings, lake):
    seed_landing(lake)
    lake.objects["bronze/postgres/teams/ingest_date=2026-10-15/teams.csv"] = b"id\n1\n"  # stale, fewer rows
    with pytest.raises(RuntimeError, match="bronze reconciliation failed"):
        job.step_bronze(settings, lake, job.Lineage(settings))


def test_bronze_missing_snapshot_raises(settings, lake):
    with pytest.raises(RuntimeError, match="no snapshot"):
        job.step_bronze(settings, lake, job.Lineage(settings))


def test_snapshot_date_prefers_run_date_else_newest(lake):
    for d in ("2026-10-01", "2026-10-09"):
        lake.objects[f"landing/postgres/teams/ingest_date={d}/teams.csv"] = b"x"
    assert job.snapshot_date(lake, "landing/postgres/teams/", "2026-10-01") == "2026-10-01"
    assert job.snapshot_date(lake, "landing/postgres/teams/", "2026-10-15") == "2026-10-09"


def test_bronze_uses_newest_snapshot_when_run_date_missing(lake):
    seed_landing(lake, day="2026-10-01")
    s = make_settings(run_date="2026-10-15")
    job.step_bronze(s, lake, job.Lineage(s))
    assert lake.exists("bronze/postgres/teams/ingest_date=2026-10-01/teams.csv")


# ---------------------------------------------------------------------------
# silver step + quarantine on a tiny dataset
# ---------------------------------------------------------------------------

MINI = {
    "teams": TEAMS,
    "competencies": b"competency_id,name,category\n1,SQL,Technical\n",
    "employees": f"employee_id,first_name,last_name,email,team_id,job_level,hire_date\n1,A,B,{addr('a')},1,IC1,2021-01-01\n2,C,D,{addr('c')},9,IC1,2021-01-01\n".encode(),
    "projects": b"project_id,name,team_id,outcome_status,outcome_score\n1,P,1,on-time,50\n",
    "performance_reviews": b"review_id,employee_id,review_period,rating,manager_comment,submitted_date\n1,1,2025-H1,4,,2020-06-01\n2,2,2025-H1,3,ok,2025-07-01\n",
    "competency_assessments": b"assessment_id,employee_id,competency_id,proficiency_level,required_level,assessed_date\n1,1,1,2,4,2025-01-01\n",
    "talent_review": b"employee_id,review_cycle,potential_rating,promotion_readiness,attrition_risk\n1,2025-H1,High,Ready Now,Low\n",
}


def test_silver_step_quarantines_and_replaces_partition(settings, lake):
    for t, data in MINI.items():
        lake.objects[job.pg_key("bronze/postgres/", t, "2026-10-15")] = data
    k = "bronze/s3/learning_activity/year=2024/month=01/day=02/events-000.jsonl"
    lake.objects[k] = (
        b'{"event_id":"EV0000000001","employee_id":"1","event_date":"2024-01-02T10:00:00","event_type":"enrollment","course_name":"SQL"}\n'
        b"\n"
        b"not json\n"
    )
    lake.objects["silver/learning_activity/year=1999/month=01/part.parquet"] = b"stale"
    lin = job.Lineage(settings)
    job.step_silver(settings, lake, lin)

    emp = lake.read_parquet("silver/employees/employees.parquet")
    assert emp["employee_id"].tolist() == [1] and {"_source", "_run_id", "_processed_at"} <= set(emp.columns)
    q = lake.read_parquet("silver/_quarantine/employees/run_date=2026-10-15/rejects.parquet")
    assert q["error"].tolist() == ["orphan team_id"]
    rq = lake.read_parquet("silver/_quarantine/performance_reviews/run_date=2026-10-15/rejects.parquet")
    assert rq["error"].tolist() == ["orphan employee_id"]  # cascades from the employee orphan
    rev = lake.read_parquet("silver/performance_reviews/performance_reviews.parquet")
    assert rev["dq_before_hire"].tolist() == [True]
    assert not lake.exists("silver/learning_activity/year=1999/month=01/part.parquet")
    assert lake.list("silver/learning_activity/") == ["silver/learning_activity/year=2024/month=01/part.parquet"]
    ev = next(s for s in lin.steps if s["dataset"] == "learning_activity")
    assert (ev["rows_in"], ev["rows_out"], ev["rows_rejected"]) == (2, 1, 1)

    # a rerun with clean data clears the previous run_date's rejects
    lake.objects[job.pg_key("bronze/postgres/", "employees", "2026-10-15")] = MINI["employees"].rsplit(b"2,C", 1)[0]
    job.step_silver(settings, lake, job.Lineage(settings))
    assert not lake.list("silver/_quarantine/employees/")


# ---------------------------------------------------------------------------
# Lake wrapper against a fake boto3 client
# ---------------------------------------------------------------------------


def client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "Op")


class FakeS3:
    def __init__(self):
        self.objects, self.buckets, self.deleted, self.created = {}, set(), [], None

    def head_bucket(self, Bucket):
        if Bucket not in self.buckets:
            raise client_error("404")

    def create_bucket(self, Bucket, **kw):
        self.buckets.add(Bucket)
        self.created = kw

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise client_error("NoSuchKey")
        return {"Body": io.BytesIO(self.objects[Key])}

    def copy_object(self, Bucket, Key, CopySource):
        self.objects[Key] = self.objects[CopySource["Key"]]

    def head_object(self, Bucket, Key):
        if Key == "forbidden":
            raise client_error("403")
        if Key not in self.objects:
            raise client_error("404")
        return {"ContentLength": len(self.objects[Key])}

    def get_paginator(self, name):
        return self

    def paginate(self, Bucket, Prefix):
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        for i in range(0, max(len(keys), 1), 2):
            page = keys[i : i + 2]
            yield {"Contents": [{"Key": k, "ETag": f'"tag-{k}"'} for k in page]} if page else {}

    def delete_objects(self, Bucket, Delete):
        self.deleted.append(len(Delete["Objects"]))
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)


def test_lake_wrapper_roundtrip():
    s3 = FakeS3()
    lake = job.Lake("b", client=s3, region="eu-west-1")
    lake.ensure_bucket()
    assert s3.created == {"CreateBucketConfiguration": {"LocationConstraint": "eu-west-1"}}
    lake.ensure_bucket()  # exists -> no error

    lake.put("a/1.txt", b"one")
    lake.copy("a/1.txt", "a/2.txt")
    lake.put("b/3.txt", b"x")
    assert lake.get("a/2.txt") == b"one" and lake.size("a/1.txt") == 3
    assert lake.size("missing") is None and not lake.exists("missing")
    with pytest.raises(ClientError):
        lake.size("forbidden")
    assert lake.list("a/") == ["a/1.txt", "a/2.txt"] and lake.list("zzz") == []
    assert lake.fingerprints("b/") == {"b/3.txt": "tag-b/3.txt"}
    assert lake.uri("k") == "s3://b/k"

    import pandas as pd

    df = pd.DataFrame({"x": [1, 2], "y": ["a", None]})
    lake.write_parquet(df, "p/1.parquet")
    lake.write_parquet(df, "p/2.parquet")
    lake.put("p/readme.txt", b"skip me")
    pd.testing.assert_frame_equal(lake.read_parquet("p/1.parquet"), df)
    assert len(lake.read_parquet_prefix("p/")) == 4
    with pytest.raises(FileNotFoundError):
        lake.read_parquet_prefix("nothing/")

    lake.write_json("j.json", {"d": 1})
    assert lake.read_json("j.json") == {"d": 1}
    assert lake.delete_prefix("a/") == 2 and lake.list("a/") == []


def test_lake_delete_prefix_batches_of_1000():
    s3 = FakeS3()
    s3.objects = {f"k/{i:04d}": b"" for i in range(2500)}
    lake = job.Lake("b", client=s3)
    lake.list = lambda prefix: sorted(s3.objects)  # skip the 2-per-page fake paginator
    assert lake.delete_prefix("k/") == 2500
    assert s3.deleted == [1000, 1000, 500]


def test_lake_us_east_1_create_has_no_constraint():
    s3 = FakeS3()
    job.Lake("b", client=s3).ensure_bucket()
    assert s3.created == {}


def test_lake_get_retries_then_raises(no_sleep):
    lake = job.Lake("b", client=FakeS3())
    with pytest.raises(ClientError):
        lake.get("missing")
    assert no_sleep == [1.0, 2.0]


def test_lake_builds_path_style_boto3_client():
    lake = job.Lake.from_settings(make_settings(s3_endpoint="http://minio:9000"))
    cfg = lake.s3.meta.config
    assert lake.s3.meta.endpoint_url == "http://minio:9000"
    # botocore normalises max_attempts=5 (retries) to total_max_attempts=6 (calls)
    assert cfg.s3["addressing_style"] == "path" and cfg.retries == {"total_max_attempts": 6, "mode": "standard"}


def test_verify_size(lake):
    lake.put("k", b"abc")
    job.verify_size(lake, "k", 3)
    with pytest.raises(IOError):
        job.verify_size(lake, "k", 4)


def test_pg_export_rejects_unknown_table(settings):
    with pytest.raises(ValueError, match="not allowed"):
        job.pg_export_csv(settings, "pg_shadow; --")
