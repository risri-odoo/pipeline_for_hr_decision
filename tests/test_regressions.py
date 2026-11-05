"""One regression test per fixed bug."""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

import job
from conftest import ROOT, run_sample
from fakes import MemoryLake


# Bug 1: the default Postgres host port clashed with other local projects on 5433


def test_default_postgres_host_port_is_5544_and_configurable():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert '"127.0.0.1:${PG_HOST_PORT:-5544}:5432"' in compose
    assert "PG_HOST_PORT=5544" in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
    assert "${PG_HOST_PORT:-5544}" in (ROOT / "scripts" / "_env.sh").read_text(encoding="utf-8")


# Bug 2: run_tests.sh let .env overwrite settings already in the shell


def find_bash() -> str | None:
    git_bash = Path(r"C:\Program Files\Git\bin\bash.exe")
    return str(git_bash) if git_bash.exists() else shutil.which("bash")


@pytest.mark.skipif(find_bash() is None, reason="needs bash")
def test_shell_env_wins_over_dotenv(tmp_path):
    (tmp_path / ".env").write_text("# comment\nPG_HOST_PORT=5433\r\nMINIO_ROOT_USER=from-dotenv\nS3_ENDPOINT=http://from-dotenv\n\nFROM_FILE=a=b\n", encoding="utf-8", newline="")
    helpers = (ROOT / "scripts" / "_env.sh").read_text(encoding="utf-8")
    (tmp_path / "env.sh").write_text(helpers.replace("\r\n", "\n"), encoding="utf-8", newline="\n")
    script = (
        "unset MINIO_ROOT_USER POSTGRES_PORT POSTGRES_HOST AWS_ACCESS_KEY_ID FROM_FILE; "
        "export PG_HOST_PORT=6000 S3_ENDPOINT=http://from-shell; "
        ". ./env.sh; load_dotenv .env; export_test_connection; "
        'echo "$PG_HOST_PORT|$POSTGRES_PORT|$S3_ENDPOINT|$MINIO_ROOT_USER|$AWS_ACCESS_KEY_ID|$FROM_FILE"'
    )
    out = subprocess.run([find_bash(), "-c", script], cwd=tmp_path, capture_output=True, text=True, check=True).stdout.strip()
    assert out == "6000|6000|http://from-shell|from-dotenv|from-dotenv|a=b"


def test_run_tests_uses_the_safe_loader():
    script = (ROOT / "scripts" / "run_tests.sh").read_text(encoding="utf-8")
    assert "load_dotenv .env" in script and ". ./.env" not in script


# Bug 3: a refused run stuck in the overall status, so a good bronze run still showed FAILED


def test_refused_run_does_not_stick_in_the_overall_status(small_data):
    lake = MemoryLake()
    assert run_sample(lake, data=small_data, LAYER="gold") == 1  # refused: no silver yet
    assert run_sample(lake, data=small_data, LAYER="bronze") == 0
    view = lake.read_json("lineage/latest.json")
    assert view["status"] == "INCOMPLETE" and "silver, gold" in view["error"]
    assert list(view["layers"]) == ["bronze"]
    assert lake.read_json(lake.list("lineage/gold/runs/")[0])["status"] == job.REFUSED  # still on record


def test_refused_run_keeps_the_layers_latest_real_run(small_data):
    lake = MemoryLake()
    assert run_sample(lake, data=small_data, RUN_ID="good") == 0
    lake.delete_prefix("silver/_SUCCESS")
    assert run_sample(lake, data=small_data, LAYER="gold", RUN_ID="refused") == 1
    assert lake.read_json("lineage/gold/latest.json")["run_id"] == "good-gold"


def test_real_failure_shows_until_that_layer_succeeds(small_data, monkeypatch):
    lake = MemoryLake()
    assert run_sample(lake, data=small_data, LAYER="bronze") == 0

    with monkeypatch.context() as m:
        m.setitem(job.STEP_FUNCS, "silver", lambda s, lk, lin: 1 / 0)
        assert run_sample(lake, data=small_data, LAYER="silver") == 1
    assert run_sample(lake, data=small_data, LAYER="bronze") == 0  # a good bronze doesn't excuse a failed silver
    view = lake.read_json("lineage/latest.json")
    assert view["status"] == "FAILED" and view["error"].startswith("silver: ZeroDivisionError")

    assert run_sample(lake, data=small_data, LAYER="silver") == 0
    assert lake.read_json("lineage/latest.json")["status"] == "INCOMPLETE"


# Bug 4: re-running a layer on unchanged input marked everything downstream STALE


@pytest.fixture
def full_lake(small_data):
    lake = MemoryLake()
    assert run_sample(lake, data=small_data, RUN_ID="first") == 0
    return lake


def status(lake):
    return lake.read_json("lineage/latest.json")["status"]


def test_rerunning_bronze_on_unchanged_input_is_not_stale(full_lake, small_data):
    assert run_sample(full_lake, data=small_data, LAYER="bronze", RUN_ID="second") == 0
    assert status(full_lake) == "SUCCEEDED"
    assert job.read_marker(full_lake, "bronze")["run_id"] == "second-bronze"
    assert run_sample(full_lake, data=small_data, LAYER="gold", RUN_ID="third") == 0
    assert full_lake.read_json("lineage/gold/latest.json")["warnings"] == []


def test_same_input_on_a_new_day_is_not_stale(full_lake, small_data):
    assert run_sample(full_lake, data=small_data, LAYER="bronze", RUN_DATE="2026-10-16") == 0
    assert full_lake.exists("bronze/postgres/teams/ingest_date=2026-10-16/teams.csv")
    assert status(full_lake) == "SUCCEEDED"


def test_rerunning_silver_on_unchanged_input_is_not_stale(full_lake, small_data):
    assert run_sample(full_lake, data=small_data, LAYER="silver") == 0
    assert status(full_lake) == "SUCCEEDED"


def test_changed_input_is_stale_until_rebuilt(full_lake, small_data, caplog):
    day = "2026-10-16"
    key = job.pg_key("landing/postgres/", "teams", day)
    original = small_data.joinpath("teams.csv").read_bytes()
    full_lake.objects[key] = original.replace(b"Team ", b"Squad ", 1)  # same rows, different content
    assert run_sample(full_lake, data=small_data, LAYER="bronze", RUN_DATE=day, RUN_ID="new") == 0
    view = full_lake.read_json("lineage/latest.json")
    assert view["status"] == "STALE" and "rerun silver" in view["error"]

    with caplog.at_level("WARNING"):
        assert run_sample(full_lake, data=small_data, LAYER="gold", RUN_DATE=day) == 0
    gold = full_lake.read_json("lineage/gold/latest.json")
    assert len(gold["warnings"]) == 1 and "bronze has changed since (run new-bronze" in gold["warnings"][0]
    assert any("Rerun the silver layer" in r.message for r in caplog.records)
    assert status(full_lake) == "STALE"

    assert run_sample(full_lake, data=small_data, LAYER="silver", RUN_DATE=day) == 0
    assert status(full_lake) == "STALE"  # gold still comes from the old silver
    assert run_sample(full_lake, data=small_data, LAYER="gold", RUN_DATE=day) == 0
    view = full_lake.read_json("lineage/latest.json")
    assert view["status"] == "SUCCEEDED" and view["warnings"] == []
    assert view["layers"]["gold"]["upstream"]["bronze_run_id"] == "new-bronze"


def test_checksum_ignores_dates_but_not_content(lake):
    def seed(day, csv, events):
        for table in job.TABLES:
            lake.objects[job.pg_key(job.BRONZE_PG, table, day)] = csv
        lake.objects[f"{job.BRONZE_EVENTS}year=2024/month=01/day=02/events-000.jsonl"] = events

    seed("2026-01-01", b"id\n1\n", b"{}\n")
    days = {t: "2026-01-01" for t in job.TABLES}
    base = job.bronze_checksum(lake, days)
    seed("2026-01-02", b"id\n1\n", b"{}\n")
    assert job.bronze_checksum(lake, {t: "2026-01-02" for t in job.TABLES}) == base
    seed("2026-01-03", b"id\n2\n", b"{}\n")
    assert job.bronze_checksum(lake, {t: "2026-01-03" for t in job.TABLES}) != base
    seed("2026-01-01", b"id\n1\n", b"[]\n")
    assert job.bronze_checksum(lake, days) != base


def test_no_stale_comparison_by_run_id_left():
    source = (ROOT / "job.py").read_text(encoding="utf-8")
    assert not re.search(r'used\.get\("run_id"\) != ', source)
