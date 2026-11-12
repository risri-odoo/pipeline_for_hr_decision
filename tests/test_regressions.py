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


# Bug 5 (first CI run): run_layer.sh / run_compose.sh set `umask 077` script-wide while writing .env,
# so data/generated came out owner-only and the Postgres container could not read the CSVs.


@pytest.mark.skipif(find_bash() is None, reason="needs bash")
def test_create_dotenv_keeps_the_callers_umask(tmp_path):
    helpers = (ROOT / "scripts" / "_env.sh").read_text(encoding="utf-8")
    (tmp_path / "env.sh").write_text(helpers.replace("\r\n", "\n"), encoding="utf-8", newline="\n")
    (tmp_path / ".env.example").write_text("POSTGRES_PASS=change-me\nMINIO_ROOT_PASSWORD=x\nLAKE_BUCKET=b\n", encoding="utf-8", newline="\n")
    script = "umask 022; . ./env.sh; create_dotenv; echo \"umask=$(umask)\"; mkdir -p data/generated; stat -c %a .env data/generated 2>/dev/null || true"
    out = subprocess.run([find_bash(), "-c", script], cwd=tmp_path, capture_output=True, text=True, check=True).stdout.split()
    assert out[0] == "umask=0022"
    env = (tmp_path / ".env").read_text(encoding="utf-8")
    assert "change-me" not in env and re.search(r"^POSTGRES_PASS=[0-9a-f]{32}$", env, flags=re.M) and "LAKE_BUCKET=b" in env
    if len(out) == 3 and not Path("C:/").exists():  # file modes are only meaningful on a POSIX filesystem
        assert out[1:] == ["600", "755"]


def test_run_scripts_never_set_a_script_wide_umask():
    for name in ("run_layer.sh", "run_compose.sh", "run_tests.sh"):
        assert "umask" not in (ROOT / "scripts" / name).read_text(encoding="utf-8"), name


# Bug 6 (first CI run): gold ran after bronze failed, because a skipped silver was read as
# "gold picked alone". The real `if:` expressions from the workflow are evaluated below.

WORKFLOW = (ROOT / ".github" / "workflows" / "pipeline.yml").read_text(encoding="utf-8")


def job_condition(job: str) -> str:
    block = re.search(rf"^  {job}:\n(.*?)(?=^  \w+:\n|\Z)", WORKFLOW, flags=re.M | re.S).group(1)
    m = re.search(r"^    if: >-\n((?:      .*\n)+)", block, flags=re.M) or re.search(r"^    if: (.*)$", block, flags=re.M)
    return " ".join(m.group(1).split())


def evaluate(expr: str, ctx: dict) -> bool:
    py = expr.replace("!cancelled()", "True").replace("&&", " and ").replace("||", " or ")
    py = re.sub(r"!(?!=)", " not ", py)
    py = re.sub(r"needs\.(\w+)\.result", r"result['\1']", py)
    py = py.replace("needs.gold.outputs.deployable", "deployable").replace("github.event_name", "event")
    py = py.replace("inputs.layer", "layer").replace("github.ref", "ref")
    return bool(eval(py, {}, ctx))  # noqa: S307 - expressions come from our own workflow file


NEEDS = {"bronze": [], "silver": ["bronze"], "gold": ["silver"], "deploy": ["bronze", "silver", "gold"]}


def simulate(event: str, layer, fails: tuple[str, ...] = (), ref: str = "refs/heads/main") -> dict:
    result: dict[str, str] = {}
    for job in ("bronze", "silver", "gold", "deploy"):
        expr = job_condition(job)
        ctx = {"result": result, "event": event, "layer": layer, "ref": ref, "deployable": "true"}
        runs = evaluate(expr, ctx)
        if "cancelled()" not in expr and "always()" not in expr:  # GitHub adds an implicit success()
            runs = runs and all(result[n] == "success" for n in NEEDS[job])
        result[job] = ("failure" if job in fails else "success") if runs else "skipped"
    return result


@pytest.mark.parametrize(
    "event,layer,fails,expected",
    [
        ("push", None, (), ("success", "success", "success", "success")),
        ("push", None, ("bronze",), ("failure", "skipped", "skipped", "skipped")),
        ("push", None, ("silver",), ("success", "failure", "skipped", "skipped")),
        ("push", None, ("gold",), ("success", "success", "failure", "skipped")),
        ("workflow_dispatch", "all", ("bronze",), ("failure", "skipped", "skipped", "skipped")),
        ("workflow_dispatch", "all", (), ("success", "success", "success", "success")),
        ("workflow_dispatch", "bronze", (), ("success", "skipped", "skipped", "skipped")),
        ("workflow_dispatch", "silver", (), ("skipped", "success", "skipped", "skipped")),
        ("workflow_dispatch", "gold", (), ("skipped", "skipped", "success", "skipped")),
    ],
)
def test_jobs_never_run_after_an_upstream_failure(event, layer, fails, expected):
    got = simulate(event, layer, fails)
    assert (got["bronze"], got["silver"], got["gold"], got["deploy"]) == expected


def test_deploy_only_from_main():
    assert simulate("workflow_dispatch", "all", ref="refs/heads/feature")["deploy"] == "skipped"


def test_no_actions_left_on_node20_majors():
    old = {"actions/checkout": 5, "actions/upload-artifact": 6, "actions/download-artifact": 7, "actions/deploy-pages": 5}
    for action, major in re.findall(r"uses: ([\w/-]+)@v(\d+)", WORKFLOW):
        assert int(major) >= old.get(action, 0), f"{action}@v{major}"
