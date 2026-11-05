"""Layer pipelines: bronze (extract + bronze), silver, gold; markers, dependency checks, lineage chain."""

import json
import logging

import pytest

import findings
import job
from conftest import MANIFEST, assert_findings_match, make_settings, run_sample
from fakes import MemoryLake


# ---------------------------------------------------------------------------
# entry points
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "layer,steps",
    [("bronze", ("extract", "bronze")), ("silver", ("silver",)), ("gold", ("gold",)), ("all", job.ALL_STEPS), ("SILVER", ("silver",))],
)
def test_layer_maps_to_steps(layer, steps):
    s = job.Settings.from_env({"LAKE_BUCKET": "b", "LAYER": layer, "STEPS": "gold"})  # LAYER wins over STEPS
    assert s.steps == steps and s.layer == layer.lower()


def test_invalid_layer():
    with pytest.raises(job.ConfigError, match="LAYER must be one of"):
        job.Settings.from_env({"LAKE_BUCKET": "b", "LAYER": "platinum"})


def test_steps_still_work_and_run_in_pipeline_order():
    s = job.Settings.from_env({"LAKE_BUCKET": "b", "STEPS": "gold,silver,silver"})
    assert s.steps == ("silver", "gold") and s.layer == ""


def test_layer_groups():
    assert job.layer_groups(["gold", "extract"]) == [("bronze", ("extract",)), ("gold", ("gold",))]
    assert job.layer_groups(job.ALL_STEPS) == [("bronze", ("extract", "bronze")), ("silver", ("silver",)), ("gold", ("gold",))]


def test_cli_layer_flag_overrides_env(lake):
    rc = job.main({"LAKE_BUCKET": "b", "STEPS": "bronze"}, lake, argv=["--layer", "gold"])
    assert rc == 1  # gold has no silver to read
    assert lake.list("lineage/gold/runs/") and not lake.list("lineage/bronze/")


def test_cli_rejects_unknown_layer(capsys):
    with pytest.raises(SystemExit):
        job.main({"LAKE_BUCKET": "b"}, MemoryLake(), argv=["--layer", "platinum"])
    assert "invalid choice" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# dependency checks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("layer,needs", [("silver", "bronze"), ("gold", "silver")])
def test_layer_fails_fast_without_upstream(lake, layer, needs, caplog):
    with caplog.at_level(logging.ERROR):
        rc = job.main({"LAKE_BUCKET": "b", "LAYER": layer, "RUN_ID": "r"}, lake)
    assert rc == 1
    rep = lake.read_json(f"lineage/{layer}/runs/r-{layer}.json")
    assert rep["status"] == job.REFUSED and rep["run_id"] == f"r-{layer}"
    assert f"{needs}/_SUCCESS was not found" in rep["error"] and f"--layer {needs}" in rep["error"]
    assert any(f"Run the {needs} layer first" in r.message for r in caplog.records)
    assert not lake.list(f"{layer}/")  # nothing written before the check
    assert lake.read_json("lineage/latest.json")["status"] == "INCOMPLETE"


def test_failed_layer_removes_its_marker(lake, monkeypatch):
    lake.write_json("bronze/_SUCCESS", {"layer": "bronze", "run_id": "b1", "completed_at": "t0", "upstream": None})
    lake.write_json("silver/_SUCCESS", {"layer": "silver", "run_id": "s0", "completed_at": "t1", "upstream": {"run_id": "b1"}})

    def boom(s, lk, lin):
        raise RuntimeError("disk full")

    monkeypatch.setitem(job.STEP_FUNCS, "silver", boom)
    assert job.main({"LAKE_BUCKET": "b", "LAYER": "silver"}, lake) == 1
    assert job.read_marker(lake, "silver") is None  # stale marker gone -> gold will fail fast
    assert job.main({"LAKE_BUCKET": "b", "LAYER": "gold"}, lake) == 1
    refused = lake.read_json(lake.list("lineage/gold/runs/")[0])
    assert refused["status"] == job.REFUSED and "silver/_SUCCESS was not found" in refused["error"]


def test_extract_only_does_not_complete_bronze(lake, monkeypatch):
    monkeypatch.setitem(job.STEP_FUNCS, "extract", lambda s, lk, lin: lin.record("landing", "teams", "pg", "s3", 3, 3))
    assert job.main({"LAKE_BUCKET": "b", "STEPS": "extract"}, lake) == 0
    assert job.read_marker(lake, "bronze") is None
    assert lake.read_json("lineage/bronze/latest.json")["status"] == "SUCCEEDED"
    view = lake.read_json("lineage/latest.json")
    assert view["status"] == "INCOMPLETE" and "bronze, silver, gold" in view["error"]


def test_main_returns_1_when_lake_unusable(monkeypatch):
    class Broken(MemoryLake):
        def ensure_bucket(self):
            raise ConnectionError("minio down")

    assert job.main({"LAKE_BUCKET": "b", "LAYER": "silver"}, Broken()) == 1


def test_pipeline_view_of_empty_lake(lake):
    view = job.pipeline_view(lake)
    assert view["status"] == "INCOMPLETE" and view["run_id"] is None and view["steps"] == [] and view["duration_s"] == 0


# ---------------------------------------------------------------------------
# layers one by one on the sample data: markers, lineage chain, acceptance gate
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def layered():
    """Run bronze, silver and gold as three separate invocations on one lake."""
    lake = MemoryLake()
    views = {}
    for layer in ("bronze", "silver", "gold"):
        assert run_sample(lake, LAYER=layer, RUN_ID=f"run{layer[0]}") == 0, layer
        views[layer] = lake.read_json("lineage/latest.json")
    return {"lake": lake, "views": views}


def test_each_layer_writes_a_marker_with_row_counts(layered):
    lake = layered["lake"]
    b, s, g = (job.read_marker(lake, layer) for layer in ("bronze", "silver", "gold"))
    assert b["run_id"] == "runb-bronze" and b["upstream"] is None and b["completed_at"]
    assert b["row_counts"]["employees"] == MANIFEST["rows"]["employees"]
    assert b["row_counts"]["learning_activity"] == MANIFEST["events"]["files"]
    assert s["row_counts"] == {**MANIFEST["rows"], "learning_activity": MANIFEST["events"]["kept"]}
    assert g["row_counts"]["gold_promotion_readiness"] == MANIFEST["rows"]["employees"] and len(g["row_counts"]) == 7
    assert len(b["data_checksum"]) == 64 and s["data_checksum"] == g["data_checksum"] == b["data_checksum"]


def test_lineage_chain_links_upstream_run_ids(layered):
    lake = layered["lake"]
    silver = lake.read_json("lineage/silver/latest.json")
    gold = lake.read_json("lineage/gold/latest.json")
    assert silver["upstream"]["run_id"] == "runb-bronze" and silver["upstream"]["layer"] == "bronze"
    assert gold["upstream"]["run_id"] == "runs-silver" and gold["upstream"]["bronze_run_id"] == "runb-bronze"
    assert gold["warnings"] == []
    assert job.read_marker(lake, "gold")["upstream"]["run_id"] == "runs-silver"


def test_checks_stay_in_their_layer(layered):
    lake = layered["lake"]
    bronze = lake.read_json("lineage/bronze/latest.json")["reconciliation"]
    gold = lake.read_json("lineage/gold/latest.json")["reconciliation"]
    assert [c["layer"] for c in bronze] == ["bronze"] and len(gold) == 8 and {c["layer"] for c in gold} == {"gold"}
    assert lake.read_json("lineage/silver/latest.json")["reconciliation"] == []


def test_pipeline_view_progresses_to_succeeded(layered):
    v = layered["views"]
    assert v["bronze"]["status"] == "INCOMPLETE" and v["silver"]["status"] == "INCOMPLETE"
    final = v["gold"]
    assert final["status"] == "SUCCEEDED" and final["error"] is None and final["run_id"] == "rung-gold"
    assert list(final["layers"]) == ["bronze", "silver", "gold"]
    s = findings.run_summary(final)
    events = MANIFEST["events"]
    assert s["reconciliation"] == "9/9" and (s["events_in"], s["events_kept"], s["events_quarantined"]) == (events["lines"], events["kept"], events["quarantined"])


def test_acceptance_gate_with_layers_run_one_by_one(layered, sample_run):
    lake = layered["lake"]
    layered_findings = findings.compute_findings(job.read_gold(lake))
    assert layered_findings == findings.compute_findings(sample_run["gold"])
    assert_findings_match(layered_findings, MANIFEST["expected"])
    site = lake.read_json(job.GOLD_SITE_DATA)  # gold's site data
    assert site["skill_gaps"]["critical"] == MANIFEST["expected"]["skill_gaps"]["critical"]


# ---------------------------------------------------------------------------
# lake export / import (CI hands the lake from job to job)
# ---------------------------------------------------------------------------


def test_export_import_roundtrip(tmp_path):
    src = MemoryLake()
    src.put("bronze/_SUCCESS", b'{"run_id": "x"}')
    src.put("silver/teams/teams.parquet", b"PAR1")
    out = tmp_path / "state"
    assert job.main({"LAKE_BUCKET": "b"}, src, argv=["--export-lake", str(out)]) == 0
    assert (out / "silver" / "teams" / "teams.parquet").read_bytes() == b"PAR1"

    dst = MemoryLake()
    dst.put("silver/teams/teams.parquet", b"PAR1")  # same size -> skipped
    assert job.main({"LAKE_BUCKET": "b"}, dst, argv=["--import-lake", str(out)]) == 0
    assert dst.objects == src.objects
    assert sorted(c for c in dst.calls if c[0] == "put") == [("put", "bronze/_SUCCESS"), ("put", "silver/teams/teams.parquet")]  # 1 pre-seeded + 1 imported


def test_import_missing_dir(tmp_path, lake):
    with pytest.raises(FileNotFoundError, match="lake state directory not found"):
        job.import_lake(lake, tmp_path / "nope")


@pytest.mark.filterwarnings("ignore:invalid value encountered in divide:RuntimeWarning")  # r over 2 identical points
def test_site_data_is_valid_json_on_tiny_data(lake, mini_silver):
    s = make_settings()
    for t in job.TABLES:
        lake.write_parquet(mini_silver[t], f"silver/{t}/{t}.parquet")
    lake.write_parquet(mini_silver["learning_activity"], "silver/learning_activity/year=2026/month=09/part.parquet")
    job.step_gold(s, lake, job.Lineage(s))
    raw = lake.get(job.GOLD_SITE_DATA).decode()
    assert "NaN" not in raw and json.loads(raw)["teams"]["pearson_r"] is None
