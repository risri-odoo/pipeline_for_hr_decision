import math

import pandas as pd
import pytest

import findings
import job
from conftest import MANIFEST, assert_findings_match, make_settings, run_sample
from fakes import MemoryLake


@pytest.fixture
def gold(mini_silver):
    return job.build_gold(mini_silver)


def row(df, key, value):
    return df.set_index(key).loc[value]


def test_employee_dim(mini_silver):
    dim = job.employee_dim(mini_silver["employees"], mini_silver["teams"])
    r = row(dim, "employee_id", 3)
    assert (r["employee_name"], r["team_name"], r["department"]) == ("Cy C", "Beta", "Sales")


def test_review_history_prev_and_change(gold):
    h = gold["gold_employee_review_history"]
    assert len(h) == 6
    e1 = h[h["employee_id"] == 1]
    assert e1["review_period"].tolist() == ["2024-H2", "2025-H1"]
    assert math.isnan(e1["prev_rating"].iloc[0]) and e1["prev_rating"].iloc[1] == 3 and e1["rating_change"].iloc[1] == 1
    assert h.loc[h["employee_id"] == 2, "rating_change"].iloc[1] == -2
    assert h["employee_id"].is_monotonic_increasing


def test_skill_gaps_severity(gold):
    g = gold["gold_skill_gaps"].set_index("assessment_id")
    assert g["gap"].tolist() == [3, 1, -1, 1]
    assert g["gap_severity"].tolist() == ["critical", "minor", "none", "minor"]
    assert g.loc[2, "competency_name"] == "Negotiation" and g.loc[2, "category"] == "Leadership"


def test_promotion_readiness_rule(gold):
    p = gold["gold_promotion_readiness"].set_index("employee_id")
    assert len(p) == 4
    # High + Ready Now + latest >= 4
    assert p["is_promotion_ready"].tolist() == [True, False, False, False]
    assert p.loc[1, "critical_gaps"] == 1 and p.loc[4, "critical_gaps"] == 0
    assert p.loc[1, "avg_rating"] == 3.5 and p.loc[1, "latest_period"] == "2025-H1"
    assert p["is_high_potential"].tolist() == [True, True, True, False]


def test_training_summary(gold):
    t = gold["gold_training_summary"].set_index("employee_id")
    assert len(t) == 4
    assert t.loc[1, "courses_completed"] == 2 and t.loc[1, "completed_course_list"] == "Python; SQL"
    assert t.loc[1, "avg_quiz_score"] == 80.5 and t.loc[1, "total_events"] == 4
    assert t.loc[1, "n_quiz_passed"] == 1 and t.loc[1, "n_certification_earned"] == 0
    assert t.loc[1, "last_activity"] == pd.Timestamp("2026-09-01T10:00")
    # no events -> zeros
    assert t.loc[4, "total_events"] == 0 and t.loc[4, "courses_completed"] == 0 and t.loc[4, "completed_course_list"] == ""
    assert {f"n_{e}" for e in job.EVENT_TYPES} <= set(t.columns)


def test_attrition_risk_score(gold):
    a = gold["gold_attrition_risk"].set_index("employee_id")
    assert (a["as_of_date"] == "2026-09-02").all()
    assert a["risk_score"].tolist() == [0, 5, 1, 4]
    assert a["computed_risk_tier"].tolist() == ["Low", "High", "Low", "High"]
    assert a.loc[2, "risk_drivers"] == "hr_flag_high,declining_rating,low_rating,no_recent_learning"
    assert a.loc[1, "recent_learning_events"] == 4 and a.loc[2, "recent_learning_events"] == 0


def test_attrition_medium_tier(mini_silver):
    mini_silver["talent_review"].loc[2, "attrition_risk"] = "High"  # employee 3: 2 points
    a = job.build_gold(mini_silver)["gold_attrition_risk"].set_index("employee_id")
    assert a.loc[3, "risk_score"] == 2 and a.loc[3, "computed_risk_tier"] == "Medium"


def test_team_performance(gold):
    t = gold["gold_team_performance"].set_index("team_id")
    assert len(t) == 3
    assert t.loc[1, "headcount"] == 2 and t.loc[1, "avg_rating"] == 3.0
    assert t.loc[1, "pct_high_performers"] == 50 and t.loc[1, "pct_low_performers"] == 50
    assert t.loc[1, "project_count"] == 2 and t.loc[1, "avg_outcome_score"] == 70 and t.loc[1, "pct_on_time"] == 50
    assert t.loc[2, "n_delayed_or_at_risk"] == 1 and t.loc[2, "pct_on_time"] == 0
    assert not t.loc[3, "has_projects"] and t.loc[3, "headcount"] == 0 and t.loc[3, "project_count"] == 0
    assert (t["review_period"] == "2025-H1").all()


def test_skill_distribution(gold):
    d = gold["gold_skill_distribution"].set_index(["department", "category", "competency_name"])
    py = d.loc[("Eng", "Technical", "Python")]
    assert py["n_assessments"] == 2 and py["employees_assessed"] == 2
    assert py["avg_proficiency"] == 2.5 and py["avg_required"] == 3.5
    assert py["pct_meeting_required"] == 50 and py["critical_gap_count"] == 1
    assert d.loc[("Sales", "Leadership", "Negotiation"), "pct_meeting_required"] == 0
    assert d["n_assessments"].sum() == 4


def test_gold_checks_pass_and_fail(mini_silver, gold):
    lin = job.Lineage(make_settings())
    assert job.gold_checks(lin, mini_silver, gold)
    assert len(lin.reconciliation) == 8 and all(c["passed"] for c in lin.reconciliation)

    broken = dict(gold, gold_promotion_readiness=gold["gold_promotion_readiness"].iloc[:-1])
    lin2 = job.Lineage(make_settings())
    assert not job.gold_checks(lin2, mini_silver, broken)
    assert [c["check"] for c in lin2.reconciliation if not c["passed"]] == ["promotion rows == employees"]


@pytest.mark.filterwarnings("ignore:invalid value encountered in divide:RuntimeWarning")  # r over 2 identical points
def test_step_gold_raises_on_failed_check(lake, settings, mini_silver, monkeypatch):
    for t in job.TABLES:
        lake.write_parquet(mini_silver[t], f"silver/{t}/{t}.parquet")
    lake.write_parquet(mini_silver["learning_activity"], "silver/learning_activity/year=2026/month=09/part.parquet")
    lin = job.Lineage(settings)
    gold = job.step_gold(settings, lake, lin)
    assert set(gold) == set(job.GOLD_TABLES)
    assert all(lake.exists(f"gold/{t}/{t}.parquet") for t in job.GOLD_TABLES)

    monkeypatch.setattr(job, "gold_checks", lambda *a: False)
    with pytest.raises(RuntimeError, match="gold reconciliation failed"):
        job.step_gold(settings, lake, job.Lineage(settings))


def test_on_time_band():
    assert [findings.on_time_band(p) for p in (0, 0.5, 50, 50.01, 100)] == ["0%", "1-50%", "1-50%", "51-100%", "51-100%"]


# ---------------------------------------------------------------------------
# acceptance gate on the generated data: pipeline output vs data/generated/manifest.json
# ---------------------------------------------------------------------------

EVENTS = MANIFEST["events"]


def test_sample_run_succeeds_with_9_of_9(sample_run):
    lin = sample_run["lineage"]
    assert sample_run["rc"] == 0 and lin["status"] == "SUCCEEDED" and lin["error"] is None
    assert len(lin["reconciliation"]) == 9 and all(c["passed"] for c in lin["reconciliation"])
    assert lin["reconciliation"][0]["expected"] == sum(MANIFEST["rows"].values())
    s = findings.run_summary(lin)
    assert (s["events_in"], s["events_kept"], s["events_quarantined"]) == (EVENTS["lines"], EVENTS["kept"], EVENTS["quarantined"])
    assert s["reconciliation"] == "9/9" and s["environment"] == "pytest"


def test_sample_timings_reported(sample_run):
    steps = sample_run["lineage"]["steps"]
    assert {(s["layer"], s["dataset"]) for s in steps} >= {("gold", t) for t in job.GOLD_TABLES}
    for s in steps:
        assert s["duration_s"] >= 0
        assert s["rows_per_min"] is None or s["rows_per_min"] > 0
    total = sample_run["lineage"]["duration_s"]
    print(f"\nfull run (in-memory lake): {total:.2f}s")
    assert total < 300  # SLA: full run < 5 min


def test_sample_quarantine_matches_planted_counts(sample_run):
    lake = sample_run["lake"]
    rej = lake.read_parquet("silver/_quarantine/learning_activity/run_date=2026-10-15/rejects.parquet")
    assert rej["error"].map(job.reject_reason).value_counts().to_dict() == MANIFEST["quarantine_by_reason"]
    assert (rej["run_id"] == sample_run["lineage"]["layers"]["silver"]["run_id"]).all()
    # the tables have no planted rejects
    assert not [k for k in lake.list("silver/_quarantine/") if "learning_activity" not in k]


def test_sample_silver_counts(sample_run):
    lake = sample_run["lake"]
    silver = job.read_silver(lake)
    assert {k: len(v) for k, v in silver.items()} == {**MANIFEST["rows"], "learning_activity": EVENTS["kept"]}
    planted = MANIFEST["planted"]
    assert silver["performance_reviews"]["dq_before_hire"].sum() == planted["reviews_before_hire"]
    assert silver["competency_assessments"]["dq_before_hire"].sum() == planted["assessments_before_hire"]
    assert silver["learning_activity"]["schema_drift"].sum() == planted["schema_drift_kept"]
    assert len([k for k in lake.list("silver/learning_activity/") if k.endswith(".parquet")]) == EVENTS["months"]


def test_sample_findings_match_manifest(sample_run):
    assert_findings_match(findings.compute_findings(sample_run["gold"]), MANIFEST["expected"])


def test_sample_rerun_is_idempotent(sample_run):
    lake = MemoryLake()
    assert run_sample(lake) == 0
    first = lake.read_json("lineage/latest.json")
    before = {t: len(df) for t, df in job.read_gold(lake).items()}
    assert run_sample(lake) == 0
    lin = lake.read_json("lineage/latest.json")
    assert lin["run_id"] != first["run_id"] and lin["status"] == "SUCCEEDED"
    assert findings.run_summary(lin)["events_quarantined"] == EVENTS["quarantined"]
    assert {t: len(df) for t, df in job.read_gold(lake).items()} == before
    notes = {(s["layer"], s["dataset"]): s["note"] for s in lin["steps"]}
    assert notes[("landing", "employees")] == "already extracted today, skipped"
    assert notes[("bronze", "employees")] == "already in bronze, skipped"
    assert len(lake.list("lineage/gold/runs/")) == 2 and len(lake.list("lineage/bronze/runs/")) == 2


def test_findings_main_prints_json(sample_run, monkeypatch, capsys):
    monkeypatch.setenv("LAKE_BUCKET", "test-lake")
    monkeypatch.setattr(job.Lake, "from_settings", classmethod(lambda cls, s: sample_run["lake"]))
    assert findings.main() == 0
    out = capsys.readouterr().out
    assert f'"promotion_ready": {MANIFEST["expected"]["promotion"]["promotion_ready"]}' in out and '"status": "SUCCEEDED"' in out
