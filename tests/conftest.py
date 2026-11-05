from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts"), str(ROOT / "tests")]

import generate_data  # noqa: E402
import job  # noqa: E402
from fakes import MemoryLake, csv_exporter  # noqa: E402

DATA = ROOT / "data" / "generated"
RUN_DATE = "2026-10-15"

if not (DATA / "manifest.json").exists():
    generate_data.generate(DATA)
MANIFEST = json.loads((DATA / "manifest.json").read_text(encoding="utf-8"))


def addr(name: str) -> str:
    """A fake address, built at runtime so the repo holds no literal email addresses."""
    return f"{name}@{generate_data.EMAIL_DOMAIN}"


def assert_findings_match(f: dict, e: dict) -> None:
    """Pipeline findings vs the generator's independent expected results.

    Counts match exactly. Averages are compared with a tolerance because Gold rounds its
    intermediate values to 2 dp, while the generator works unrounded."""
    r, g, p, t, a, tm, sk = (e[k] for k in ("ratings", "skill_gaps", "promotion", "training", "attrition", "teams", "skills"))
    assert f["ratings"]["avg_by_period"] == pytest.approx(r["avg_by_period"], abs=0.0051)
    assert (f["ratings"]["n_improved"], f["ratings"]["n_same"], f["ratings"]["n_declined"]) == (r["n_improved"], r["n_same"], r["n_declined"])
    assert (f["skill_gaps"]["critical"], f["skill_gaps"]["employees_with_critical"]) == (g["critical"], g["employees_with_critical"])
    if g["critical"]:
        assert f["skill_gaps"]["top_competencies"][0][1] == g["max_critical_for_one_competency"]
    assert f["promotion"] == p
    assert (f["training"]["events"], f["training"]["employees_with_activity"], f["training"]["no_completed_course"]) == (t["events"], t["employees_with_activity"], t["no_completed_course"])
    assert f["training"]["avg_courses_per_employee"] == pytest.approx(t["avg_courses_per_employee"], abs=0.0051)
    assert f["attrition"]["computed_tiers"] == a["computed_tiers"]
    assert (f["attrition"]["hr_flag_high"], f["attrition"]["high_risk_and_high_potential"], f["attrition"]["as_of_date"]) == (a["hr_flag_high"], a["high_risk_and_high_potential"], a["as_of_date"])
    assert f["teams"]["teams_with_projects"] == tm["teams_with_projects"]
    if tm["pearson_r"] is not None:
        assert f["teams"]["pearson_r"] == pytest.approx(tm["pearson_r"], abs=0.02)
    assert f["teams"]["avg_rating_by_on_time_band"] == pytest.approx(tm["avg_rating_by_on_time_band"], abs=0.011)
    assert f["skills"]["strongest"][2] == pytest.approx(sk["strongest_avg_proficiency"], abs=0.011)
    assert f["skills"]["weakest"][2] == pytest.approx(sk["weakest_avg_proficiency"], abs=0.011)
    assert f["skills"]["lowest_pct_meeting"][1] == pytest.approx(sk["lowest_pct_meeting"], abs=0.06)
    assert f["skills"]["overall_pct_meeting"] == pytest.approx(sk["overall_pct_meeting"], abs=0.06)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Retries back off with time.sleep; tests should not wait."""
    sleeps: list[float] = []
    monkeypatch.setattr(job.time, "sleep", sleeps.append)
    return sleeps


@pytest.fixture
def lake() -> MemoryLake:
    return MemoryLake()


def make_settings(**overrides) -> job.Settings:
    env = {"LAKE_BUCKET": "test-lake", "RUN_DATE": RUN_DATE, "RUN_ID": "20261015T000000Z-abc123"}
    env.update({k.upper(): v for k, v in overrides.items()})
    return job.Settings.from_env(env)


@pytest.fixture
def settings() -> job.Settings:
    return make_settings()


def run_sample(lake: MemoryLake, data: Path = DATA, **env) -> int:
    """Full pipeline on generated data with an in-memory lake and CSV-backed 'Postgres'."""
    base = {
        "LAKE_BUCKET": "test-lake",
        "RUN_DATE": RUN_DATE,
        "EVENTS_SOURCE_DIR": str(data / "learning_activity"),
        "EXECUTION_ENV": "pytest",
    }
    base.update(env)
    original = job.STEP_FUNCS["extract"]
    job.STEP_FUNCS["extract"] = lambda s, lk, lin: job.step_extract(s, lk, lin, csv_exporter(data))
    try:
        return job.main(base, lake)
    finally:
        job.STEP_FUNCS["extract"] = original


@pytest.fixture(scope="session")
def sample_run():
    """One full run on the generated data, shared by the slower tests."""
    lk = MemoryLake()
    rc = run_sample(lk)
    return {"rc": rc, "lake": lk, "lineage": lk.read_json("lineage/latest.json"), "gold": job.read_gold(lk)}


@pytest.fixture(scope="session")
def small_data(tmp_path_factory) -> Path:
    """A small generated dataset for tests that run the pipeline several times."""
    out = tmp_path_factory.mktemp("small-data")
    generate_data.generate(out, seed=7, employees=60)
    return out


# ---------------------------------------------------------------------------
# tiny hand-built silver layer for rule-level Gold tests
# ---------------------------------------------------------------------------


@pytest.fixture
def mini_silver() -> dict[str, pd.DataFrame]:
    ts = pd.to_datetime
    teams = pd.DataFrame({"team_id": [1, 2, 3], "name": ["Alpha", "Beta", "Gamma"], "department": ["Eng", "Sales", "Eng"]})
    competencies = pd.DataFrame({"competency_id": [10, 20], "name": ["Python", "Negotiation"], "category": ["Technical", "Leadership"]})
    employees = pd.DataFrame(
        {
            "employee_id": [1, 2, 3, 4],
            "first_name": ["Ann", "Bob", "Cy", "Di"],
            "last_name": ["A", "B", "C", "D"],
            "email": [addr("a"), addr("b"), addr("c"), addr("d")],
            "team_id": [1, 1, 2, 2],
            "job_level": ["IC1", "IC2", "Manager", "IC1"],
            "hire_date": ts(["2020-01-01"] * 4),
        }
    )
    projects = pd.DataFrame(
        {
            "project_id": [100, 101, 102],
            "name": ["P1", "P2", "P3"],
            "team_id": [1, 1, 2],
            "outcome_status": ["on-time", "delayed", "at-risk"],
            "outcome_score": [80.0, 60.0, 40.0],
        }
    )
    reviews = pd.DataFrame(
        {
            "review_id": [1, 2, 3, 4, 5, 6],
            "employee_id": [1, 1, 2, 2, 3, 4],
            "review_period": ["2024-H2", "2025-H1", "2024-H2", "2025-H1", "2025-H1", "2025-H1"],
            "rating": [3, 4, 4, 2, 5, 1],
            "manager_comment": ["ok"] * 6,
            "submitted_date": ts(["2025-01-10", "2025-07-10"] * 3),
            "dq_before_hire": [False] * 6,
        }
    )
    assessments = pd.DataFrame(
        {
            "assessment_id": [1, 2, 3, 4],
            "employee_id": [1, 1, 2, 3],
            "competency_id": [10, 20, 10, 20],
            "proficiency_level": [1, 3, 4, 2],
            "required_level": [4, 4, 3, 3],
            "assessed_date": ts(["2025-03-01"] * 4),
            "dq_before_hire": [False] * 4,
        }
    )
    talent = pd.DataFrame(
        {
            "employee_id": [1, 2, 3, 4],
            "review_cycle": ["2025-H1"] * 4,
            "potential_rating": ["High", "High", "High", "Low"],
            "promotion_readiness": ["Ready Now", "Ready Now", "1-2 Years", "Not Ready"],
            "attrition_risk": ["Low", "High", "Medium", "High"],
        }
    )
    events = pd.DataFrame(
        {
            "event_id": ["EV0000000001", "EV0000000002", "EV0000000003", "EV0000000004", "EV0000000005", "EV0000000006"],
            "employee_id": [1, 1, 1, 1, 2, 3],
            "event_date": ts(["2026-09-01T10:00", "2026-08-01T10:00", "2026-08-02T10:00", "2026-08-03T10:00", "2025-01-01T10:00", "2026-09-02T12:00"]),
            "event_type": ["course_completed", "course_completed", "quiz_attempted", "quiz_passed", "enrollment", "course_completed"],
            "course_name": ["SQL", "Python", "Python", "Python", "Go", "SQL"],
            "text": [None] * 6,
            "score": pd.array([None, None, 70, 91, None, None], dtype="Int64"),
            "schema_drift": [False] * 6,
        }
    )
    return {
        "teams": teams,
        "competencies": competencies,
        "employees": employees,
        "projects": projects,
        "performance_reviews": reviews,
        "competency_assessments": assessments,
        "talent_review": talent,
        "learning_activity": events,
    }
