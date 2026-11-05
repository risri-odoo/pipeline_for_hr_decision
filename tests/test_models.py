from datetime import date, timedelta

import pytest
from pydantic import ValidationError

import job
from conftest import addr

EMP = dict(employee_id="1", first_name=" Ann ", last_name="Lee", email=addr("Ann.Lee").upper(), team_id="3", job_level="IC1", hire_date="2021-05-01")


def test_employee_valid_strips_and_lowercases():
    e = job.Employee.model_validate(EMP)
    assert e.employee_id == 1 and e.first_name == "Ann"
    assert e.email == addr("ann.lee")


@pytest.mark.parametrize("email", ["no-at-sign.example", "a@b", addr("a b"), ""])
def test_employee_bad_email_rejected(email):
    with pytest.raises(ValidationError, match="email"):
        job.Employee.model_validate({**EMP, "email": email})


def test_employee_future_hire_rejected():
    future = (date.today() + timedelta(days=1)).isoformat()
    with pytest.raises(ValidationError, match="future"):
        job.Employee.model_validate({**EMP, "hire_date": future})


REVIEW = dict(review_id=1, employee_id=1, review_period="2025-H1", rating=3, submitted_date="2025-07-01")


@pytest.mark.parametrize("rating,ok", [(0, False), (1, True), (3, True), (5, True), (6, False)])
def test_rating_range(rating, ok):
    if ok:
        assert job.PerformanceReview.model_validate({**REVIEW, "rating": rating}).rating == rating
    else:
        with pytest.raises(ValidationError, match="rating"):
            job.PerformanceReview.model_validate({**REVIEW, "rating": rating})


@pytest.mark.parametrize("period", ["2025-H3", "2025H1", "25-H1"])
def test_review_period_pattern(period):
    with pytest.raises(ValidationError, match="review_period"):
        job.PerformanceReview.model_validate({**REVIEW, "review_period": period})


@pytest.mark.parametrize(
    "model,record,field",
    [
        (job.Competency, dict(competency_id=1, name="SQL", category="Soft"), "category"),
        (job.Project, dict(project_id=1, name="P", team_id=1, outcome_status="late", outcome_score=5), "outcome_status"),
        (job.Project, dict(project_id=1, name="P", team_id=1, outcome_status="delayed", outcome_score=101), "outcome_score"),
        (job.TalentReview, dict(employee_id=1, review_cycle="2025-H1", potential_rating="Huge", promotion_readiness="Ready Now", attrition_risk="Low"), "potential_rating"),
        (job.TalentReview, dict(employee_id=1, review_cycle="2025-H1", potential_rating="High", promotion_readiness="Soon", attrition_risk="Low"), "promotion_readiness"),
        (job.CompetencyAssessment, dict(assessment_id=1, employee_id=1, competency_id=1, proficiency_level=6, required_level=3, assessed_date="2025-01-01"), "proficiency_level"),
        (job.Team, dict(team_id=1, name="", department="Eng"), "name"),
    ],
)
def test_enums_and_ranges(model, record, field):
    with pytest.raises(ValidationError, match=field):
        model.model_validate(record)


EVENT = dict(event_id="EV0000000001", employee_id="7", event_date="2024-01-02T10:00:00", event_type="quiz_passed", course_name="SQL", score=88)


def test_event_valid_and_extra_ignored():
    e = job.LearningEvent.model_validate({**EVENT, "unexpected": 1})
    assert e.employee_id == 7 and e.score == 88


def test_score_only_on_quiz():
    with pytest.raises(ValidationError, match="score only allowed on quiz events"):
        job.LearningEvent.model_validate({**EVENT, "event_type": "enrollment"})
    assert job.LearningEvent.model_validate({**EVENT, "event_type": "enrollment", "score": None}).score is None


@pytest.mark.parametrize("field,value", [("event_id", "EV123"), ("event_type", "watched"), ("score", 101), ("employee_id", None), ("event_date", "yesterday")])
def test_event_rules(field, value):
    with pytest.raises(ValidationError, match=field):
        job.LearningEvent.model_validate({**EVENT, field: value})
