import json
from datetime import date

import pandas as pd
import pytest
from pydantic import ValidationError

import job
from conftest import addr


def test_format_errors_and_make_reject():
    with pytest.raises(ValidationError) as exc:
        job.Team.model_validate({"team_id": "x", "name": "", "department": "Eng"})
    msg = job.format_errors(exc.value)
    assert "team_id:" in msg and "name:" in msg and "; " in msg

    r = job.make_reject("teams", {"b": 1, "a": None}, "bad", "f.csv#L2")
    assert r == {"dataset": "teams", "error": "bad", "record": '{"a": null, "b": 1}', "source": "f.csv#L2"}
    assert job.make_reject("x", "raw line", "e", "s")["record"] == "raw line"


def test_format_errors_strips_value_error_prefix():
    with pytest.raises(ValidationError) as exc:
        job.Employee.model_validate(dict(employee_id=1, first_name="a", last_name="b", email="bad", team_id=1, job_level="x", hire_date="2020-01-01"))
    assert job.format_errors(exc.value) == "email: invalid email"


def test_read_raw_csv_keeps_text_and_nulls_empty():
    df = job.read_raw_csv(b"id,name,comment\n007,Ann,\n2,,ok\n")
    assert df.loc[0, "id"] == "007"
    assert df.loc[0, "comment"] is None and df.loc[1, "name"] is None


def test_validate_table_quarantines_and_dedupes():
    raw = job.read_raw_csv(b"team_id,name,department\n1,A,Eng\n2,,Eng\n1,A2,Eng\nx,C,Ops\n3,D,Ops\n")
    good, rejects = job.validate_table(raw, job.Team, "team_id", "teams", "bronze/teams.csv")
    assert good["team_id"].tolist() == [1, 3]
    assert good["name"].tolist() == ["A", "D"]  # first duplicate kept
    reasons = {r["source"]: r["error"] for r in rejects}
    assert reasons["bronze/teams.csv#L3"].startswith("name:")
    assert reasons["bronze/teams.csv#L4"] == "duplicate team_id"
    assert reasons["bronze/teams.csv#L5"].startswith("team_id:")


def test_validate_table_converts_dates_and_handles_empty():
    raw = job.read_raw_csv(f"employee_id,first_name,last_name,email,team_id,job_level,hire_date\n1,a,b,{addr('A')},1,IC1,2020-02-03\n".encode())
    good, rejects = job.validate_table(raw, job.Employee, "employee_id", "employees", "s")
    assert not rejects and pd.api.types.is_datetime64_any_dtype(good["hire_date"])
    empty, _ = job.validate_table(raw.iloc[0:0], job.Employee, "employee_id", "employees", "s")
    assert len(empty) == 0 and "hire_date" in empty.columns


def test_split_orphans():
    df = pd.DataFrame({"employee_id": [1, 2, 9], "v": [1, 2, 3]})
    kept, rejects = job.split_orphans(df, "employee_id", [1, 2], "reviews", "src")
    assert kept["employee_id"].tolist() == [1, 2]
    assert [r["error"] for r in rejects] == ["orphan employee_id"]
    assert json.loads(rejects[0]["record"])["employee_id"] == 9


def test_flag_before_hire_keeps_rows():
    emp = pd.DataFrame({"employee_id": [1, 2], "hire_date": pd.to_datetime(["2022-01-01", "2020-01-01"])})
    rev = pd.DataFrame({"employee_id": [1, 2, 3], "submitted_date": pd.to_datetime(["2021-12-31", "2021-01-01", "2021-01-01"])})
    out = job.flag_before_hire(rev, "submitted_date", emp)
    assert len(out) == 3
    assert out["dq_before_hire"].tolist() == [True, False, False]
    assert "dq_before_hire" not in rev.columns


@pytest.mark.parametrize(
    "key,expected",
    [
        ("landing/x/year=2024/month=03/day=05/events-000.jsonl", date(2024, 3, 5)),
        ("no/partition/here.jsonl", None),
        ("year=2024/month=02/day=31/e.jsonl", None),
    ],
)
def test_partition_date(key, expected):
    assert job.partition_date(key) == expected


KEY = "bronze/s3/learning_activity/year=2024/month=01/day=02/events-000.jsonl"


def ev(**kw):
    base = dict(event_id="EV0000000001", employee_id="1", event_date="2024-01-02T09:00:00", event_type="enrollment", course_name="SQL", text="t", score=None)
    base.update(kw)
    return json.dumps(base)


PLANTED = [
    ev(),  # 1 good
    '{"event_id": "EV0000000002", "employee_id": "1", "text": "unterminated',  # 2 malformed
    "{'single': 'quotes'}",  # 3 malformed
    ev(event_id="EV0000000003", employee_id=None),  # 4 null employee
    ev(event_id="EV0000000004", event_date="yesterday"),  # 5 unparseable
    ev(event_id="EV0000000005", event_date="2026-13-45"),  # 6 impossible
    ev(event_id="EV0000000006", event_date="2024-01-03T09:00:00"),  # 7 wrong partition
    ev(),  # 8 duplicate of line 1
    ev(event_id="EV0000000007", employee_id="999"),  # 9 orphan
    json.dumps({**json.loads(ev(event_id="EV0000000008")), "notes": "drifted"} | {"text": None}),  # 10 has text key -> not drift
    json.dumps({k: v for k, v in json.loads(ev(event_id="EV0000000009")).items() if k != "text"} | {"notes": "old field"}),  # 11 drift
    "[1, 2]",  # 12 not an object
]


def test_clean_events_handles_every_planted_issue():
    lines = [(KEY, i, t) for i, t in enumerate(PLANTED, 1)]
    df, rejects = job.clean_events(lines, [1, 2])
    assert df["event_id"].tolist() == ["EV0000000001", "EV0000000008", "EV0000000009"]
    by_line = {int(r["source"].rsplit("#L", 1)[1]): job.reject_reason(r["error"]) for r in rejects}
    assert by_line == {
        2: "malformed json",
        3: "malformed json",
        4: "employee_id null / not int",
        5: "unparseable date",
        6: "impossible date",
        7: "event_date != partition",
        8: "duplicate event_id",
        9: "orphan employee_id",
        12: "malformed json",
    }
    drift = df.set_index("event_id")["schema_drift"]
    assert drift["EV0000000009"] and not drift["EV0000000008"]
    assert df.set_index("event_id").loc["EV0000000009", "text"] == "old field"
    assert df["_source"].iloc[0] == f"{KEY}#L1"
    assert str(df["score"].dtype) == "Int64" and df["employee_id"].dtype == "int64"


def test_clean_events_is_idempotent():
    lines = [(KEY, i, t) for i, t in enumerate(PLANTED, 1)]
    a, ra = job.clean_events(lines, [1, 2])
    b, rb = job.clean_events(lines, [1, 2])
    pd.testing.assert_frame_equal(a, b)
    assert ra == rb


def test_duplicate_keeps_first_valid_copy():
    lines = [(KEY, 1, ev(event_date="bad")), (KEY, 2, ev()), (KEY, 3, ev())]
    df, rejects = job.clean_events(lines, [1])
    assert df["_source"].tolist() == [f"{KEY}#L2"]
    assert [job.reject_reason(r["error"]) for r in rejects] == ["unparseable date", "duplicate event_id"]


def test_reject_reason_fallback():
    assert job.reject_reason("course_name: String should have at least 1 character") == "course_name: invalid"


def test_add_audit_cols():
    out = job.add_audit_cols(pd.DataFrame({"a": [1]}), "s3://b/k", "run-1")
    assert out.loc[0, "_source"] == "s3://b/k" and out.loc[0, "_run_id"] == "run-1"
    assert out["_processed_at"].dt.tz is not None
    kept = job.add_audit_cols(pd.DataFrame({"a": [1], "_source": ["line#L1"]}), "other", "r")
    assert kept.loc[0, "_source"] == "line#L1"


def test_count_csv_rows_handles_quoted_newlines():
    assert job.count_csv_rows(b'id,comment\n1,"two\nlines, and comma"\n2,x\n') == 2
    assert job.count_csv_rows(b"") == 0
