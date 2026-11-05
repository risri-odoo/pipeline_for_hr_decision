"""scripts/generate_data.py: determinism, schema, planted counts, and agreement with the pipeline."""

import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

import pytest

import findings
import generate_data
import job
from conftest import MANIFEST, ROOT, assert_findings_match, run_sample
from fakes import MemoryLake


def digest(folder: Path) -> dict[str, str]:
    return {p.relative_to(folder).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(folder.rglob("*")) if p.is_file()}


def read_csv(folder: Path, table: str) -> list[dict]:
    with (folder / f"{table}.csv").open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_same_seed_gives_identical_files(tmp_path):
    a, b, c = tmp_path / "a", tmp_path / "b", tmp_path / "c"
    generate_data.generate(a, seed=11, employees=40)
    generate_data.generate(b, seed=11, employees=40)
    generate_data.generate(c, seed=12, employees=40)
    assert digest(a) == digest(b)
    assert digest(a) != digest(c)


def test_rerun_replaces_old_output(tmp_path):
    generate_data.generate(tmp_path, seed=1, employees=200)
    generate_data.generate(tmp_path, seed=1, employees=20)
    assert generate_data.generate(tmp_path, seed=1, employees=20)["events"]["files"] == len(list(tmp_path.rglob("*.jsonl")))


def test_employees_flag_scales_the_dataset(tmp_path):
    m = generate_data.generate(tmp_path, employees=100)
    assert m["rows"]["employees"] == m["rows"]["talent_review"] == 100
    assert m["rows"]["performance_reviews"] == m["rows"]["competency_assessments"] == 200
    assert m["rows"]["teams"] == 5 and m["rows"]["competencies"] == 40


def test_too_few_employees_rejected(capsys):
    with pytest.raises(SystemExit):
        generate_data.main(["--employees", "5", "--out", "unused"])
    assert "at least" in capsys.readouterr().err


def test_cli(tmp_path, capsys):
    assert generate_data.main(["--seed", "3", "--employees", "30", "--out", str(tmp_path)]) == 0
    assert "planted bad lines" in capsys.readouterr().out
    assert json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))["seed"] == 3


def test_csv_headers_match_the_postgres_schema(small_data):
    sql = (ROOT / "docker" / "postgres-init" / "01-schema.sql").read_text(encoding="utf-8")
    for table, body in re.findall(r"CREATE TABLE (\w+) \((.*?)\n\);", sql, flags=re.S):
        columns = [line.split()[0] for line in body.strip().splitlines()]
        header = (small_data / f"{table}.csv").read_text(encoding="utf-8").splitlines()[0].split(",")
        assert header == columns == list(generate_data.TABLE_COLUMNS[table]), table


def test_event_files_are_day_partitioned(small_data):
    keys = [p.relative_to(small_data).as_posix() for p in small_data.rglob("*.jsonl")]
    assert keys and all(re.fullmatch(r"learning_activity/year=\d{4}/month=\d{2}/day=\d{2}/events-000\.jsonl", k) for k in keys)


def scan_events(folder: Path) -> Counter:
    """Count the planted issues by reading the raw lines, without using job.py."""
    c: Counter = Counter()
    seen = set()
    for path in folder.rglob("*.jsonl"):
        for line in path.read_text(encoding="utf-8").splitlines():
            c["lines"] += 1
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                c["malformed_json"] += 1
                continue
            c["schema_drift_lines"] += "notes" in rec and "text" not in rec
            c["null_employee_id"] += rec["employee_id"] is None
            c["duplicate_event_id"] += rec["event_id"] in seen
            seen.add(rec["event_id"])
            c["unparseable_date"] += rec["event_date"] in generate_data.UNPARSEABLE_DATES
            c["impossible_date"] += rec["event_date"] == generate_data.IMPOSSIBLE_DATE
    return c


def test_manifest_counts_match_the_files():
    data = ROOT / "data" / "generated"
    planted, found = MANIFEST["planted"], scan_events(data)
    assert found["lines"] == MANIFEST["events"]["lines"]
    for key in ("malformed_json", "schema_drift_lines", "null_employee_id", "duplicate_event_id", "unparseable_date", "impossible_date"):
        assert found[key] == planted[key], key
    assert sum(planted["unparseable_date_values"].values()) == planted["unparseable_date"]
    assert all(planted["unparseable_date_values"].values())  # all three bad strings are present

    hire = {r["employee_id"]: r["hire_date"] for r in read_csv(data, "employees")}
    assert sum(r["submitted_date"] < hire[r["employee_id"]] for r in read_csv(data, "performance_reviews")) == planted["reviews_before_hire"]
    assert sum(r["assessed_date"] < hire[r["employee_id"]] for r in read_csv(data, "competency_assessments")) == planted["assessments_before_hire"]
    assert MANIFEST["events"]["lines"] == MANIFEST["events"]["kept"] + MANIFEST["events"]["quarantined"] == sum(MANIFEST["quarantine_by_reason"].values()) + MANIFEST["events"]["kept"]


@pytest.mark.parametrize("seed,employees", [(1, 60), (2, 150)])
def test_pipeline_agrees_with_the_manifest_for_other_datasets(tmp_path, seed, employees):
    m = generate_data.generate(tmp_path, seed=seed, employees=employees)
    lake = MemoryLake()
    assert run_sample(lake, data=tmp_path) == 0
    view = lake.read_json("lineage/latest.json")
    assert view["status"] == "SUCCEEDED" and findings.run_summary(view)["events_quarantined"] == m["events"]["quarantined"]
    rej = lake.read_parquet("silver/_quarantine/learning_activity/run_date=2026-10-15/rejects.parquet")
    assert rej["error"].map(job.reject_reason).value_counts().to_dict() == m["quarantine_by_reason"]
    assert_findings_match(findings.compute_findings(job.read_gold(lake)), m["expected"])


def test_pearson_edge_cases():
    assert generate_data.pearson([1.0], [2.0]) is None
    assert generate_data.pearson([1.0, 1.0], [2.0, 3.0]) is None  # no variance
    assert generate_data.pearson([1.0, 2.0, 3.0], [2.0, 4.0, 6.0]) == pytest.approx(1.0)
