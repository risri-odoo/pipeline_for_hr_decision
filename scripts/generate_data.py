"""Generate the synthetic ACME dataset: 7 CSV tables + a day-partitioned JSONL event feed.

Deterministic for a given --seed and --employees (standard library only, no clock).
Data-quality problems are planted on purpose, and data/generated/manifest.json records
how many of each. The manifest also holds the expected results, computed here from the clean
records in plain Python, independently of the pandas code in job.py; the tests compare the two.

Usage: python scripts/generate_data.py [--seed 42] [--employees 2000] [--out data/generated]
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import shutil
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "data" / "generated"
DEFAULT_SEED = 42
DEFAULT_EMPLOYEES = 2000
MIN_EMPLOYEES = 20

EMAIL_DOMAIN = "acme.example"
FIRST_EVENT_DAY = date(2023, 9, 4)
LAST_EVENT_DAY = date(2026, 9, 2)
FIRST_HIRE_DAY = date(2018, 9, 3)
LAST_HIRE_DAY = date(2024, 5, 31)  # before every review, so a date before hire_date is always a planted one
PERIODS = (("2024-H2", date(2025, 1, 6)), ("2025-H1", date(2025, 7, 7)))
REVIEW_CYCLE = "2025-H1"
RECENT_DAYS = 180

DEPARTMENTS = ("Engineering", "Data", "Product", "Design", "Sales", "Marketing", "Finance", "People", "Operations", "Support")
TEAM_WORDS = ("Cedar", "Orion", "Harbor", "Falcon", "Summit", "Delta", "Maple", "Comet", "Atlas", "Willow", "Ember", "Quartz")
JOB_LEVELS = ("IC1", "IC2", "IC3", "Senior IC", "Manager", "Senior Manager", "Director")
COMPETENCIES = {
    "Technical": (
        "Python", "SQL", "Data Modeling", "Cloud Platforms", "API Design", "Testing", "Observability", "Security Basics",
        "Automation", "Version Control", "Distributed Systems", "Performance Tuning", "Containers", "Data Pipelines",
        "Machine Learning", "Accessibility",
    ),
    "Leadership": (
        "Coaching", "Delegation", "Conflict Resolution", "Decision Making", "Vision Setting", "Mentoring", "Feedback",
        "Change Management", "Hiring", "Team Building", "Prioritization", "Executive Communication",
    ),
    "Domain": (
        "Market Analysis", "Forecasting", "Contract Basics", "Customer Research", "Pricing", "Compliance", "Budgeting",
        "Vendor Management", "Roadmapping", "Risk Assessment", "Process Design", "Reporting",
    ),
}
COURSES = (
    "Practical SQL", "Python Foundations", "Writing Clear Specs", "Effective One-to-Ones", "Intro to Forecasting",
    "Designing for Accessibility", "Cloud Cost Basics", "Giving Useful Feedback", "Negotiating with Vendors",
    "Secure Coding Essentials", "Data Storytelling", "Running Better Meetings", "Testing in Practice",
    "Budget Planning 101", "Customer Interview Skills", "Leading Through Change", "Incident Response Drills",
    "Roadmaps that Work", "Time Management at Work", "Pricing Fundamentals", "Hiring Without Bias",
    "Automation with Scripts", "Dashboard Design", "Compliance Refresher",
)
FIRST_NAMES = (
    "Asha", "Ben", "Carla", "Dev", "Elena", "Farid", "Grace", "Hugo", "Ines", "Jonas", "Kira", "Liam", "Maya", "Noah",
    "Olga", "Pedro", "Quinn", "Rosa", "Sam", "Tara", "Uma", "Viktor", "Wen", "Yara", "Zane",
)
LAST_NAMES = (
    "Abara", "Bianchi", "Castro", "Dubois", "Eriksen", "Fischer", "Gupta", "Haddad", "Ivanova", "Jensen", "Kimura",
    "Larsen", "Mensah", "Novak", "Okafor", "Petrov", "Quispe", "Rossi", "Sato", "Tanaka", "Ueda", "Vargas", "Weber",
    "Yilmaz", "Zhou",
)
COMMENTS = (
    "Met the goals agreed for the period.",
    "Solid delivery, with room to grow in planning.",
    "Strong collaborator, trusted by the team.",
    "Needs closer support on priorities.",
    "Delivered well, despite shifting scope.",
    "Consistent results and clear communication.",
)
EVENT_TYPES = ("enrollment", "module_started", "module_completed", "quiz_attempted", "quiz_passed", "course_completed", "certification_earned")
EVENT_WEIGHTS = (15, 22, 20, 15, 10, 12, 6)
EVENT_TEXT = {
    "enrollment": "Signed up for {c}.",
    "module_started": "Opened the first module of {c}.",
    "module_completed": "Finished a module of {c}.",
    "quiz_attempted": "Took the quiz for {c}.",
    "quiz_passed": "Passed the quiz for {c}.",
    "course_completed": "Completed {c}.",
    "certification_earned": "Earned the certificate for {c}.",
}
QUIZ_EVENTS = ("quiz_attempted", "quiz_passed")
UNPARSEABLE_DATES = ("yesterday", "N/A", "")
IMPOSSIBLE_DATE = "2026-13-45"

TABLE_COLUMNS = {
    "teams": ("team_id", "name", "department"),
    "competencies": ("competency_id", "name", "category"),
    "employees": ("employee_id", "first_name", "last_name", "email", "team_id", "job_level", "hire_date"),
    "projects": ("project_id", "name", "team_id", "outcome_status", "outcome_score"),
    "performance_reviews": ("review_id", "employee_id", "review_period", "rating", "manager_comment", "submitted_date"),
    "competency_assessments": ("assessment_id", "employee_id", "competency_id", "proficiency_level", "required_level", "assessed_date"),
    "talent_review": ("employee_id", "review_cycle", "potential_rating", "promotion_readiness", "attrition_risk"),
}


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def planted_count(n: int, rate: float, floor: int) -> int:
    return max(floor, round(n * rate))


def weekdays(first: date, last: date) -> list[date]:
    days = (first + timedelta(days=i) for i in range((last - first).days + 1))
    return [d for d in days if d.weekday() < 5]


def random_day(rng: random.Random, first: date, last: date) -> date:
    return first + timedelta(days=rng.randrange((last - first).days + 1))


# ---------------------------------------------------------------------------
# clean records
# ---------------------------------------------------------------------------


def build_tables(rng: random.Random, n_employees: int) -> tuple[dict[str, list[dict]], dict[int, float]]:
    n_teams = max(3, n_employees // 20)
    teams = [
        {"team_id": i, "name": f"Team {TEAM_WORDS[i % len(TEAM_WORDS)]}-{i}", "department": DEPARTMENTS[i % len(DEPARTMENTS)]}
        for i in range(1, n_teams + 1)
    ]
    quality = {t["team_id"]: rng.gauss(0, 0.35) for t in teams}  # latent: drives both ratings and project outcomes

    competencies, cid = [], 0
    for category, names in COMPETENCIES.items():
        for name in names:
            cid += 1
            competencies.append({"competency_id": cid, "name": name, "category": category})

    employees = []
    for i in range(1, n_employees + 1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        employees.append(
            {
                "employee_id": i,
                "first_name": first,
                "last_name": last,
                "email": f"{first}.{last}.{i}@{EMAIL_DOMAIN}".lower(),
                "team_id": (i - 1) % n_teams + 1,
                "job_level": rng.choice(JOB_LEVELS),
                "hire_date": random_day(rng, FIRST_HIRE_DAY, LAST_HIRE_DAY),
            }
        )

    projects = []
    for i in range(1, max(3, n_teams // 3) + 1):
        team_id = rng.randint(1, n_teams)
        score = clamp(60 + 40 * quality[team_id] + rng.gauss(0, 12), 20, 98)
        on_time = rng.random() < clamp((score - 35) / 50, 0.05, 0.95)
        status = "on-time" if on_time else rng.choices(("delayed", "at-risk", "cancelled"), (6, 2, 1))[0]
        projects.append({"project_id": i, "name": f"Project {TEAM_WORDS[i % len(TEAM_WORDS)]} {i}", "team_id": team_id, "outcome_status": status, "outcome_score": round(score, 2)})

    reviews, rid = [], 0
    for e in employees:
        first_rating = int(clamp(round(rng.gauss(3.0 + quality[e["team_id"]], 0.9)), 1, 5))
        second_rating = int(clamp(first_rating + rng.choices((-1, 0, 1, 2), (12, 44, 36, 8))[0], 1, 5))
        for (period, opens), rating in zip(PERIODS, (first_rating, second_rating)):
            rid += 1
            comment = rng.choice(COMMENTS) if rng.random() > 0.05 else ""
            reviews.append(
                {
                    "review_id": rid,
                    "employee_id": e["employee_id"],
                    "review_period": period,
                    "rating": rating,
                    "manager_comment": comment,
                    "submitted_date": opens + timedelta(days=rng.randrange(60)),
                }
            )

    assessments, aid = [], 0
    for e in employees:
        for competency in rng.sample(competencies, 2):
            aid += 1
            required = rng.randint(2, 4)
            proficiency = int(clamp(required + rng.choices((-3, -2, -1, 0, 1, 2), (2, 7, 17, 44, 22, 8))[0], 1, 5))
            assessments.append(
                {
                    "assessment_id": aid,
                    "employee_id": e["employee_id"],
                    "competency_id": competency["competency_id"],
                    "proficiency_level": proficiency,
                    "required_level": required,
                    "assessed_date": random_day(rng, date(2025, 1, 6), date(2026, 6, 30)),
                }
            )

    latest = {r["employee_id"]: r["rating"] for r in reviews}  # later period overwrites
    talent = []
    for e in employees:
        rating = latest[e["employee_id"]]
        talent.append(
            {
                "employee_id": e["employee_id"],
                "review_cycle": REVIEW_CYCLE,
                "potential_rating": rng.choices(("Low", "Medium", "High"), (5 - min(rating, 4), 4, rating))[0],
                "promotion_readiness": rng.choices(("Ready Now", "1-2 Years", "Not Ready"), (rating, 4, 3))[0],
                "attrition_risk": rng.choices(("Low", "Medium", "High"), (rating + 2, 3, max(1, 4 - rating)))[0],
            }
        )

    tables = {
        "teams": teams,
        "competencies": competencies,
        "employees": employees,
        "projects": projects,
        "performance_reviews": reviews,
        "competency_assessments": assessments,
        "talent_review": talent,
    }
    return tables, quality


def build_events(rng: random.Random, employees: list[dict]) -> list[dict]:
    days = weekdays(FIRST_EVENT_DAY, LAST_EVENT_DAY)
    lapsed_until = len(weekdays(FIRST_EVENT_DAY, LAST_EVENT_DAY - timedelta(days=RECENT_DAYS + 30)))
    events = []
    for e in employees:
        pool = days[:lapsed_until] if rng.random() < 0.15 else days  # some people stopped learning a while ago
        courses = rng.sample(COURSES, rng.randint(2, 6))
        for _ in range(rng.randint(6, 24)):
            kind = rng.choices(EVENT_TYPES, EVENT_WEIGHTS)[0]
            course = rng.choice(courses)
            score = None
            if kind == "quiz_attempted":
                score = rng.randint(30, 100)
            elif kind == "quiz_passed":
                score = rng.randint(70, 100)
            at = datetime.combine(rng.choice(pool), time(rng.randint(8, 19), rng.randrange(60)))
            events.append({"employee_id": e["employee_id"], "event_date": at, "event_type": kind, "course_name": course, "text": EVENT_TEXT[kind].format(c=course), "score": score})
    events.sort(key=lambda ev: (ev["event_date"], ev["employee_id"], ev["event_type"], ev["course_name"]))
    for i, ev in enumerate(events, 1):
        ev["event_id"] = f"EV{i:010d}"
    return events


def event_record(ev: dict) -> dict:
    return {
        "event_id": ev["event_id"],
        "employee_id": str(ev["employee_id"]),
        "event_date": ev["event_date"].isoformat(),
        "event_type": ev["event_type"],
        "course_name": ev["course_name"],
        "text": ev["text"],
        "score": ev["score"],
    }


# ---------------------------------------------------------------------------
# planted data-quality issues
# ---------------------------------------------------------------------------


def plant_before_hire(rng: random.Random, rows: list[dict], date_col: str, hire: dict[int, date], count: int) -> int:
    for row in rng.sample(rows, count):
        row[date_col] = hire[row["employee_id"]] - timedelta(days=rng.randint(1, 400))
    return count


def build_event_lines(rng: random.Random, events: list[dict]) -> tuple[dict[date, list[str]], dict]:
    """Write clean events as JSON lines, then plant the bad lines. Each bad line fails exactly one rule first."""
    n = len(events)
    counts = {
        "malformed_json": planted_count(n, 0.0005, 2),
        "duplicate_event_id": planted_count(n, 0.005, 2),
        "null_employee_id": planted_count(n, 0.0025, 2),
        "unparseable_date": planted_count(n, 0.0015, 4),
        "impossible_date": planted_count(n, 0.0004, 2),
        "schema_drift_kept": planted_count(n, 0.0013, 2),
    }
    drifted = {id(ev) for ev in rng.sample(events, counts["schema_drift_kept"])}
    files: dict[date, list[str]] = defaultdict(list)
    for ev in events:
        rec = event_record(ev)
        if id(ev) in drifted:
            rec["notes"] = rec.pop("text")
        files[ev["event_date"].date()].append(json.dumps(rec))

    next_id = n

    def template() -> dict:
        nonlocal next_id
        next_id += 1
        rec = event_record(rng.choice(events))
        rec["event_id"] = f"EV{next_id:010d}"
        return rec

    def insert(day: date, line: str) -> None:
        lines = files[day]
        lines.insert(rng.randint(0, len(lines)), line)

    all_days = sorted(files)
    # duplicates go last in the original's own file, so the first copy is kept
    plain = [ev for ev in events if id(ev) not in drifted]  # a copy of a drifted line would be one more drifted line
    for ev in rng.sample(plain, counts["duplicate_event_id"]):
        day = ev["event_date"].date()
        original = next(line for line in files[day] if ev["event_id"] in line)
        files[day].append(original)

    for i in range(counts["malformed_json"]):
        rec = template()
        line = json.dumps(rec)
        if i % 2:
            line = "{'event_id': '" + rec["event_id"] + "', 'employee_id': '" + rec["employee_id"] + "'}"
        else:
            line = line[: rng.randint(30, len(line) - 5)]  # cut off mid-record
        insert(rng.choice(all_days), line)

    for _ in range(counts["null_employee_id"]):
        rec = template()
        rec["employee_id"] = None
        insert(date.fromisoformat(rec["event_date"][:10]), json.dumps(rec))

    by_value: Counter = Counter()
    for i in range(counts["unparseable_date"]):
        rec = template()
        rec["event_date"] = UNPARSEABLE_DATES[i % len(UNPARSEABLE_DATES)]
        by_value[rec["event_date"]] += 1
        if i == 0:  # drifted AND bad: quarantined for the date, so it never counts as kept drift
            rec["notes"] = rec.pop("text")
        insert(rng.choice(all_days), json.dumps(rec))

    for _ in range(counts["impossible_date"]):
        rec = template()
        rec["event_date"] = IMPOSSIBLE_DATE
        insert(rng.choice(all_days), json.dumps(rec))

    counts["schema_drift_lines"] = counts["schema_drift_kept"] + 1
    counts["unparseable_date_values"] = {repr(k): by_value[k] for k in UNPARSEABLE_DATES}
    return files, counts


# ---------------------------------------------------------------------------
# expected results (independent of job.py)
# ---------------------------------------------------------------------------


def pearson(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 2:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sxx, syy = sum((x - mx) ** 2 for x in xs), sum((y - my) ** 2 for y in ys)
    return sxy / math.sqrt(sxx * syy) if sxx and syy else None


def band(pct: float) -> str:
    return "0%" if pct == 0 else ("1-50%" if pct <= 50 else "51-100%")


def compute_expected(tables: dict[str, list[dict]], events: list[dict]) -> dict:
    employees, reviews, assessments = tables["employees"], tables["performance_reviews"], tables["competency_assessments"]
    team_of = {e["employee_id"]: e["team_id"] for e in employees}
    dept_of_team = {t["team_id"]: t["department"] for t in tables["teams"]}
    talent = {t["employee_id"]: t for t in tables["talent_review"]}
    competency = {c["competency_id"]: c for c in tables["competencies"]}
    periods = [p for p, _ in PERIODS]

    by_employee: dict[int, dict[str, int]] = defaultdict(dict)
    for r in reviews:
        by_employee[r["employee_id"]][r["review_period"]] = r["rating"]
    change = {e: v[periods[1]] - v[periods[0]] for e, v in by_employee.items()}
    latest = {e: v[periods[1]] for e, v in by_employee.items()}
    avg_by_period = {p: sum(v[p] for v in by_employee.values()) / len(by_employee) for p in periods}

    gaps = [(a, a["required_level"] - a["proficiency_level"]) for a in assessments]
    critical = [a for a, g in gaps if g >= 2]
    critical_per_employee = Counter(a["employee_id"] for a in critical)
    critical_per_competency = Counter(competency[a["competency_id"]]["name"] for a in critical)

    ready = [e for e, t in talent.items() if t["potential_rating"] == "High" and t["promotion_readiness"] == "Ready Now" and latest[e] >= 4]

    completed: dict[int, set] = defaultdict(set)
    active = set()
    for ev in events:
        active.add(ev["employee_id"])
        if ev["event_type"] == "course_completed":
            completed[ev["employee_id"]].add(ev["course_name"])

    as_of = max(ev["event_date"] for ev in events)
    window_start = as_of - timedelta(days=RECENT_DAYS)
    recent = {ev["employee_id"] for ev in events if ev["event_date"] > window_start}
    tiers: Counter = Counter()
    high_and_potential = 0
    for e in by_employee:
        t = talent[e]
        score = 2 * (t["attrition_risk"] == "High") + (t["attrition_risk"] == "Medium") + (change[e] < 0) + (latest[e] <= 2) + (e not in recent)
        tier = "High" if score >= 4 else ("Medium" if score >= 2 else "Low")
        tiers[tier] += 1
        high_and_potential += tier == "High" and t["potential_rating"] == "High"

    team_ratings: dict[int, list[int]] = defaultdict(list)
    for e, rating in latest.items():
        team_ratings[team_of[e]].append(rating)
    team_projects: dict[int, list[dict]] = defaultdict(list)
    for p in tables["projects"]:
        team_projects[p["team_id"]].append(p)
    xs, ys, bands = [], [], defaultdict(list)
    for team, plist in team_projects.items():
        rating = sum(team_ratings[team]) / len(team_ratings[team])
        xs.append(rating)
        ys.append(sum(p["outcome_score"] for p in plist) / len(plist))
        bands[band(sum(p["outcome_status"] == "on-time" for p in plist) / len(plist) * 100)].append(rating)

    dept_cat: dict[tuple[str, str], list[int]] = defaultdict(list)
    meeting: dict[str, list[bool]] = defaultdict(list)
    for a, g in gaps:
        c = competency[a["competency_id"]]
        dept_cat[(dept_of_team[team_of[a["employee_id"]]], c["category"])].append(a["proficiency_level"])
        meeting[c["name"]].append(g <= 0)
    dept_cat_avg = {k: sum(v) / len(v) for k, v in dept_cat.items()}
    pct_meeting = {k: sum(v) / len(v) * 100 for k, v in meeting.items()}

    return {
        "ratings": {
            "avg_by_period": avg_by_period,
            "n_improved": sum(c > 0 for c in change.values()),
            "n_same": sum(c == 0 for c in change.values()),
            "n_declined": sum(c < 0 for c in change.values()),
        },
        "skill_gaps": {
            "critical": len(critical),
            "employees_with_critical": len(critical_per_employee),
            "max_critical_for_one_competency": max(critical_per_competency.values(), default=0),
        },
        "promotion": {
            "high_potential": sum(t["potential_rating"] == "High" for t in talent.values()),
            "promotion_ready": len(ready),
            "ready_with_critical_gap": sum(e in critical_per_employee for e in ready),
        },
        "training": {
            "events": len(events),
            "employees_with_activity": len(active),
            "no_completed_course": sum(e["employee_id"] not in completed for e in employees),
            "avg_courses_per_employee": sum(len(v) for v in completed.values()) / len(employees),
        },
        "attrition": {
            "computed_tiers": {t: tiers[t] for t in ("Low", "Medium", "High")},
            "hr_flag_high": sum(t["attrition_risk"] == "High" for t in talent.values()),
            "high_risk_and_high_potential": high_and_potential,
            "as_of_date": as_of.date().isoformat(),
        },
        "teams": {
            "teams_with_projects": len(team_projects),
            "pearson_r": pearson(xs, ys),
            "avg_rating_by_on_time_band": {b: sum(v) / len(v) for b, v in sorted(bands.items())},
        },
        "skills": {
            "strongest_avg_proficiency": max(dept_cat_avg.values()),
            "weakest_avg_proficiency": min(dept_cat_avg.values()),
            "lowest_pct_meeting": min(pct_meeting.values()),
            "overall_pct_meeting": sum(g <= 0 for _, g in gaps) / len(gaps) * 100,
        },
    }


# ---------------------------------------------------------------------------
# output
# ---------------------------------------------------------------------------


def write_csv(path: Path, columns: tuple[str, ...], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def generate(out_dir: Path = DEFAULT_OUT, seed: int = DEFAULT_SEED, employees: int = DEFAULT_EMPLOYEES) -> dict:
    if employees < MIN_EMPLOYEES:
        raise ValueError(f"--employees must be at least {MIN_EMPLOYEES}")
    rng = random.Random(seed)
    tables, _ = build_tables(rng, employees)
    events = build_events(rng, tables["employees"])
    expected = compute_expected(tables, events)  # before dates are damaged; planted dates change none of these figures

    hire = {e["employee_id"]: e["hire_date"] for e in tables["employees"]}
    reviews, assessments = tables["performance_reviews"], tables["competency_assessments"]
    reviews_before_hire = plant_before_hire(rng, reviews, "submitted_date", hire, planted_count(len(reviews), 0.08, 2))
    assessments_before_hire = plant_before_hire(rng, assessments, "assessed_date", hire, planted_count(len(assessments), 0.06, 2))
    files, planted = build_event_lines(rng, events)
    planted["reviews_before_hire"] = reviews_before_hire
    planted["assessments_before_hire"] = assessments_before_hire

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(out_dir / "learning_activity", ignore_errors=True)
    for name, columns in TABLE_COLUMNS.items():
        write_csv(out_dir / f"{name}.csv", columns, tables[name])
    for day, lines in files.items():
        folder = out_dir / "learning_activity" / f"year={day.year:04d}" / f"month={day.month:02d}" / f"day={day.day:02d}"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "events-000.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")

    quarantine = {
        "duplicate event_id": planted["duplicate_event_id"],
        "employee_id null / not int": planted["null_employee_id"],
        "unparseable date": planted["unparseable_date"],
        "malformed json": planted["malformed_json"],
        "impossible date": planted["impossible_date"],
    }
    lines_total = sum(len(lines) for lines in files.values())
    manifest = {
        "seed": seed,
        "employees": employees,
        "rows": {name: len(tables[name]) for name in TABLE_COLUMNS},
        "events": {
            "files": len(files),
            "months": len({(d.year, d.month) for d in files}),
            "lines": lines_total,
            "kept": len(events),
            "quarantined": sum(quarantine.values()),
        },
        "planted": planted,
        "quarantine_by_reason": quarantine,
        "expected": expected,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
    return manifest


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Generate the synthetic ACME dataset with planted data-quality issues.")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED, help=f"random seed (default {DEFAULT_SEED})")
    ap.add_argument("--employees", type=int, default=DEFAULT_EMPLOYEES, help=f"dataset size (default {DEFAULT_EMPLOYEES})")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT, help="output directory (default data/generated)")
    args = ap.parse_args(argv)
    try:
        m = generate(args.out, args.seed, args.employees)
    except ValueError as exc:
        ap.error(str(exc))
    print(f"wrote {args.out}: {sum(m['rows'].values()):,} table rows, {m['events']['lines']:,} event lines in {m['events']['files']} files, {m['events']['quarantined']} planted bad lines")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
