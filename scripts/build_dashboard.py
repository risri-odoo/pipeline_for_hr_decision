"""Static dashboard: Gold + lineage -> site/index.html (+ site/lineage.json).

Self-contained: inline SVG charts (matplotlib), inline CSS, no JavaScript, no external links.
Aggregate only: no employee ids, names, emails or per-employee rows.

Usage: python scripts/build_dashboard.py [--out site]   (lake settings from env, like job.py)
"""

from __future__ import annotations

import argparse
import html
import io
import json
import re
import sys
from pathlib import Path

import matplotlib
import pandas as pd
from matplotlib.figure import Figure

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import findings  # noqa: E402
import job  # noqa: E402

# ---------------------------------------------------------------------------
# chart style: light values are drawn by matplotlib, then swapped for CSS variables
# so the same SVG follows the page's light/dark theme.
# ---------------------------------------------------------------------------

LIGHT = {
    "s1": "#2a78d6",
    "s2": "#eb6834",
    "s3": "#1baf7a",
    "ink": "#0b0b0b",
    "ink2": "#52514e",
    "grid": "#e4e3df",
    "muted": "#a6a59f",
}
DARK = {
    "s1": "#3987e5",
    "s2": "#d95926",
    "s3": "#199e70",
    "ink": "#f0efec",
    "ink2": "#c3c2b7",
    "grid": "#383835",
    "muted": "#6b6a64",
}
SERIES = [LIGHT["s1"], LIGHT["s2"], LIGHT["s3"]]
FONT = "system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"


def apply_style() -> None:
    matplotlib.rcParams.update(
        {
            "svg.fonttype": "none",
            "svg.hashsalt": "acme-talent-etl",
            "font.family": "sans-serif",
            "font.size": 10,
            "axes.edgecolor": LIGHT["grid"],
            "axes.labelcolor": LIGHT["ink2"],
            "axes.titlesize": 11,
            "axes.titlecolor": LIGHT["ink"],
            "axes.titleweight": "bold",
            "axes.titlelocation": "left",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.axisbelow": True,
            "grid.color": LIGHT["grid"],
            "grid.linewidth": 0.8,
            "xtick.color": LIGHT["ink2"],
            "ytick.color": LIGHT["ink2"],
            "xtick.major.size": 0,
            "ytick.major.size": 0,
            "legend.frameon": False,
            "legend.labelcolor": LIGHT["ink2"],
            "figure.facecolor": "none",
            "axes.facecolor": "none",
            "savefig.facecolor": "none",
            "text.color": LIGHT["ink"],
        }
    )


def svg(fig, label: str) -> str:
    """Render a figure to inline SVG: no metadata, no namespaces/URLs, theme-aware colours."""
    buf = io.StringIO()
    fig.savefig(buf, format="svg", bbox_inches="tight", metadata={"Date": None, "Creator": None, "Format": None, "Type": None})
    s = buf.getvalue()
    s = s[s.index("<svg") :]
    s = re.sub(r"<metadata>.*?</metadata>", "", s, flags=re.S)
    s = re.sub(r'\s+xmlns(:\w+)?="[^"]*"', "", s)
    s = re.sub(r'\s+(width|height)="[^"]*pt"', "", s, count=2)
    s = re.sub(r"font-family:[^;\"]*", f"font-family: {FONT}", s)
    s = re.sub(r"font: (\d+(?:\.\d+)?px) '[^']*'", rf"font-size: \1; font-family: {FONT}", s)
    for name, hexval in LIGHT.items():
        s = s.replace(hexval, f"var(--{name})")
    return s.replace("<svg", f'<svg role="img" aria-label="{html.escape(label)}"', 1)


def _subplots(figsize):
    """Figure without pyplot: no backend switch, so notebooks keep their inline backend."""
    fig = Figure(figsize=figsize)
    return fig, fig.subplots()


def hbar(labels, values, title, xlabel="", fmt="{:.0f}", ref=None, ref_label="", color=None, height=None):
    labels, values = list(labels)[::-1], list(values)[::-1]
    fig, ax = _subplots(figsize=(6.2, height or max(1.6, 0.32 * len(labels) + 0.8)))
    bars = ax.barh(labels, values, color=color or LIGHT["s1"], height=0.62)
    ax.grid(axis="y", visible=False)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    top = max(values) if values else 1
    for b, v in zip(bars, values):
        ax.text(b.get_width() + top * 0.01, b.get_y() + b.get_height() / 2, fmt.format(v), va="center", fontsize=9, color=LIGHT["ink2"])
    if ref is not None:
        ax.axvline(ref, color=LIGHT["muted"], linestyle="--", linewidth=1)
        ax.text(ref, len(labels) - 0.4, f" {ref_label}", color=LIGHT["ink2"], fontsize=9, va="bottom")
    ax.set_xlim(0, top * 1.15)
    return fig


def vbar(labels, values, title, ylabel="", fmt="{:.0f}", ylim=None, color=None):
    fig, ax = _subplots(figsize=(6.2, 2.8))
    bars = ax.bar(list(labels), list(values), color=color or LIGHT["s1"], width=0.6)
    ax.grid(axis="x", visible=False)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    for b, v in zip(bars, values):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height(), fmt.format(v), ha="center", va="bottom", fontsize=9, color=LIGHT["ink2"])
    if ylim:
        ax.set_ylim(*ylim)
    return fig


def grouped_bars(table: pd.DataFrame, title, ylabel="", fmt="{:.0f}"):
    """Rows = x groups, columns = series (max 3, fixed palette order)."""
    fig, ax = _subplots(figsize=(6.2, 2.9))
    n = len(table.columns)
    width = 0.8 / n
    xs = range(len(table.index))
    for i, col in enumerate(table.columns):
        pos = [x - 0.4 + width * (i + 0.5) for x in xs]
        bars = ax.bar(pos, table[col].tolist(), width=width * 0.92, color=SERIES[i], label=str(col))
        for b, v in zip(bars, table[col].tolist()):
            ax.text(b.get_x() + b.get_width() / 2, b.get_height(), fmt.format(v), ha="center", va="bottom", fontsize=8, color=LIGHT["ink2"])
    ax.set_xticks(list(xs), [str(i) for i in table.index])
    ax.grid(axis="x", visible=False)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.legend(title=table.columns.name, ncols=n, loc="upper right", fontsize=9, title_fontsize=9)
    ax.set_ylim(0, table.to_numpy().max() * 1.4)
    return fig


# ---------------------------------------------------------------------------
# aggregates (everything the page shows; no per-employee rows leave here)
# ---------------------------------------------------------------------------

TIERS = ["Low", "Medium", "High"]
DRIVERS = ["hr_flag_high", "hr_flag_medium", "declining_rating", "low_rating", "no_recent_learning"]


def dashboard_data(gold: dict[str, pd.DataFrame]) -> dict:
    f = findings.compute_findings(gold)
    promo = gold["gold_promotion_readiness"]
    train = gold["gold_training_summary"]
    attr = gold["gold_attrition_risk"]
    dist = gold["gold_skill_distribution"]

    promo_dept = promo.groupby("department").agg(employees=("employee_id", "size"), high_potential=("is_high_potential", "sum"), ready=("is_promotion_ready", "sum"))
    promo_dept = promo_dept.sort_values("ready", ascending=False)

    events_by_type = {e: int(train[f"n_{e}"].sum()) for e in job.EVENT_TYPES}
    dept_training = train.groupby("department").agg(
        employees=("employee_id", "size"),
        events=("total_events", "sum"),
        avg_courses=("courses_completed", "mean"),
        no_course=("courses_completed", lambda s: (s == 0).sum()),
    )
    dept_training["pct_no_course"] = dept_training["no_course"] / dept_training["employees"] * 100

    tier_vs_flag = pd.crosstab(attr["computed_risk_tier"], attr["attrition_risk"]).reindex(index=TIERS, columns=TIERS, fill_value=0)
    tier_vs_flag.index.name, tier_vs_flag.columns.name = "Computed tier", "HR flag"
    drivers = {d: int(attr[d].sum()) for d in DRIVERS}

    by_comp = findings.weighted(dist, ["competency_name"], "pct_meeting_required").sort_values()
    dept_cat = findings.weighted(dist, ["department", "category"], "avg_proficiency").unstack("category")

    return {
        "findings": f,
        "promo_dept": promo_dept,
        "events_by_type": events_by_type,
        "dept_training": dept_training,
        "tier_vs_flag": tier_vs_flag,
        "drivers": drivers,
        "lowest_meeting": by_comp.head(8),
        "dept_cat": dept_cat,
    }


def quarantine_summary(lake: job.Lake, lineage: dict) -> dict[str, int] | None:
    key = f"{job.QUARANTINE}learning_activity/run_date={lineage.get('run_date')}/rejects.parquet"
    try:
        rej = lake.read_parquet(key)
    except Exception:
        return None
    return {k: int(v) for k, v in rej["error"].map(job.reject_reason).value_counts().items()}


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

e = html.escape


def kpi(value, label, sub="") -> str:
    return f'<div class="kpi"><div class="v">{e(str(value))}</div><div class="l">{e(label)}</div>{f"<div class=s>{e(sub)}</div>" if sub else ""}</div>'


def table(df: pd.DataFrame, fmt: dict[str, str] | None = None, index_label: str = "") -> str:
    fmt = fmt or {}
    head = "".join(f"<th>{e(str(c))}</th>" for c in [index_label or (df.index.name or ""), *df.columns])
    rows = []
    for idx in df.index:
        cells = "".join(f'<td class="n">{e(fmt.get(c, "{}").format(df.at[idx, c]))}</td>' for c in df.columns)
        rows.append(f"<tr><th>{e(str(idx))}</th>{cells}</tr>")
    return f'<div class="tw"><table><thead><tr>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'


def section(num: int, title: str, answer: str, body: str) -> str:
    return f'<section id="q{num}"><h2><span class="q">Q{num}</span> {e(title)}</h2><p class="answer">{e(answer)}</p>{body}</section>'


def charts(*svgs: str) -> str:
    return '<div class="charts">' + "".join(f"<figure>{s}</figure>" for s in svgs) + "</div>"


def render_sections(d: dict) -> str:
    f = d["findings"]
    r, g, p, t, a, tm, sk = (f[k] for k in ("ratings", "skill_gaps", "promotion", "training", "attrition", "teams", "skills"))
    periods = list(r["avg_by_period"])
    out = []

    out.append(
        section(
            1,
            "Ratings and review history",
            f"Average rating rose from {r['avg_by_period'][periods[0]]:.2f} ({periods[0]}) to {r['avg_by_period'][periods[-1]]:.2f} ({periods[-1]}); "
            f"{r['pct_improved']}% of employees improved and {r['pct_declined']}% declined.",
            '<div class="kpis">'
            + "".join(kpi(f"{v:.2f}", f"Avg rating {k}") for k, v in r["avg_by_period"].items())
            + kpi(f"{r['pct_improved']}%", "Improved", f"{r['n_improved']:,} employees")
            + kpi(f"{r['pct_same']}%", "Unchanged", f"{r['n_same']:,} employees")
            + kpi(f"{r['pct_declined']}%", "Declined", f"{r['n_declined']:,} employees")
            + "</div>"
            + charts(svg(hbar(["Improved", "Unchanged", "Declined"], [r["n_improved"], r["n_same"], r["n_declined"]], "Rating change between periods", "employees", "{:,}"), "Rating change counts")),
        )
    )

    sev = g["severity_counts"]
    top = g["top_competencies"]
    out.append(
        section(
            2,
            "Critical skill gaps",
            f"{g['critical']} of {g['assessments']:,} assessments ({g['pct_critical']}%) show a critical gap (required minus proficiency >= 2), "
            f"across {g['employees_with_critical']} employees. Most common: {top[0][0]} ({top[0][1]}), {top[1][0]} ({top[1][1]}).",
            '<div class="kpis">'
            + kpi(g["critical"], "Critical gaps", f"{g['pct_critical']}% of assessments")
            + kpi(g["employees_with_critical"], "Employees affected")
            + kpi(sev.get("minor", 0), "Minor gaps", "gap of 1 level")
            + "</div>"
            + charts(
                svg(vbar(["none", "minor", "critical"], [sev.get(k, 0) for k in ("none", "minor", "critical")], "Assessments by gap severity", "assessments", "{:,}"), "Gap severity"),
                svg(hbar([c for c, _ in top], [n for _, n in top], "Top 10 competencies by critical gaps", "critical gaps"), "Top competencies by critical gaps"),
            ),
        )
    )

    pdept = d["promo_dept"]
    out.append(
        section(
            3,
            "High potential and promotion-ready",
            f"{p['high_potential']} employees are rated high potential; {p['promotion_ready']} are promotion-ready "
            f"(high potential, 'Ready Now' and latest rating >= 4), and {p['ready_with_critical_gap']} of those still have a critical skill gap.",
            '<div class="kpis">'
            + kpi(p["high_potential"], "High potential")
            + kpi(p["promotion_ready"], "Promotion-ready")
            + kpi(p["ready_with_critical_gap"], "Ready, with a critical gap")
            + "</div>"
            + charts(svg(hbar(pdept.index, pdept["ready"], "Promotion-ready employees by department", "employees"), "Promotion-ready by department"))
            + table(pdept.rename(columns={"employees": "Employees", "high_potential": "High potential", "ready": "Promotion-ready"}), index_label="Department"),
        )
    )

    ev = d["events_by_type"]
    dt = d["dept_training"][["employees", "events", "avg_courses", "pct_no_course"]].rename(
        columns={"employees": "Employees", "events": "Events", "avg_courses": "Avg courses completed", "pct_no_course": "% no completed course"}
    )
    out.append(
        section(
            4,
            "Training completed",
            f"{t['events']:,} learning events; every employee has some activity ({t['employees_with_activity']:,} of {t['employees']:,}). "
            f"Employees completed {t['avg_courses_per_employee']:.2f} courses on average; {t['no_completed_course']} completed none.",
            '<div class="kpis">'
            + kpi(f"{t['events']:,}", "Learning events")
            + kpi(f"{t['avg_courses_per_employee']:.2f}", "Courses completed per employee")
            + kpi(t["no_completed_course"], "No completed course")
            + "</div>"
            + charts(svg(hbar([k.replace("_", " ") for k in ev], list(ev.values()), "Learning events by type", "events", "{:,}"), "Events by type"))
            + table(dt, {"Employees": "{:,}", "Events": "{:,}", "Avg courses completed": "{:.2f}", "% no completed course": "{:.1f}"}, "Department"),
        )
    )

    tiers = a["computed_tiers"]
    drv = d["drivers"]
    out.append(
        section(
            5,
            "Attrition risk from performance trends",
            f"The rule-based score puts {tiers['High']} employees in the High tier, {tiers['Medium']} Medium and {tiers['Low']:,} Low "
            f"(HR flags {a['hr_flag_high']} as High). {a['high_risk_and_high_potential']} employees are both high risk and high potential. "
            f"'Recent' learning is measured over 180 days before {a['as_of_date']}.",
            '<div class="kpis">'
            + "".join(kpi(f"{tiers[k]:,}", f"{k} risk (computed)") for k in ("High", "Medium", "Low"))
            + kpi(a["high_risk_and_high_potential"], "High risk + high potential")
            + "</div>"
            + charts(
                svg(grouped_bars(d["tier_vs_flag"], "Computed tier vs HR attrition flag", "employees"), "Computed risk tier versus HR flag"),
                svg(hbar([k.replace("_", " ") for k in drv], list(drv.values()), "Risk driver frequency", "employees", "{:,}"), "Risk driver frequency"),
            )
            + '<p class="note">risk_score = 2 x HR flag High + HR flag Medium + declining rating + latest rating &lt;= 2 + no learning in 180 days. '
            "Tiers: High &gt;= 4, Medium 2-3, Low 0-1.</p>",
        )
    )

    bands = tm["avg_rating_by_on_time_band"]
    counts = tm["teams_by_on_time_band"]
    band_labels = [f"{b} on time\n({counts[b]} teams)" for b in bands]
    out.append(
        section(
            6,
            "Performance vs team and project outcomes",
            f"Team ratings move modestly with project outcomes: r = {tm['pearson_r']} across {tm['teams_with_projects']} teams with projects. "
            f"Teams with no on-time projects average {bands.get('0%', float('nan')):.2f}; teams with more than half on time average {bands.get('51-100%', float('nan')):.2f}.",
            '<div class="kpis">'
            + kpi(f"{tm['pearson_r']:.2f}", "Pearson r", "team avg rating vs avg outcome score")
            + kpi(tm["teams_with_projects"], "Teams with projects")
            + "</div>"
            + charts(svg(vbar(band_labels, list(bands.values()), "Average team rating by share of projects on time", "avg rating (latest period)", "{:.2f}", ylim=(0, 5)), "Average team rating by on-time band")),
        )
    )

    low = d["lowest_meeting"]
    dc = d["dept_cat"]
    out.append(
        section(
            7,
            "Skill distribution",
            f"Strongest: {sk['strongest'][0]} / {sk['strongest'][1]} (avg proficiency {sk['strongest'][2]:.2f}); "
            f"weakest: {sk['weakest'][0]} / {sk['weakest'][1]} ({sk['weakest'][2]:.2f}). "
            f"{sk['lowest_pct_meeting'][0]} has the lowest share meeting the required level ({sk['lowest_pct_meeting'][1]:.0f}% vs {sk['overall_pct_meeting']:.0f}% overall).",
            charts(svg(hbar(low.index, low.values, "Lowest 8 competencies: % meeting required level", "% of assessments", "{:.1f}%", ref=sk["overall_pct_meeting"], ref_label=f"overall {sk['overall_pct_meeting']:.0f}%"), "Lowest share meeting required level"))
            + "<h3>Average proficiency by department and category</h3>"
            + table(dc, {c: "{:.2f}" for c in dc.columns}, "Department"),
        )
    )
    return "".join(out)


ARCHITECTURE = """\
generated CSVs -> Postgres 16 --COPY--> landing/postgres/<table>/ingest_date=YYYY-MM-DD/
generated JSONL ------------upload--> landing/learning_activity/year=/month=/day=/
landing -> bronze   immutable byte copies, never overwritten
bronze  -> silver   Pydantic validation, FK + duplicate checks, rejects -> _quarantine/
silver  -> gold     7 tables (one per question) + 9 reconciliation checks
lineage/            one JSON report per run (this page's header)
Storage: MinIO (S3 API). Compute: one job.py container (Docker Compose / GitHub Actions)."""


def render_footer(lineage: dict, dq: dict[str, int] | None) -> str:
    silver = [s for s in lineage.get("steps", []) if s["layer"] == "silver"]
    rows = "".join(
        f'<tr><th>{e(s["dataset"])}</th><td class="n">{s["rows_in"]:,}</td><td class="n">{s["rows_out"]:,}</td><td class="n">{s["rows_rejected"]:,}</td><td>{e(s.get("note") or "")}</td></tr>'
        for s in silver
    )
    dq_table = (
        f'<div class="tw"><table><thead><tr><th>Silver dataset</th><th>Rows in</th><th>Kept</th><th>Quarantined</th><th>Note</th></tr></thead><tbody>{rows}</tbody></table></div>'
        if rows
        else "<p>No silver step in this run.</p>"
    )
    reasons = "".join(f"<li>{e(k)}: {v:,}</li>" for k, v in (dq or {}).items())
    checks = "".join(
        f'<tr><td>{e(c["layer"])}</td><td>{e(c["check"])}</td><td class="n">{e(f"{c["expected"]:,}" if isinstance(c["expected"], int) else str(c["expected"]))}</td>'
        f'<td class="n">{e(f"{c["actual"]:,}" if isinstance(c["actual"], int) else str(c["actual"]))}</td><td>{"pass" if c["passed"] else "FAIL"}</td></tr>'
        for c in lineage.get("reconciliation", [])
    )
    return (
        '<footer><h2>Data quality</h2>'
        + dq_table
        + (f"<h3>Quarantined learning events by reason</h3><ul>{reasons}</ul>" if reasons else "")
        + "<p>Pre-hire reviews and assessments are flagged (<code>dq_before_hire</code>) and kept, not dropped; schema-drift rows (<code>notes</code> instead of <code>text</code>) are mapped and kept.</p>"
        + (f'<h3>Reconciliation</h3><div class="tw"><table><thead><tr><th>Layer</th><th>Check</th><th>Expected</th><th>Actual</th><th>Result</th></tr></thead><tbody>{checks}</tbody></table></div>' if checks else "")
        + f"<h2>Architecture</h2><pre>{e(ARCHITECTURE)}</pre>"
        + "<p class=\"note\">Synthetic data. Figures are aggregates; no individual employee data is published on this page.</p></footer>"
    )


CSS = """
:root{color-scheme:light;--bg:#fcfcfb;--card:#ffffff;--ink:#0b0b0b;--ink2:#52514e;--grid:#e4e3df;--muted:#a6a59f;
--s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;--ok:#0b7a0b;--bad:#d03b3b;--badbg:#fdecec}
@media (prefers-color-scheme:dark){:root:not([data-theme=light]){color-scheme:dark;--bg:#1a1a19;--card:#232322;--ink:#f0efec;--ink2:#c3c2b7;
--grid:#383835;--muted:#6b6a64;--s1:#3987e5;--s2:#d95926;--s3:#199e70;--ok:#4cc24c;--bad:#e66767;--badbg:#3a1f1f}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 system-ui,-apple-system,'Segoe UI',Roboto,sans-serif}
main,header .in,footer{max-width:1100px;margin:0 auto;padding:0 16px}
header{border-bottom:1px solid var(--grid);background:var(--card)}header .in{padding:20px 16px}
h1{font-size:24px;margin:0 0 4px}h2{font-size:19px;margin:32px 0 6px}h3{font-size:15px;margin:18px 0 6px}
.meta{display:flex;flex-wrap:wrap;gap:6px 18px;color:var(--ink2);font-size:13px}.meta b{color:var(--ink)}
.ok{color:var(--ok);font-weight:600}.bad{color:var(--bad);font-weight:600}
.banner{background:var(--badbg);border:1px solid var(--bad);border-radius:8px;padding:10px 14px;margin:16px 0}
nav{font-size:13px;margin-top:10px}nav a{color:var(--s1);margin-right:12px;text-decoration:none}
.q{display:inline-block;background:var(--s1);color:#fff;border-radius:6px;padding:0 7px;font-size:13px;vertical-align:2px}
.answer{font-size:16px;margin:4px 0 14px;max-width:80ch}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:10px 0}
.kpi{background:var(--card);border:1px solid var(--grid);border-radius:8px;padding:10px 12px}
.kpi .v{font-size:24px;font-weight:650;font-variant-numeric:tabular-nums}.kpi .l{font-size:13px;color:var(--ink2)}.kpi .s{font-size:12px;color:var(--ink2)}
.charts{display:grid;grid-template-columns:repeat(auto-fill,minmax(min(100%,460px),1fr));gap:12px}
figure{margin:0;background:var(--card);border:1px solid var(--grid);border-radius:8px;padding:10px}figure svg{width:100%;height:auto;display:block}
.tw{overflow-x:auto}table{border-collapse:collapse;font-size:13px;margin:8px 0;min-width:50%}
th,td{border-bottom:1px solid var(--grid);padding:5px 10px;text-align:left}td.n{text-align:right;font-variant-numeric:tabular-nums}
thead th{color:var(--ink2);font-weight:600}
pre{background:var(--card);border:1px solid var(--grid);border-radius:8px;padding:12px;overflow-x:auto;font-size:12.5px}
.note{color:var(--ink2);font-size:13px}footer{padding-bottom:40px}
"""


def render_dashboard(lineage: dict, gold: dict[str, pd.DataFrame] | None, dq: dict[str, int] | None = None, insights: bool = False, site_data: bool = False) -> str:
    apply_style()
    status = str(lineage.get("status", "UNKNOWN"))
    rec = lineage.get("reconciliation", [])
    passed = sum(bool(c.get("passed")) for c in rec)
    ok = status == "SUCCEEDED"
    ex = lineage.get("execution", {})
    meta = [
        ("Run", lineage.get("run_id", "?")),
        ("Status", status),
        ("Reconciliation", f"{passed}/{len(rec)} passed"),
        ("Duration", f"{lineage.get('duration_s', 0):.1f} s"),
        ("Environment", ex.get("environment", "?")),
        ("Run date", lineage.get("run_date", "?")),
        ("Finished", str(lineage.get("ended_at", "?"))[:19].replace("T", " ") + " UTC"),
    ]
    meta_html = "".join(f'<span>{e(k)}: <b class="{("ok" if ok else "bad") if k == "Status" else ""}">{e(str(v))}</b></span>' for k, v in meta)
    layers = lineage.get("layers") or {}
    if layers:
        meta_html += '<span class="layers">Layers: ' + " &middot; ".join(
            f'{e(name)} <b class="{"ok" if r.get("status") == "SUCCEEDED" else "bad"}">{e(str(r.get("status")))}</b> ({e(str(r.get("run_id")))})'
            for name, r in layers.items()
        ) + "</span>"
    for w in lineage.get("warnings") or []:
        meta_html += f'<span class="bad">Warning: {e(w)}</span>'

    banner = ""
    if not ok:
        banner = (
            f'<div class="banner" role="alert"><b class="bad">Run {e(status)}</b>: {e(str(lineage.get("error") or "no error recorded"))}. '
            "Figures below come from the Gold tables currently in the lake and may be stale or missing.</div>"
        )
    if gold is None:
        body = '<div class="banner" role="alert"><b class="bad">Gold tables unavailable</b>: no figures to show for this run.</div>'
    else:
        body = render_sections(dashboard_data(gold))

    nav = (
        "".join(f'<a href="#q{i}">Q{i}</a>' for i in range(1, 8))
        + ('<a href="insights.html">Notebook</a>' if insights else "")
        + '<a href="lineage.json">Lineage JSON</a>'
        + ('<a href="findings.json">Findings JSON</a>' if site_data else "")
    )
    return (
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
        "<title>ACME Talent Insights</title><meta name=\"description\" content=\"Performance and talent analytics from the ACME Talent ETL pipeline (synthetic data).\">"
        f"<style>{CSS}</style></head><body>"
        f'<header><div class="in"><h1>ACME Talent Insights</h1><div class="meta">{meta_html}</div><nav>{nav}</nav></div></header>'
        f"<main>{banner}{body}</main>{render_footer(lineage, dq)}</body></html>"
    )


def sanitize_html(text: str) -> str:
    """Make an nbconvert export self-contained: no scripts, no external resources, no URLs in comments."""
    text = re.sub(r"<script\b[^>]*>.*?</script>", "", text, flags=re.S | re.I)
    text = re.sub(r"<link\b[^>]*href=[\"']https?://[^>]*>", "", text, flags=re.I)
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
    text = re.sub(r"<style\b[^>]*>.*?</style>", lambda m: re.sub(r"/\*.*?\*/", "", m.group(0), flags=re.S), text, flags=re.S | re.I)
    text = re.sub(r"<\?xml[^>]*\?>|<!DOCTYPE svg[^>]*>", "", text, flags=re.I)
    return re.sub(r'\s+xmlns(:\w+)?="[^"]*"', "", text)


def build_site(lake: job.Lake, out_dir: Path) -> Path:
    lineage = lake.read_json("lineage/latest.json")
    try:
        gold = job.read_gold(lake)
    except Exception:
        gold = None
    out_dir.mkdir(parents=True, exist_ok=True)
    insights = out_dir / "insights.html"
    if insights.exists():
        insights.write_text(sanitize_html(insights.read_text(encoding="utf-8")), encoding="utf-8")
    site_data = lake.exists(job.GOLD_SITE_DATA)  # aggregates written by the Gold layer
    if site_data:
        (out_dir / "findings.json").write_bytes(lake.get(job.GOLD_SITE_DATA))
    page = render_dashboard(lineage, gold, quarantine_summary(lake, lineage), insights=insights.exists(), site_data=site_data)
    (out_dir / "index.html").write_text(page, encoding="utf-8")
    (out_dir / "lineage.json").write_text(json.dumps(lineage, indent=2, default=str), encoding="utf-8")
    return out_dir / "index.html"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", default="site", help="output directory (default: site)")
    args = ap.parse_args(argv)
    settings = job.Settings.from_env()
    path = build_site(job.Lake.from_settings(settings), Path(args.out))
    print(f"wrote {path} ({path.stat().st_size / 1024:.0f} KB)" + (" + insights.html" if (path.parent / "insights.html").exists() else ""))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
