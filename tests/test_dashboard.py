import copy
import json
import re
from pathlib import Path

import nbformat
import pytest

import build_dashboard as bd
import job
from conftest import MANIFEST
from fakes import MemoryLake

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def page(sample_run):
    lake = sample_run["lake"]
    return bd.render_dashboard(sample_run["lineage"], sample_run["gold"], bd.quarantine_summary(lake, sample_run["lineage"]), insights=True)


def test_shows_run_metadata(page, sample_run):
    lin = sample_run["lineage"]
    assert lin["run_id"] in page
    assert 'class="ok">SUCCEEDED<' in page
    assert "9/9 passed" in page
    assert f"{lin['duration_s']:.1f} s" in page
    assert ">pytest<" in page
    assert 'role="alert"' not in page


def test_shows_all_7_questions_with_headline_numbers(page, sample_run):
    for i in range(1, 8):
        assert f'<section id="q{i}">' in page and f'href="#q{i}"' in page
    f = job.compute_findings(sample_run["gold"])
    r, sk = f["ratings"], f["skills"]
    facts = [f"{v:.2f}" for v in r["avg_by_period"].values()] + [f"{r['pct_improved']}%", f"{f['training']['events']:,}", f"{f['attrition']['computed_tiers']['Low']:,}"]
    facts += [str(v) for v in (f["skill_gaps"]["critical"], f["skill_gaps"]["employees_with_critical"], f["promotion"]["high_potential"], f["promotion"]["promotion_ready"], f["training"]["no_completed_course"])]
    facts += [f"{f['teams']['pearson_r']:.2f}", sk["lowest_pct_meeting"][0], f"{sk['strongest'][0]} / {sk['strongest'][1]}"]
    for fact in facts:
        assert fact in page, fact
    assert page.count("<svg") == 9


def test_footer_has_dq_summary_and_architecture(page):
    assert "Quarantined learning events by reason" in page
    for reason, count in MANIFEST["quarantine_by_reason"].items():
        assert f"{reason}: {count}" in page
    assert "<h2>Reconciliation" not in page and "<h3>Reconciliation</h3>" in page
    assert "bronze rows == landing rows (postgres)" in page and f"{sum(MANIFEST['rows'].values()):,}" in page
    assert "<h2>Architecture</h2>" in page and "_quarantine/" in page


def test_aggregate_only(page, sample_run):
    emp = job.read_silver(sample_run["lake"])["employees"]
    names = set(emp["first_name"] + " " + emp["last_name"])
    leaked = [n for n in names if n in page]
    assert not leaked, leaked[:5]
    assert not re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", page), "email address in page"
    assert "<th>employee_id" not in page and "employee_name" not in page and "<th>Employee</th>" not in page
    assert not re.search(r"EV\d{10}", page)


def test_self_contained(page):
    lower = page.lower()
    for bad in ("<script", "<link", "<iframe", "src=", "http://", "https://", "javascript:", "@import"):
        assert bad not in lower, bad
    assert set(re.findall(r'href="([^"]+)"', page)) == {f"#q{i}" for i in range(1, 8)} | {"insights.html", "lineage.json"}


def test_charts_follow_theme_colours(page):
    for hexval in bd.LIGHT.values():
        assert hexval not in page.split("<style>")[1].split("</style>")[1], hexval
    assert "var(--s1)" in page and "prefers-color-scheme:dark" in page


def test_failed_run_flagged_without_gold():
    lin = {"run_id": "r-1", "status": "FAILED", "error": "RuntimeError('boom <x>')", "duration_s": 1.0, "execution": {"environment": "ci"}, "steps": [], "reconciliation": []}
    page = bd.render_dashboard(lin, None)
    assert 'class="bad">FAILED<' in page and "Run FAILED" in page
    assert "boom &lt;x&gt;" in page and "boom <x>" not in page
    assert "Gold tables unavailable" in page and "0/0 passed" in page
    assert "No silver step in this run." in page and '<section id="q1">' not in page


def test_failed_run_with_stale_gold_still_flagged(sample_run):
    lin = copy.deepcopy(sample_run["lineage"])
    lin.update(status="FAILED", error=None)
    lin["reconciliation"][1].update(passed=False, actual=3999.5)
    page = bd.render_dashboard(lin, sample_run["gold"])
    assert "no error recorded" in page and "may be stale" in page
    assert "8/9 passed" in page and ">FAIL<" in page and "3999.5" in page
    assert '<section id="q7">' in page


def test_build_site_writes_files_and_sanitises_notebook(sample_run, tmp_path):
    (tmp_path / "insights.html").write_text('<html><head><script src="https://cdn.example/x.js"></script></head><body><p>ok</p></body></html>', encoding="utf-8")
    index = bd.build_site(sample_run["lake"], tmp_path)
    assert index == tmp_path / "index.html" and 'href="insights.html"' in index.read_text(encoding="utf-8")
    assert json.loads((tmp_path / "lineage.json").read_text(encoding="utf-8"))["run_id"] == sample_run["lineage"]["run_id"]
    nb = (tmp_path / "insights.html").read_text(encoding="utf-8")
    assert "<script" not in nb and "<p>ok</p>" in nb


def test_build_site_without_gold_or_notebook(tmp_path):
    lake = MemoryLake()
    lake.write_json("lineage/latest.json", {"run_id": "r", "run_date": "2026-10-15", "status": "FAILED", "error": "x", "steps": [], "reconciliation": []})
    out = tmp_path / "site"
    page = bd.build_site(lake, out).read_text(encoding="utf-8")
    assert "Gold tables unavailable" in page and "insights.html" not in page
    assert bd.quarantine_summary(lake, {"run_date": "2026-10-15"}) is None


def test_main(sample_run, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("LAKE_BUCKET", "test-lake")
    monkeypatch.setattr(job.Lake, "from_settings", classmethod(lambda cls, s: sample_run["lake"]))
    assert bd.main(["--out", str(tmp_path)]) == 0
    assert "wrote" in capsys.readouterr().out and (tmp_path / "index.html").exists()


def test_sanitize_html():
    raw = (
        '<?xml version="1.0"?><!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/x.dtd">'
        '<link rel="stylesheet" href="https://cdn.example/a.css"><link rel="icon" href="data:,">'
        '<style>/* see https://example.com */ .a{color:red}</style><!-- https://example.com -->'
        '<script type="text/x-mathjax-config">x</script><SCRIPT src="https://e/x.js"></SCRIPT>'
        '<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink"><use xlink:href="#p1"/></svg>'
    )
    out = bd.sanitize_html(raw)
    assert "http" not in out and "<script" not in out.lower()
    assert ".a{color:red}" in out and 'href="data:,"' in out and 'xlink:href="#p1"' in out


def test_svg_has_label_and_no_metadata():
    bd.apply_style()
    s = bd.svg(bd.vbar(["a", "b"], [1, 2], "t"), 'chart "x" & y')
    assert s.startswith('<svg role="img" aria-label="chart &quot;x&quot; &amp; y"')
    assert "<metadata" not in s and "xmlns" not in s and "pt\"" not in s.split(">")[0]


# ---------------------------------------------------------------------------
# notebook structure (execution is covered by the integration test)
# ---------------------------------------------------------------------------


def test_notebook_structure():
    nb = nbformat.read(ROOT / "notebooks" / "insights.ipynb", as_version=4)
    nbformat.validate(nb)
    code = "\n".join(c.source for c in nb.cells if c.cell_type == "code")
    text = "\n".join(c.source for c in nb.cells if c.cell_type == "markdown")
    assert code.count("plt.show()") == 12
    assert "def review_history(employee_id" in code and "def training_history(employee_id" in code
    assert "## Assumptions and limitations" in text
    assert all(not c.get("outputs") for c in nb.cells if c.cell_type == "code"), "commit the notebook without outputs"
    assert "first_name" not in code and "email" not in code


def test_layers_and_stale_warning_in_header():
    lin = {
        "run_id": "r-gold", "status": "STALE", "error": "silver run s1 was built from bronze run b1, but the latest bronze run is b2; rerun silver",
        "warnings": ["gold is using older silver data"], "duration_s": 3.0, "execution": {"environment": "ci"}, "steps": [], "reconciliation": [],
        "layers": {"bronze": {"run_id": "b2", "status": "SUCCEEDED"}, "silver": {"run_id": "s1", "status": "SUCCEEDED"}, "gold": {"run_id": "g1", "status": "FAILED"}},
    }
    page = bd.render_dashboard(lin, None)
    assert "Layers: bronze <b class=\"ok\">SUCCEEDED</b> (b2)" in page and "gold <b class=\"bad\">FAILED</b> (g1)" in page
    assert "Warning: gold is using older silver data" in page
    assert "Run STALE" in page and "rerun silver" in page
