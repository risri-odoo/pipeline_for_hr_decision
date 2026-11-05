import json

import pytest

import ci_gate

CHECKS = [{"passed": True}] * 9


@pytest.mark.parametrize(
    "view,ok",
    [
        ({"status": "SUCCEEDED", "reconciliation": CHECKS}, True),
        ({"status": "SUCCEEDED", "reconciliation": CHECKS[:8]}, False),  # a layer's checks missing
        ({"status": "SUCCEEDED", "reconciliation": CHECKS[:8] + [{"passed": False}]}, False),
        ({"status": "STALE", "reconciliation": CHECKS, "error": "rerun silver"}, False),
        ({"status": "FAILED", "reconciliation": []}, False),
        (None, False),
    ],
)
def test_evaluate(view, ok):
    assert ci_gate.evaluate(view)[0] is ok


def test_main_writes_github_outputs(tmp_path, monkeypatch, capsys):
    view = {"status": "STALE", "error": "rerun silver", "reconciliation": CHECKS, "layers": {"gold": {"status": "SUCCEEDED"}}}
    (tmp_path / "lineage.json").write_text(json.dumps(view), encoding="utf-8")
    out, summary = tmp_path / "out", tmp_path / "summary"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert ci_gate.main([str(tmp_path / "lineage.json")]) == 0
    assert out.read_text() == "deployable=false\n"
    assert "NOT deployable: pipeline STALE, checks 9/9" in summary.read_text() and "gold: SUCCEEDED" in capsys.readouterr().out


def test_main_without_lineage_or_ci_env(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    assert ci_gate.main([str(tmp_path / "missing.json")]) == 0
    assert "NOT deployable: no lineage found" in capsys.readouterr().out
