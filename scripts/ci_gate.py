"""CI gate: summarise a pipeline view (site/lineage.json) and decide whether the site may be deployed.

Deployable only when the pipeline is SUCCEEDED and all 9 reconciliation checks passed
(1 bronze + 8 gold). Writes `deployable=true|false` to $GITHUB_OUTPUT and a summary line to
$GITHUB_STEP_SUMMARY when those are set; always exits 0 (the deploy job reads the output).

Usage: python scripts/ci_gate.py site/lineage.json
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

EXPECTED_CHECKS = 9


def evaluate(view: dict | None) -> tuple[bool, str]:
    if not view:
        return False, "NOT deployable: no lineage found (the gold layer did not produce a site)"
    checks = view.get("reconciliation") or []
    passed = sum(bool(c.get("passed")) for c in checks)
    layers = ", ".join(f"{name}: {r.get('status')}" for name, r in (view.get("layers") or {}).items())
    status = view.get("status")
    ok = status == "SUCCEEDED" and len(checks) == EXPECTED_CHECKS and passed == EXPECTED_CHECKS
    line = f"pipeline {status}, checks {passed}/{len(checks)} (need {EXPECTED_CHECKS}/{EXPECTED_CHECKS}); layers: {layers or 'none'}"
    if view.get("error"):
        line += f"; {view['error']}"
    return ok, f"{'deployable' if ok else 'NOT deployable'}: {line}"


def main(argv: list[str]) -> int:
    path = Path(argv[0] if argv else "site/lineage.json")
    view = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
    ok, line = evaluate(view)
    print(line)
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
            f.write(f"deployable={'true' if ok else 'false'}\n")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(f"### Pipeline\n\n{line}\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv[1:]))
