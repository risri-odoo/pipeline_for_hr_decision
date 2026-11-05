"""Headline findings (spec section 7) from the Gold tables, printed as JSON.

The logic lives in job.py (the Gold layer also writes it to gold/_site/findings.json);
this module re-exports it for the dashboard, the notebook and the tests.
Usage: python scripts/findings.py   (reads Gold + lineage from the lake configured by env)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import job  # noqa: E402
from job import ON_TIME_BANDS, compute_findings, on_time_band, run_summary, weighted  # noqa: E402,F401


def main() -> int:
    settings = job.Settings.from_env()
    lake = job.Lake.from_settings(settings)
    out = {"run": run_summary(lake.read_json("lineage/latest.json")), **compute_findings(job.read_gold(lake))}
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
