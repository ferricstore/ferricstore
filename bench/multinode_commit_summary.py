"""Compare the published dependency, deadline fix, and propagation experiment."""
import json
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
FILES = {
    "control": "multinode-commit-deadline-control.json",
    "deadline": "multinode-commit-deadline-candidate.json",
    "deadline_and_propagation": "multinode-commit-propagation-candidate.json",
    "propagation_leader_targeted": "multinode-commit-propagation-leader.json",
}
summary = {}
for name, filename in FILES.items():
    data = json.loads((ROOT / "bench/results" / filename).read_text())
    summary[name] = {}
    for count in sorted({row["nodes"] for row in data["results"]}):
        rows = [row for row in data["results"] if row["nodes"] == count]
        values = {"ops_per_second": [r["ops_per_second"] for r in rows]}
        for metric in ("p50_us", "p95_us", "p99_us", "max_us"):
            values["write_" + metric] = [r["operations"]["write"][metric] for r in rows]
        summary[name][str(count)] = {metric: {"median": median(v), "min": min(v), "max": max(v)}
                                    for metric, v in values.items()}
print(json.dumps(summary, indent=2))
(ROOT / "bench/results/multinode-commit-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
