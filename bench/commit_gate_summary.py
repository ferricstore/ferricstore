"""Release gate summary; only the compiler-aligned OTP 29 trials are included."""
import json
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
rows = [r for path in sorted((ROOT / "bench/results").glob("commit-otp29-*.json"))
        for r in json.loads(path.read_text())["results"]]
if len({r["server_compiler"] for r in rows}) != 1:
    raise ValueError("gate trials used different compiler versions")
summary = {"server_compiler": rows[0]["server_compiler"], "scenarios": {}}
for nodes, expected_trials in ((1, 5), (3, 3)):
    scenario = {}
    for variant in ("control", "deadline"):
        selected = [r for r in rows if r["nodes"] == nodes and r["dependency_variant"] == variant]
        if len(selected) != expected_trials:
            raise ValueError(f"expected {expected_trials} trials for {nodes}/{variant}")
        for r in selected:
            if r["errors"] != 0 or r["verified_replicas"] != nodes:
                raise ValueError("a gate trial had errors or incomplete replica verification")
        values = {"ops_per_second": [r["ops_per_second"] for r in selected]}
        for q in ("p50_us", "p95_us", "p99_us"):
            values["write_" + q] = [r["operations"]["write"][q] for r in selected]
        scenario[variant] = {name: {"median": median(v), "min": min(v), "max": max(v)}
                             for name, v in values.items()}
    scenario["median_change_pct"] = {
        name: 100 * (scenario["deadline"][name]["median"] / scenario["control"][name]["median"] - 1)
        for name in scenario["control"]}
    summary["scenarios"][str(nodes)] = scenario
print(json.dumps(summary, indent=2))
(ROOT / "bench/results/commit-release-gate-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
