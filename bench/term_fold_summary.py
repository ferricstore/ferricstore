"""Validate and summarize the unprofiled, matched heartbeat term-fold gate."""

import json
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"
rows = [row for path in sorted(RESULTS.glob("term-fold-gate-*.json"))
        for row in json.loads(path.read_text())["results"]]
if len(rows) != 24 or {row["server_compiler"] for row in rows} != {"10.0.3"}:
    raise ValueError("expected 24 compiler-aligned scenario trials")
for row in rows:
    if (row["errors"] or row["verified_replicas"] != row["nodes"] or row["profile"]
            or row["heartbeat_ms"] != 120 or row["requested_seconds"] != 15
            or row["warmup_seconds"] != 3 or row["clients"] != 8 or row["shards"] != 4):
        raise ValueError("trial failed validation or used unmatched settings")
for field, axis in (("server_beam_md5", "dependency_variant"),
                    ("segment_provider_beam_md5", "segment_terms_variant")):
    for variant in {row[axis] for row in rows}:
        if len({row[field] for row in rows if row[axis] == variant}) != 1:
            raise ValueError("trial source checksums differ within a variant")

summary = {"aggregation": "medians and ranges of trial metrics, not pooled percentiles",
           "server_compiler": "10.0.3", "scenarios": {}}
for dependency in ("installed", "deadline"):
    for nodes in (1, 3):
        scenario = {}
        for variant in ("baseline", "bounded"):
            selected = [row for row in rows if row["dependency_variant"] == dependency
                        and row["nodes"] == nodes and row["segment_terms_variant"] == variant]
            if len(selected) != 3 or {row["trial_label"] for row in selected} != {"1", "2", "3"}:
                raise ValueError("expected three distinct trials per scenario/variant")
            values = {"ops_per_second": [row["ops_per_second"] for row in selected]}
            for kind in ("write", "read"):
                for q in ("p50_us", "p95_us", "p99_us"):
                    values[f"{kind}_{q}"] = [row["operations"][kind][q] for row in selected]
            scenario[variant] = {key: {"median": median(v), "min": min(v), "max": max(v)}
                                 for key, v in values.items()}
        scenario["median_change_pct"] = {
            key: 100 * (scenario["bounded"][key]["median"] / scenario["baseline"][key]["median"] - 1)
            for key in scenario["baseline"]}
        summary["scenarios"][f"{dependency}/{nodes}"] = scenario
(RESULTS / "term-fold-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
