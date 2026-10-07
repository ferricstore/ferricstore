"""Validate and aggregate the unprofiled promoted cached-read gate."""
import json
import sys
from collections import Counter
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"
trials = []
sources = {}
checksums = {}
cohort = sys.argv[1] if len(sys.argv) > 1 else "gate"
prefix = sys.argv[2] if len(sys.argv) > 2 else f"hash-read-{cohort}"
if cohort not in ("gate", "paced", "publication", "protected", "protected-paced"):
    raise ValueError("expected gate, paced, publication, protected, or protected-paced")
paced = cohort in ("paced", "protected-paced")
variants = ("baseline", "protected") if cohort in ("publication", "protected", "protected-paced") else ("baseline", "cached")
trial_count = 2 if cohort == "publication" else 3
final_source = len(sys.argv) > 3 and sys.argv[3] == "final"
trial_labels = ["final"] if final_source else range(1, trial_count + 1)
if final_source:
    trial_count = 1
identity = None
for variant in variants:
    for trial in trial_labels:
        report = json.loads((RESULTS / f"{prefix}-{variant}-{trial}.json").read_text())
        if (report["errors"] or report["promoted_read_variant"] != variant or report["trial"] != str(trial)
                or report["mode"] != "auto" or "profile" in report or report["seconds"] != (60 if paced else 120)
                or report.get("cycle_period_ms", 0) != (100 if paced else 0)
                or report["clients"] != 16 or report["fields_per_hash"] != 4096):
            raise ValueError("invalid or unmatched gate trial")
        if sources.setdefault(variant, report["router_source"]) != report["router_source"]:
            raise ValueError("router source changed within a variant")
        if checksums.setdefault(variant, report["router_beam_md5"]) != report["router_beam_md5"]:
            raise ValueError("router checksum changed within a variant")
        if cohort in ("protected", "protected-paced"):
            current_identity = report["publication_identity"]
            if identity is None:
                identity = current_identity
            if current_identity != identity:
                raise ValueError("publication source, compiled modules, or runtime changed")
        operations = {}
        for kind in ("hash_write", "hash_read", "kv_read"):
            windows = [window for window in report["windows"] if window["operation"] == kind]
            buckets = Counter()
            for window in windows:
                buckets.update({int(bucket): count for bucket, count in window["buckets"].items()})
            count = sum(buckets.values())
            metrics = {"count": count, "max_us": max(window["max_us"] for window in windows)}
            for q in (0.5, 0.95, 0.99):
                seen = 0
                for value, n in sorted(buckets.items()):
                    seen += n
                    if seen >= count * q:
                        metrics[f"p{int(q * 100)}_us"] = value
                        break
            operations[kind] = metrics
        trials.append({"variant": variant, "trial": trial, "operations": operations,
            "ops_per_second": sum(item["count"] for item in operations.values()) / report["measured_elapsed_seconds"],
            "compactions": sum(event["event"] == "compaction" for event in report["events"]),
            "compaction_failures": sum(event["event"] == "compaction_failed" for event in report["events"])})
summary = {"cohort": cohort, "aggregation": f"per-trial histogram percentiles, then medians/ranges across {trial_count} trials",
            "trials": trials, "variants": {}, "router_checksums": checksums}
if identity is not None:
    summary["publication_identity"] = identity
for variant in variants:
    rows = [trial for trial in trials if trial["variant"] == variant]
    values = {"ops_per_second": [row["ops_per_second"] for row in rows]}
    for kind in ("hash_write", "hash_read", "kv_read"):
        for metric in ("p50_us", "p95_us", "p99_us", "max_us"):
            values[f"{kind}_{metric}"] = [row["operations"][kind][metric] for row in rows]
    summary["variants"][variant] = {metric: {"median": median(values), "min": min(values), "max": max(values)}
                                    for metric, values in values.items()}
output = f"{prefix}-summary.json" if len(sys.argv) > 2 else ("hash-read-summary.json" if cohort == "gate" else "hash-read-paced-summary.json")
if final_source:
    output = f"{prefix}-final-summary.json"
(RESULTS / output).write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
