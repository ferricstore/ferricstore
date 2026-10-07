"""Keep frozen candidate comparisons separate from exploratory source cohorts."""
import json
from collections import Counter
from pathlib import Path

root = Path(__file__).resolve().parent.parent
results = root / "bench/results"

def load(name):
    return json.loads((results / name).read_text())

def histogram(report, operation):
    windows = [w for w in report["windows"] if w["operation"] == operation]
    bins = Counter()
    for window in windows:
        bins.update({int(us): n for us, n in window["buckets"].items()})
    count = sum(bins.values())
    output = {"count": count, "max_us": max(w["max_us"] for w in windows)}
    for name, q in (("p50_us", .5), ("p95_us", .95), ("p99_us", .99), ("p999_us", .999)):
        seen = 0
        for us, n in sorted(bins.items()):
            seen += n
            if seen >= count * q:
                output[name] = us
                break
    return output

summary = {"accepted": False, "retained": "durably ordered old-log cleanup correctness fix",
           "saturated": {}, "offered": {}}
saturated = {mode: load(f"separate-output-frozen-saturated-{mode}-1.json") for mode in ("whole", "separate")}
assert saturated["whole"]["publication_identity"] == saturated["separate"]["publication_identity"]
for mode, report in saturated.items():
    assert report["errors"] == 0 and report["seconds"] == 120 and not report["hset_coalescing_enabled"]
    assert report["compaction_latch"] == mode and not report["after_quiet_events"]
    events = [event for event in report["events"] if event["event"] == "compaction"]
    assert len(events) == 8 and all(event["layout"] == mode for event in events)
    assert not any(event["event"] == "compaction_failed" for event in report["events"])
    operations = {op: histogram(report, op) for op in ("hash_write", "hash_read", "kv_read")}
    summary["saturated"][mode] = {"operations": operations, "compactions": len(events),
        "ops_per_second": sum(stat["count"] for stat in operations.values()) / report["measured_elapsed_seconds"]}
offered = {mode: load(f"separate-output-frozen-offered-{mode}-1.json") for mode in ("whole", "separate")}
assert offered["whole"]["publication_identity"] == offered["separate"]["publication_identity"]
for mode, report in offered.items():
    assert report["offered"] == 12000 and report["completed"] == 12000 and report["dropped"] == 0
    assert report["compactions"] == 8 and report["quiet_compactions"] == 0 and report["errors"] == 0
    summary["offered"][mode] = {key: report[key] for key in ("histograms", "completed_in_window", "compactions")}
summary["warning"] = "These measure the rejected candidate before the retained cleanup repair; no production latency gain is claimed."
(results / "separate-output-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
for mode in ("whole", "separate"):
    print(mode, "saturated", summary["saturated"][mode]["operations"]["hash_write"],
          "offered", summary["offered"][mode]["histograms"]["write_total_us"])
