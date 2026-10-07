"""Summarize separate yielding cohorts and verify restoration to the prior production source."""
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"

def load(name):
    return json.loads((RESULTS / name).read_text())

def histogram(report, operation):
    windows = [w for w in report["windows"] if w["operation"] == operation]
    bins = Counter()
    for w in windows:
        bins.update({int(us): n for us, n in w["buckets"].items()})
    count = sum(bins.values())
    out = {"count": count, "max_us": max(w["max_us"] for w in windows)}
    for name, q in (("p50_us", .5), ("p95_us", .95), ("p99_us", .99), ("p999_us", .999)):
        seen = 0
        for us, n in sorted(bins.items()):
            seen += n
            if seen >= count * q:
                out[name] = us
                break
    return out

summary = {"accepted": False, "reason": "yielding worsened saturated write p99",
           "production_latch_policy": "whole", "cohorts": {}}
for cohort in ("pilot", "four-page"):
    reports = {mode: load(f"compaction-latch-{cohort}-saturated-{mode}-1.json") for mode in ("whole", "pages")}
    assert reports["whole"]["publication_identity"] == reports["pages"]["publication_identity"]
    assert reports["whole"]["promoted_beam_md5"] == reports["pages"]["promoted_beam_md5"]
    rows = {}
    for mode, report in reports.items():
        assert report["errors"] == 0 and report["hset_coalescing_enabled"] is False
        assert report["compaction_latch"] == mode and report["seconds"] == 120
        assert all(not s["active"] and not s["pending"] and not s["retries"] for s in report["compaction_status"])
        assert not report["after_quiet_events"]
        operations = {op: histogram(report, op) for op in ("hash_write", "hash_read", "kv_read")}
        rows[mode] = {"operations": operations,
                      "ops_per_second": sum(s["count"] for s in operations.values()) / report["measured_elapsed_seconds"],
                      "compactions": sum(e["event"] == "compaction" for e in report["events"]),
                      "compaction_failures": sum(e["event"] == "compaction_failed" for e in report["events"])}
    summary["cohorts"][cohort] = rows

summary["offered_pilot"] = {}
for mode in ("whole", "pages"):
    report = load(f"compaction-latch-pilot-offered-{mode}-1.json")
    assert report["rate"] == 200 and report["seconds"] == 60 and report["offered"] == 12000
    assert report["errors"] == 0 and report["completed"] + report["dropped"] == report["offered"]
    summary["offered_pilot"][mode] = {key: report[key] for key in ("histograms", "completed_in_window", "dropped", "compactions", "quiet_compactions")}

before = load("compaction-latch-whole-probe-1.json")
assert (ROOT / "apps/ferricstore/lib/ferricstore/store/shard/compound/promoted.ex").read_text() == before["promoted_source"]
assert (ROOT / "apps/ferricstore/lib/ferricstore/store/shard/info.ex").read_text() == before["shard_info_source"]
summary["pre_experiment_source_restored"] = True
(RESULTS / "compaction-latch-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
