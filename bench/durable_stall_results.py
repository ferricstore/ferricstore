"""Validate and summarize durable-flush experiments, keeping their cohorts separate."""
import json
from collections import Counter
from pathlib import Path
from statistics import median

RESULTS = Path(__file__).resolve().parent / "results"


def quantiles(windows):
    bins = Counter()
    for window in windows:
        bins.update({int(value): count for value, count in window["buckets"].items()})
    total = sum(bins.values())
    result = {"count": total, "max_us": max(window["max_us"] for window in windows)}
    for q in (0.5, 0.95, 0.99):
        seen = 0
        for value, n in sorted(bins.items()):
            seen += n
            if seen >= total * q:
                result[f"p{int(q * 100)}_us"] = value
                break
    return result


summary = {"sync_trials": [], "sync_variants": {}, "copy_trials": [], "copy_variants": {}}
identity = None
signatures = {}
for mode in ("page", "grouped"):
    for trial in range(1, 4):
        report = json.loads((RESULTS / f"compaction-sync-{mode}-{trial}.json").read_text())
        if (report["errors"] or report["seconds"] != 120 or report["clients"] != 16
                or report["compaction_sync"] != mode or report["compaction_admission"] != "parallel"
                or report["request_spans_enabled"] or "profile" in report):
            raise ValueError("invalid/unmatched sync report")
        if identity is None:
            identity = report["publication_identity"]
        if identity != report["publication_identity"]:
            raise ValueError("shared source/runtime changed within sync cohort")
        signature = (report["compaction_sync_source"], report["promoted_beam_md5"])
        if signatures.setdefault(mode, signature) != signature:
            raise ValueError("sync variant source/compiled module changed")
        if any(status["active"] or status["pending"] or status["retries"]
               for status in report["compaction_status"]):
            raise ValueError("sync cohort left outstanding maintenance")
        operations = {kind: quantiles([window for window in report["windows"] if window["operation"] == kind])
                      for kind in ("hash_write", "hash_read", "kv_read", "cycle")}
        summary["sync_trials"].append({"mode": mode, "trial": trial, "operations": operations,
            "ops_per_second": sum(operations[kind]["count"] for kind in ("hash_write", "hash_read", "kv_read"))
                              / report["measured_elapsed_seconds"],
            "compactions": sum(event["event"] == "compaction" for event in report["events"]),
            "quiet_compactions": sum(event["event"] == "compaction" for event in report["after_quiet_events"])})
    rows = [row for row in summary["sync_trials"] if row["mode"] == mode]
    summary["sync_variants"][mode] = {
        "ops_per_second_median": median(row["ops_per_second"] for row in rows),
        "write_p99_us_median": median(row["operations"]["hash_write"]["p99_us"] for row in rows),
        "write_max_us": max(row["operations"]["hash_write"]["max_us"] for row in rows),
    }
    for trial in range(1, 4):
        report = json.loads((RESULTS / f"compaction-copy-{mode}-{trial}.json").read_text())
        if report["errors"] or not report["component_only"] or report["mode"] != mode:
            raise ValueError("invalid copy report")
        timings = [row["duration_us"] for row in report["rounds"] if row["round"] > 0]
        summary["copy_trials"].append({"mode": mode, "trial": trial, "median_us": median(timings),
                                      "durations_us": timings})
    summary["copy_variants"][mode] = {"median_of_trial_medians_us": median(
        row["median_us"] for row in summary["copy_trials"] if row["mode"] == mode)}
summary["publication_identity"] = identity
(RESULTS / "durable-stall-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps({key: value for key, value in summary.items() if key.endswith("variants")}, indent=2))
