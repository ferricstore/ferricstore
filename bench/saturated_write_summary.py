"""Inspect cycle, request-stage and service-time histograms without pooling runs."""
import json
import sys
from collections import Counter
from pathlib import Path


def histogram(windows):
    bins = Counter()
    for window in windows:
        bins.update({int(value): n for value, n in window["buckets"].items()})
    count = sum(bins.values())
    result = {"count": count, "max_us": max(window["max_us"] for window in windows)}
    for q in (0.5, 0.95, 0.99):
        seen = 0
        for value, n in sorted(bins.items()):
            seen += n
            if seen >= count * q:
                result[f"p{int(q * 100)}_us"] = value
                break
    return result


for filename in sys.argv[1:]:
    report = json.loads(Path(filename).read_text())
    print(filename, "variant", report["promoted_read_variant"], "errors", report["errors"],
          "compactions", len(report["events"]), "diagnostic", report.get("request_spans_enabled", False))
    for kind in sorted({window["operation"] for window in report["windows"]}):
        print(kind, histogram([window for window in report["windows"] if window["operation"] == kind]))
    metrics = report.get("waraft_metrics")
    if metrics:
        print("WARaft metrics", json.dumps(metrics, indent=2))
