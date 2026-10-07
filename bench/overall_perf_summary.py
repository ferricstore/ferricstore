"""Summarize paired steady-state performance-review measurements."""

import json
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"


def load(name):
    return json.loads((RESULTS / name).read_text())


def metrics(rows, names):
    return {name: {"median": median(row[name] for row in rows),
                   "min": min(row[name] for row in rows),
                   "max": max(row[name] for row in rows)} for name in names}


summary = {"aggregation": "medians and ranges of trial metrics, not pooled percentiles"}
embedded = load("embedded-dispatch-perf.json")["results"]
summary["embedded"] = {}
for scenario in sorted({row["scenario"] for row in embedded}):
    summary["embedded"][scenario] = {
        variant: metrics([row for row in embedded if row["scenario"] == scenario and row["variant"] == variant],
                         ["ops_per_second", "p50_us", "p95_us", "p99_us"])
        for variant in ("baseline", "instance")}

auth = load("auth-cache-maintenance-perf.json")["results"]
summary["auth_cache"] = {}
for entries in (1000, 10000):
    for groups in (0, 32):
        key = f"entries={entries},groups={groups}"
        summary["auth_cache"][key] = {}
        for variant in ("baseline", "metadata_only"):
            rows = [row for row in auth if row["entries"] == entries and row["groups"] == groups and row["variant"] == variant]
            summary["auth_cache"][key][variant] = {
                workload: metrics([row[workload] for row in rows],
                                  ["ops_per_second", "p50_us", "p95_us", "p99_us", "actor_memory_after_bytes"])
                for workload in ("sweep", "misses", "hits")}

native = load("native-cleanup-perf.json")["results"]
summary["native_cleanup"] = {
    str(count): {variant: metrics([row for row in native if row["unrelated_scopes"] == count and row["variant"] == variant],
                                  ["p50_us", "p95_us", "p99_us"])
                 for variant in ("baseline", "indexed")}
    for count in (0, 16, 128, 4096)}

frames = load("frame-accounting-perf.json")["results"]
summary["frame_accounting"] = {
    str(size): {variant: metrics([row for row in frames if row["chunk_size"] == size and row["variant"] == variant],
                                 ["us_per_frame", "reductions_per_frame"])
                for variant in ("baseline", "counted")}
    for size in sorted({row["chunk_size"] for row in frames})}

http = [json.loads(path.read_text()) for path in sorted((ROOT / "bench/output/overall-http").glob("*.json"))
        if path.stem.rsplit("-", 1)[-1].isdigit()]
summary["http_keepalive"] = {variant: metrics([row for row in http if row["variant"] == variant], ["requests_per_second"])
                             for variant in ("baseline", "current")}
summary["http_trials"] = http
summary["query_profile"] = load("overall-query-profile.json")
(RESULTS / "overall-performance-summary.json").write_text(json.dumps(summary, indent=2) + "\n")

for scenario, variants in summary["embedded"].items():
    old, new = variants["baseline"], variants["instance"]
    print("embedded", scenario, "ops/s", round(old["ops_per_second"]["median"]), round(new["ops_per_second"]["median"]),
          "speedup", round(new["ops_per_second"]["median"] / old["ops_per_second"]["median"], 2))
for key, variants in summary["auth_cache"].items():
    print("auth", key, "miss p50 us", variants["baseline"]["misses"]["p50_us"]["median"],
          variants["metadata_only"]["misses"]["p50_us"]["median"])
for count, variants in summary["native_cleanup"].items():
    print("cleanup", count, "p50 us", variants["baseline"]["p50_us"]["median"], variants["indexed"]["p50_us"]["median"])
for size, variants in summary["frame_accounting"].items():
    print("frame", size, "us", variants["baseline"]["us_per_frame"]["median"], variants["counted"]["us_per_frame"]["median"])
print("HTTP", summary["http_keepalive"])
