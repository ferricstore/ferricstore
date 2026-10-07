"""Summarize bounded latency histograms and memory observations."""
import json
import math
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent

def quantile(buckets, q):
    total = sum(buckets.values())
    target = max(1, math.ceil(total * q))
    seen = 0
    for us, n in sorted(buckets.items()):
        seen += n
        if seen >= target:
            return us

results = []
for path in sorted((ROOT / "bench/results").glob("sustained-mixed-*.json")):
    if path.stem.rsplit("-", 1)[-1] not in ("1", "2", "long"):
        continue
    run = json.loads(path.read_text())
    operations = {}
    for kind in ("hash_write", "hash_read", "kv_read"):
        windows = [w for w in run["windows"] if w["operation"] == kind]
        buckets = {}
        for w in windows:
            for us, n in w["buckets"].items():
                buckets[int(us)] = buckets.get(int(us), 0) + n
        operations[kind] = {"count": sum(buckets.values()), "p95_us": quantile(buckets, .95),
                            "p99_us": quantile(buckets, .99), "max_us": max(w["max_us"] for w in windows),
                            "worst_window_p99_us": max(w["p99_us"] for w in windows)}
    samples = run["memory_samples"]
    result = {"mode": run["mode"], "trial": run["trial"], "operations": operations,
              "ops_per_second": sum(v["count"] for v in operations.values()) / run["measured_elapsed_seconds"],
              "compactions": sum(e["event"] == "compaction" for e in run["events"]),
              "compaction_failures": sum(e["event"] == "compaction_failed" for e in run["events"]),
              "peak_sampled_rss_mib": max(s["rss_bytes"] for s in samples) / 1024**2,
              "before_rss_mib": run["before"]["rss_bytes"] / 1024**2,
              "quiet_rss_mib": run["after_quiet"]["rss_bytes"] / 1024**2,
              "first_ets_mib": samples[0]["ets_bytes"] / 1024**2,
              "last_ets_mib": samples[-1]["ets_bytes"] / 1024**2,
              "early_rss_median_mib": median(s["rss_bytes"] for s in samples[10:70]) / 1024**2,
              "late_rss_median_mib": median(s["rss_bytes"] for s in samples[-60:]) / 1024**2,
              "early_ets_median_mib": median(s["ets_bytes"] for s in samples[10:70]) / 1024**2,
              "late_ets_median_mib": median(s["ets_bytes"] for s in samples[-60:]) / 1024**2,
              "pressure_samples": sum(s.get("operational_pressure", False) for s in samples),
              "rejection_samples": sum(s.get("writes_rejected", False) for s in samples),
              "minute_memory": [{"minute": i // 60,
                                 "rss_median_mib": median(s["rss_bytes"] for s in samples[i:i+60]) / 1024**2,
                                 "ets_median_mib": median(s["ets_bytes"] for s in samples[i:i+60]) / 1024**2}
                                for i in range(0, len(samples), 60)],
              "first_processes": samples[0]["processes"], "quiet_processes": run["after_quiet"]["processes"]}
    results.append(result)
print(json.dumps(results, indent=2))
(ROOT / "bench/results/sustained-mixed-summary.json").write_text(json.dumps(results, indent=2) + "\n")
