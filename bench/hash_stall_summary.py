"""Inspect bounded diagnostic captures without pooling profiled/unprofiled runs."""

import json
import sys
from collections import Counter
from pathlib import Path

for name in sys.argv[1:]:
    report = json.loads(Path(name).read_text())
    profile = report["profile"]
    print(name, "mode", report["mode"], "errors", report["errors"])
    print("slow captures", len(profile["slow_operations"]), "events", len(profile["events"]))
    for operation in profile["slow_operations"][:8]:
        print("slow", operation["kind"], "shard", operation["shard"],
              "duration ms", round(operation["duration_us"] / 1000, 3),
              "at measured second", round((operation["started_us"] - report["before"]["at_us"]) / 1e6 - report["warmup_seconds"], 3))
        print("  spans", operation["trace"])
        queues = [sample["run_queues"] for sample in operation["samples"] if "run_queues" in sample]
        if queues:
            print("  run queue maxima: normal", max(max(queue[:-2]) for queue in queues),
                  "dirty CPU", max(queue[-2] for queue in queues), "dirty IO", max(queue[-1] for queue in queues))
        for role in ("shard", "raft", "storage"):
            actors = [actor for sample in operation["samples"] for actor in sample["actors"]
                      if actor["role"] == role]
            if actors:
                stacks = Counter(tuple(actor["stack"][:4]) for actor in actors)
                print(" ", role, "queue max", max(actor["queue"] for actor in actors),
                      "stack samples", stacks.most_common(2))
        overlapping = []
        for event in profile["events"]:
            duration = event.get("duration_us", event.get("measurements", {}).get("duration_us", 0))
            start = event.get("started_us", event.get("at_us", 0) - duration)
            finish = event.get("at_us", start + duration)
            if start <= operation["finished_us"] and finish >= operation["started_us"]:
                overlapping.append({key: value for key, value in event.items() if key != "measurements"})
        print("  overlapping events", overlapping[:12])
    for metric, buckets in profile["timing_buckets"].items():
        bins = sorted((int(bucket), count) for bucket, count in buckets.items())
        count = sum(n for _, n in bins)
        percentiles = {}
        for q in (0.5, 0.95, 0.99):
            seen = 0
            for value, n in bins:
                seen += n
                if seen >= count * q:
                    percentiles[q] = value
                    break
        print("timing", metric, "count", count, "us", percentiles, "max", bins[-1][0])
