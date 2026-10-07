"""Summarize exact per-request diagnostic stages without pooling performance gates."""
import json
import sys
from pathlib import Path

for name in sys.argv[1:]:
    report = json.loads(Path(name).read_text())
    timeline = report["write_timeline"]
    print(name, "seen", timeline["writes_seen"], "complete", timeline["complete_writes"],
          "dropped writes/io", timeline["dropped_writes"], timeline["dropped_io"])
    for key, stat in timeline["histograms"].items():
        print(key, {q: round(stat[q] / 1000, 3) if stat[q] is not None else None for q in ("p50", "p95", "p99", "max")})
    for row in timeline["slow_writes"][:16]:
        stages = {key: round(row[key] / 1000, 3) for key in
                  ("total_us", "wal_queue_us", "wal_wall_us", "apply_queue_us", "apply_wall_us", "native_apply_us", "latch_us")}
        if "wal_native_us" in row:
            stages["wal_native_us"] = round(row["wal_native_us"] / 1000, 3)
        print("slow", row["shard"], row["field"], row["version"], stages)
