"""Inspect bounded shutdown diagnostics without treating traced timing as acceptance."""
import json
import sys
from collections import Counter
from pathlib import Path

for filename in sys.argv[1:]:
    report = json.loads(Path(filename).read_text())
    samples = report["samples"]
    print(filename, "elapsed", report["elapsed_us"] / 1e6, "success", report["success"])
    print("work counts", report.get("work_counts"))
    print("offset counts", report.get("offset_counts"))
    print("offset examples", report.get("offset_examples", []))
    print("reconcile failures", report.get("reconcile_failure_counts"), report.get("reconcile_failures", []))
    print("source examples", report.get("source_examples", []))
    snapshots = report.get("snapshot_results", [])
    print("snapshot results", [row for row in snapshots
          if row["result"] != "{:error, :backend_unavailable}"])
    print("post-teardown backend-unavailable probes", sum(
          row["result"] == "{:error, :backend_unavailable}" for row in snapshots))
    print("slow snapshot returns", [row for row in report.get("slow_calls", [])
          if "create_snapshot" in row["mfa"]])
    print("commit shapes", Counter(phase.get("command_shape") for phase in report.get("phases", []) if "commit" in phase["event"]))
    for sample in (samples[0], samples[len(samples)//2], samples[-1]):
        print("progress", sample.get("history_progress"))
        print("coordinator", sample.get("flush_coordinator"))
        print("calls", sample["calls"])
    names = sorted({actor["name"] for sample in samples for actor in sample["actors"]})
    for name in names:
        actors = [actor for sample in samples for actor in sample["actors"] if actor["name"] == name]
        if "projector" in name.lower() or "writer" in name.lower() or "storage" in name.lower():
            stacks = Counter(tuple(actor["stack"][:4]) for actor in actors)
            print(name, "max queue", max(actor["queue"] for actor in actors), "stacks", stacks.most_common(2))
    shapes = Counter(call.get("command_shape") for sample in samples for call in sample["calls"] if "command_shape" in call)
    print("sampled apply shapes", shapes)
