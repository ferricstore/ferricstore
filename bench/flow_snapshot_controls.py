"""Matched native/Flow work with low-volume snapshot-return observation."""
import copy
import hashlib
import json
import os
import signal
import subprocess
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"
PREFIX = os.environ.get("BENCH_GATE_PREFIX", "flow-snapshot-matched")
PARENT = os.environ.get("BENCH_DATA_PARENT", "/var/folders/n3/4p13zj5n0kjbs8xk2qb4jq080000gn/T/opencode")
CYCLES = int(os.environ.get("BENCH_CONTROL_CYCLES_PER_CLIENT", "160"))
OFFSET_GATE = os.environ.get("BENCH_GATE_OFFSET") == "1"
MODES = ("raw", "buffered") if OFFSET_GATE else ("legacy", "bounded")
CHANGED = ((":ferricstore_waraft_spike_segment_log",) if OFFSET_GATE else
           ("Ferricstore.Flow.HistoryProjector", "Ferricstore.Flow.LMDBRebuilder"))
rows, identities = [], {}
shared = None

for trial in range(1, 4):
    for mode in (MODES if trial % 2 else tuple(reversed(MODES))):
        label = f"{PREFIX}-{mode}-{trial}"
        workload, stop, log_path = [RESULTS / f"{label}{suffix}" for suffix in
                                   (".json", "-stop.json", ".log")]
        if any(path.exists() for path in (workload, stop, log_path)):
            raise ValueError(f"trial exists: {label}")
        env = dict(os.environ)
        for key in ("BENCH_TRACE_SHUTDOWN", "BENCH_TRACE_RECONCILE", "BENCH_TRACE_OFFSETS", "BENCH_BOOTSTRAP_OUTPUT",
                    "BENCH_CONTROL_PROFILE"):
            env.pop(key, None)
        env.update({"ERL_FLAGS": "+S 8:8", "BENCH_SNAPSHOT_PROJECTION": "bounded" if OFFSET_GATE else mode,
                    "BENCH_OFFSET_SCAN": mode if OFFSET_GATE else "buffered", "BENCH_FLOW_CACHE": "retained",
                    "BENCH_FLOW_SOURCE_WAIT": "batch", "BENCH_FLOW_CLOCK": "wall",
                    "BENCH_HSET_GROUP": "direct", "BENCH_PROMOTED_READ": "protected",
                    "BENCH_COMPACTION_SYNC": "page", "BENCH_SNAPSHOT_COPY": "baseline",
                    "BENCH_CONTROL_CASES": "native_set_get,flow_lifecycle",
                    "BENCH_CONTROL_CYCLES_PER_CLIENT": str(CYCLES),
                    "BENCH_CONTROL_SECONDS": "300", "BENCH_CONTROL_WARMUP_SECONDS": "0",
                    "BENCH_DATA_PARENT": PARENT, "BENCH_VERIFY_SNAPSHOTS": "1",
                    "BENCH_SHUTDOWN_TIMEOUT_MS": "120000",
                    "BENCH_OUTPUT": str(workload), "BENCH_SHUTDOWN_OUTPUT": str(stop)})
        print(f"START {label}", flush=True)
        with log_path.open("w") as log:
            process = subprocess.Popen(["mise", "exec", "--", "mix", "run", "--no-start",
                                        "bench/promoted_read_controls.exs"], cwd=ROOT, env=env,
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = process.wait(timeout=300)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise
        report = json.loads(workload.read_text())
        stopped = json.loads(stop.read_text())
        if (report["offset_scan" if OFFSET_GATE else "snapshot_projection"] != mode or report["source_wait"] != "batch" or
                report["errors"] or report["flow_clock"] != "wall" or
                report["hset_group"] != "direct" or report["data_parent"] != PARENT or
                report["warmup_seconds"] or report["bootstrap_traced"] or
                not stopped["snapshots_only"] or not stopped["snapshot_results_verified"]):
            raise ValueError(f"unmatched control: {label}")
        scenarios = {row["scenario"]: row for row in report["results"]}
        if set(scenarios) != {"native_set_get", "flow_lifecycle"} or any(
                row["cycles"] != CYCLES * 8 for row in scenarios.values()):
            raise ValueError("workload ended before equal work completed")
        identity = copy.deepcopy(report["publication_identity"])
        sources = ({"apps/ferricstore/src/ferricstore_waraft_spike_segment_log/sections/part_05.hrl":
                    report["offset_source"]} if OFFSET_GATE else report["projection_sources"])
        candidate = {"beam_md5": {module: identity["beam_md5"].pop(module) for module in CHANGED},
                     "source_sha256": {path: hashlib.sha256(source.encode()).hexdigest()
                                       for path, source in sources.items()}}
        if mode == MODES[1] and any(identity["sha256"][path] != digest
                                     for path, digest in candidate["source_sha256"].items()):
            raise ValueError("candidate differs from frozen workspace source")
        if identities.setdefault(mode, candidate) != candidate:
            raise ValueError("candidate/control source changed")
        if shared is None:
            shared = identity
        if identity != shared:
            raise ValueError("shared source or loaded BEAM changed")
        if code != (0 if stopped["success"] else 1):
            raise ValueError("unexpected process failure")
        snapshots = [row for row in stopped["snapshot_results"]
                     if row["mfa"] == "{Ferricstore.Raft.WARaftStorage, :create_snapshot, 2}"]
        rows.append({"trial": trial, "mode": mode, "results": scenarios,
                     "elapsed_us": stopped["elapsed_us"], "success": stopped["success"],
                     "application_stop_completed": stopped["application_stop_completed"],
                     "snapshots": snapshots, "exit_code": code})
        print(f"RESULT {label}: snapshots={stopped['storage_snapshots_succeeded']} "
              f"stop={stopped['elapsed_us'] / 1e6:.3f}s", flush=True)

summary = {"trials": rows, "identities": identities, "shared_identity": shared,
           "workload_profiled": False, "shutdown_observation": "snapshot calls only",
           "cases": {}, "candidate_lifecycle_passed": all(row["success"] for row in rows
                                                          if row["mode"] == MODES[1])}
for scenario in ("native_set_get", "flow_lifecycle"):
    summary["cases"][scenario] = {}
    for mode in MODES:
        selected = [row["results"][scenario] for row in rows if row["mode"] == mode]
        summary["cases"][scenario][mode] = {
            metric: {"median": median(values), "min": min(values), "max": max(values)}
            for metric in ("cycles_per_second", "p50_us", "p99_us", "p999_us", "max_us")
            for values in [[row[metric] for row in selected]]
        }
(RESULTS / f"{PREFIX}-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps({"cases": summary["cases"],
                  "candidate_lifecycle_passed": summary["candidate_lifecycle_passed"]}, indent=2))
if not summary["candidate_lifecycle_passed"]:
    raise SystemExit(1)
