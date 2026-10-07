"""Alternating equal-work native/Flow controls, including unprofiled shutdown."""

import copy
import hashlib
import json
import os
import signal
import subprocess
import time
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"
PREFIX = os.environ.get("BENCH_GATE_PREFIX", "flow-shutdown-matched")
PARENT = os.environ.get("BENCH_DATA_PARENT", "/var/folders/n3/4p13zj5n0kjbs8xk2qb4jq080000gn/T/opencode")
CYCLES = int(os.environ.get("BENCH_CONTROL_CYCLES_PER_CLIENT", "160"))
SECONDS = int(os.environ.get("BENCH_CONTROL_SECONDS", "300"))
TIMEOUT = int(os.environ.get("BENCH_SHUTDOWN_TIMEOUT_MS", "20000"))
WRITER_PATH = "apps/ferricstore/lib/ferricstore/flow/lmdb_writer.ex"
WRITER_MODULE = "Ferricstore.Flow.LMDBWriter"
if not Path(PARENT).is_dir() or CYCLES <= 0 or SECONDS <= 0 or TIMEOUT <= 0:
    raise ValueError("existing fixture parent and positive work/time budgets required")

rows = []
shared_identity = None
writer_identities = {}
for trial in range(1, 4):
    for mode in (("sequential", "batch") if trial % 2 else ("batch", "sequential")):
        label = f"{PREFIX}-{mode}-{trial}"
        output = RESULTS / f"{label}.json"
        shutdown_output = RESULTS / f"{label}-shutdown.json"
        log_path = RESULTS / f"{label}.log"
        if any(path.exists() for path in (output, shutdown_output, log_path)):
            raise ValueError(f"refusing to overwrite existing trial: {label}")
        env = dict(os.environ)
        for name in ("BENCH_BOOTSTRAP_OUTPUT", "BENCH_TRACE_SHUTDOWN", "BENCH_CONTROL_PROFILE"):
            env.pop(name, None)
        env.update({
            "ERL_FLAGS": "+S 8:8",
            "BENCH_FLOW_SOURCE_WAIT": mode,
            "BENCH_FLOW_CLOCK": "wall",
            "BENCH_HSET_GROUP": "direct",
            "BENCH_PROMOTED_READ": "protected",
            "BENCH_COMPACTION_SYNC": "page",
            "BENCH_SNAPSHOT_COPY": "baseline",
            "BENCH_DATA_PARENT": PARENT,
            "BENCH_CONTROL_CASES": "native_set_get,flow_lifecycle",
            "BENCH_CONTROL_WARMUP_SECONDS": "0",
            "BENCH_CONTROL_CYCLES_PER_CLIENT": str(CYCLES),
            "BENCH_CONTROL_SECONDS": str(SECONDS),
            "BENCH_SHUTDOWN_TIMEOUT_MS": str(TIMEOUT),
            "BENCH_SHUTDOWN_OUTPUT": str(shutdown_output),
            "BENCH_OUTPUT": str(output),
        })
        print(f"START {label}", flush=True)
        started = time.monotonic()
        with log_path.open("w") as log:
            process = subprocess.Popen(
                ["mise", "exec", "--", "mix", "run", "--no-start",
                 "bench/promoted_read_controls.exs"],
                cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            try:
                exit_code = process.wait(timeout=SECONDS * 2 + TIMEOUT / 1000 + 120)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise
        elapsed = time.monotonic() - started
        report = json.loads(output.read_text())
        shutdown = json.loads(shutdown_output.read_text())
        if (report["errors"] or report["source_wait"] != mode or
                report["variant"] != "protected" or report["flow_clock"] != "wall" or
                report["hset_group"] != "direct" or report["snapshot_copy"] != "baseline" or
                report["compaction_sync"] != "page" or report["data_parent"] != PARENT or
                report["warmup_seconds"] != 0 or report["bootstrap_traced"] or
                report["cycles_per_client"] != str(CYCLES) or shutdown["diagnostic_only"]):
            raise ValueError(f"unmatched or profiled control: {label}")
        if shutdown["success"]:
            if exit_code != 0 or shutdown["result"] != "{:ok, :stopped}":
                raise ValueError(f"unsuccessful lifecycle: {label}")
        elif exit_code != 1 or shutdown["result"] != "{:error, :shutdown_timeout}":
            raise ValueError(f"unexpected control failure: {label}")
        identity = copy.deepcopy(report["publication_identity"])
        writer_md5 = identity["beam_md5"].pop(WRITER_MODULE)
        if writer_md5 != report["writer_beam_md5"]:
            raise ValueError("loaded writer identity mismatch")
        source_sha256 = hashlib.sha256(report["writer_source"].encode()).hexdigest()
        if mode == "batch" and source_sha256 != identity["sha256"][WRITER_PATH]:
            raise ValueError("batch writer does not match frozen workspace source")
        writer_identity = {"beam_md5": writer_md5, "source_sha256": source_sha256}
        if writer_identities.setdefault(mode, writer_identity) != writer_identity:
            raise ValueError("writer changed between trials")
        if shared_identity is None:
            shared_identity = identity
        if identity != shared_identity:
            raise ValueError("shared source/BEAM identity changed between trials")
        scenarios = {result["scenario"]: result for result in report["results"]}
        if set(scenarios) != {"native_set_get", "flow_lifecycle"}:
            raise ValueError("missing workload control")
        if any(result["cycles"] != CYCLES * 8 for result in scenarios.values()):
            raise ValueError("deadline ended before equal work completed")
        rows.append({"trial": trial, "mode": mode, "results": scenarios,
                     "shutdown": shutdown, "exit_code": exit_code,
                     "process_elapsed_seconds": elapsed})
        print(f"RESULT {label}: shutdown={shutdown['result']} "
              f"elapsed={shutdown['elapsed_us'] / 1e6:.3f}s", flush=True)

summary = {"trials": rows, "cases": {}, "shared_identity": shared_identity,
           "writer_identities": writer_identities, "cycles_per_scenario": CYCLES * 8,
           "shutdown_timeout_ms": TIMEOUT, "diagnostic_only": False,
           "acceptance_scope": "operation replies and application-stop completion",
           "snapshot_results_verified": False,
           "batch_lifecycle_passed": all(row["shutdown"]["success"]
                                         for row in rows if row["mode"] == "batch")}
for scenario in ("native_set_get", "flow_lifecycle"):
    summary["cases"][scenario] = {}
    for mode in ("sequential", "batch"):
        selected = [row["results"][scenario] for row in rows if row["mode"] == mode]
        summary["cases"][scenario][mode] = {
            metric: {"median": median(values), "min": min(values), "max": max(values)}
            for metric in ("cycles_per_second", "p50_us", "p99_us", "p999_us", "max_us")
            for values in [[row[metric] for row in selected]]
        }
(RESULTS / f"{PREFIX}-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps({"cases": summary["cases"],
                  "batch_lifecycle_passed": summary["batch_lifecycle_passed"]}, indent=2))
if not summary["batch_lifecycle_passed"]:
    raise SystemExit(1)
