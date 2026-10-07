"""Serial native/Flow controls isolating snapshot-copy policy with HSET coalescing off."""
import copy
import json
import os
import subprocess
import sys
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"
PREFIX = os.environ.get("BENCH_GATE_PREFIX", "snapshot-copy-controls")
PARENT = os.environ.get("BENCH_DATA_PARENT", "/Volumes/FerricstorePerfGate")
SECONDS = int(os.environ.get("BENCH_CONTROL_SECONDS", "5"))
WARMUP = int(os.environ.get("BENCH_CONTROL_WARMUP_SECONDS", "1"))
if not Path(PARENT).is_dir():
    raise ValueError("healthy-capacity scratch volume must already be mounted")
rows, signatures = [], {}
identity = None
TRIALS = ["final"] if "--final-source" in sys.argv else range(1, 4)
for ordinal, trial in enumerate(TRIALS, 1):
    for mode in (("baseline", "single_pass") if ordinal % 2 else ("single_pass", "baseline")):
        label = f"{PREFIX}-{mode}-{trial}"
        env = {**os.environ, "ERL_FLAGS": "+S 8:8", "BENCH_SNAPSHOT_COPY": mode,
               "BENCH_HSET_GROUP": "direct", "BENCH_PROMOTED_READ": "protected",
               "BENCH_DATA_PARENT": PARENT, "BENCH_CONTROL_CASES": "native_set_get,flow_lifecycle",
               "BENCH_OUTPUT": f"bench/results/{label}.json"}
        if os.environ.get("BENCH_TRACE_BOOTSTRAP") == "1":
            env["BENCH_BOOTSTRAP_OUTPUT"] = f"bench/results/{label}-bootstrap.json"
        print(f"START {label}", flush=True)
        with (RESULTS / f"{label}.log").open("w") as log:
            subprocess.run(["mise", "exec", "--", "mix", "run", "--no-start",
                            "bench/promoted_read_controls.exs"], cwd=ROOT, env=env,
                           stdout=log, stderr=subprocess.STDOUT, check=True)
        report = json.loads((RESULTS / f"{label}.json").read_text())
        if (report["errors"] or report["snapshot_copy"] != mode or report["hset_group"] != "direct"
                or report["data_parent"] != PARENT or "profile" in report):
            raise ValueError("unmatched snapshot control")
        if report["seconds"] != SECONDS or report["warmup_seconds"] != WARMUP:
            raise ValueError("control duration changed")
        shared = copy.deepcopy(report["publication_identity"])
        storage_md5 = shared["beam_md5"].pop("Ferricstore.Raft.WARaftStorage")
        signature = (report["snapshot_copy_source"], storage_md5)
        if signatures.setdefault(mode, signature) != signature:
            raise ValueError("variant source changed")
        if identity is None:
            identity = shared
        if shared != identity:
            raise ValueError("shared source/runtime changed")
        rows.extend({"trial": trial, "mode": mode, **result} for result in report["results"])
        print(f"PASS {label}", flush=True)
summary = {"trials": rows, "cases": {}, "publication_identity": identity,
           "seconds": SECONDS, "warmup_seconds": WARMUP}
for scenario in ("native_set_get", "flow_lifecycle"):
    summary["cases"][scenario] = {}
    for mode in ("baseline", "single_pass"):
        selected = [row for row in rows if row["scenario"] == scenario and row["mode"] == mode]
        summary["cases"][scenario][mode] = {
            metric: {"median": median(values), "min": min(values), "max": max(values)}
            for metric in ("cycles_per_second", "p50_us", "p95_us", "p99_us")
            for values in [[row[metric] for row in selected]]
        }
(RESULTS / f"{PREFIX}-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary["cases"], indent=2))
