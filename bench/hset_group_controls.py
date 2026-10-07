"""Serial, alternating native TCP and public Flow controls for opt-in HSET batching."""
import json
import os
import subprocess
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"
PREFIX = os.environ.get("BENCH_GATE_PREFIX", "hset-group-controls")
PARENT = os.environ.get("BENCH_DATA_PARENT", "/Volumes/FerricstorePerfGate")
if not Path(PARENT).is_dir():
    raise ValueError("healthy-capacity scratch volume must already be mounted")

rows = []
identity = None
for trial in range(1, 4):
    for mode in (("direct", "coalesced") if trial % 2 else ("coalesced", "direct")):
        label = f"{PREFIX}-{mode}-{trial}"
        env = {**os.environ, "ERL_FLAGS": "+S 8:8", "BENCH_HSET_GROUP": mode,
               "BENCH_PROMOTED_READ": "protected", "BENCH_DATA_PARENT": PARENT,
               "BENCH_CONTROL_CASES": "native_set_get,flow_lifecycle",
               "BENCH_OUTPUT": f"bench/results/{label}.json"}
        print(f"START {label}", flush=True)
        with (RESULTS / f"{label}.log").open("w") as log:
            subprocess.run(["mise", "exec", "--", "mix", "run", "--no-start",
                            "bench/promoted_read_controls.exs"], cwd=ROOT, env=env,
                           stdout=log, stderr=subprocess.STDOUT, check=True)
        report = json.loads((RESULTS / f"{label}.json").read_text())
        if (report["errors"] or report["hset_group"] != mode or
                report["variant"] != "protected" or report["data_parent"] != PARENT):
            raise ValueError("unmatched control")
        if identity is None:
            identity = report["publication_identity"]
        if report["publication_identity"] != identity:
            raise ValueError("control source changed")
        rows.extend({"trial": trial, "mode": mode, **result} for result in report["results"])
        print(f"PASS {label}", flush=True)

summary = {"trials": rows, "cases": {}, "publication_identity": identity}
for scenario in ("native_set_get", "flow_lifecycle"):
    summary["cases"][scenario] = {}
    for mode in ("direct", "coalesced"):
        selected = [row for row in rows if row["scenario"] == scenario and row["mode"] == mode]
        summary["cases"][scenario][mode] = {
            metric: {"median": median(values), "min": min(values), "max": max(values)}
            for metric in ("cycles_per_second", "p50_us", "p95_us", "p99_us")
            for values in [[row[metric] for row in selected]]
        }
(RESULTS / f"{PREFIX}-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary["cases"], indent=2))
