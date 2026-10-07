"""Fresh-VM, serial, alternating performance controls for protected promoted reads."""
import json
import os
import subprocess
import sys
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"
PARENT = os.environ.get("BENCH_DATA_PARENT", "/Volumes/FerricstorePerfGate")
if not Path(PARENT).is_dir():
    raise ValueError("healthy-capacity scratch volume must already be mounted")


def run(label, script, variables):
    env = {**os.environ, "ERL_FLAGS": "+S 8:8", **variables}
    path = RESULTS / f"{label}.log"
    print(f"START {label}", flush=True)
    with path.open("w") as log:
        subprocess.run(
            ["mise", "exec", "--", "mix", "run", "--no-start", script],
            cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True,
        )
    print(f"PASS {label}", flush=True)


summarize_final = "--summarize-final" in sys.argv
if "--summarize-only" not in sys.argv and not summarize_final:
    final_source = "--final-source" in sys.argv
    for trial in (["final"] if final_source else range(1, 4)):
        order = ("baseline", "protected") if final_source or trial % 2 else ("protected", "baseline")
        for variant in order:
            for cohort, seconds, cycle in (("protected", 120, 0), ("protected-paced", 60, 100)):
                label = f"promoted-live-{cohort}-{variant}-{trial}"
                run(label, "bench/sustained_mixed_perf.exs", {
                    "BENCH_PROMOTED_READ": variant,
                    "BENCH_TRIAL": str(trial),
                    "BENCH_SECONDS": str(seconds),
                    "BENCH_CYCLE_MS": str(cycle),
                    "BENCH_COMPACTION": "auto",
                    "BENCH_PROFILE": "0",
                    "BENCH_OUTPUT": f"bench/results/{label}.json",
                })
            run(f"promoted-live-controls-{variant}-{trial}", "bench/promoted_read_controls.exs", {
                "BENCH_PROMOTED_READ": variant,
                "BENCH_TRIAL": f"live-{trial}",
                "BENCH_DATA_PARENT": PARENT,
                "BENCH_CONTROL_CASES": "native_set_get,flow_lifecycle",
            })
    if final_source:
        # Keep the final-source confirmation separate from the three-pair cohort.
        sys.exit(0)

for cohort in ("protected", "protected-paced"):
    subprocess.run([
        sys.executable, "bench/hash_read_summary.py", cohort, f"promoted-live-{cohort}"
    ] + (["final"] if summarize_final else []), cwd=ROOT, check=True)

rows = []
identities = []
for trial in (["final"] if summarize_final else range(1, 4)):
    for variant in ("baseline", "protected"):
        report = json.loads((RESULTS / f"promoted-read-controls-{variant}-live-{trial}.json").read_text())
        if report["errors"] or report["variant"] != variant or report["data_parent"] != PARENT:
            raise ValueError("invalid control report")
        identities.append(report["publication_identity"])
        for result in report["results"]:
            rows.append({"trial": trial, "variant": variant, **result})
if any(identity != identities[0] for identity in identities):
    raise ValueError("control source or compiled modules changed")

summary = {"trials": rows, "variants": {}, "publication_identity": identities[0]}
for variant in ("baseline", "protected"):
    summary["variants"][variant] = {}
    for scenario in ("native_set_get", "flow_lifecycle"):
        selected = [row for row in rows if row["variant"] == variant and row["scenario"] == scenario]
        summary["variants"][variant][scenario] = {
            metric: {"median": median(values), "min": min(values), "max": max(values)}
            for metric in ("cycles_per_second", "p50_us", "p95_us", "p99_us")
            for values in [[row[metric] for row in selected]]
        }
filename = "promoted-live-controls-final-summary.json" if summarize_final else "promoted-live-controls-summary.json"
(RESULTS / filename).write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
