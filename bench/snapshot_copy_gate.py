"""Fresh-VM alternating controls for single-pass durable snapshot copying."""
import json
import os
import subprocess
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"
PREFIX = os.environ.get("BENCH_GATE_PREFIX", "snapshot-copy")
rows = []
identities = {}
for trial in range(1, 4):
    for mode in (("baseline", "single_pass") if trial % 2 else ("single_pass", "baseline")):
        label = f"{PREFIX}-{mode}-{trial}"
        env = {**os.environ, "ERL_FLAGS": "+S 8:8", "BENCH_SNAPSHOT_COPY": mode,
               "BENCH_OUTPUT": f"bench/results/{label}.json"}
        print(f"START {label}", flush=True)
        with (RESULTS / f"{label}.log").open("w") as log:
            subprocess.run(["mise", "exec", "--", "mix", "run", "--no-start",
                            "bench/snapshot_copy_perf.exs"], cwd=ROOT, env=env,
                           stdout=log, stderr=subprocess.STDOUT, check=True)
        report = json.loads((RESULTS / f"{label}.json").read_text())
        if report["errors"] or report["variant"] != mode or report["diagnostic"]:
            raise ValueError("invalid snapshot copy control")
        signature = (report["source"], report["storage_beam_md5"], report["files"], report["bytes"],
                     report["otp"], report["elixir"], report["erl_flags"])
        if identities.setdefault(mode, signature) != signature:
            raise ValueError("variant source/runtime changed")
        rows.append({"mode": mode, "trial": trial, "elapsed_us": report["elapsed_us"]})
        print(f"PASS {label}", flush=True)
summary = {"trials": rows, "variants": {}}
for mode in ("baseline", "single_pass"):
    values = [row["elapsed_us"] for row in rows if row["mode"] == mode]
    summary["variants"][mode] = {"median_us": median(values), "min_us": min(values), "max_us": max(values)}
(RESULTS / f"{PREFIX}-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
