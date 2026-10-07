"""Fixed offered-load, bounded-queue, alternating fresh-VM HSET batching controls."""
import json
import os
import subprocess
import sys
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"
PREFIX = os.environ.get("BENCH_GATE_PREFIX", "offered-mixed")
RATES = [100, 200, 300]
MODES = os.environ.get("BENCH_MODES", "direct,coalesced").split(",")
SECONDS = int(os.environ.get("BENCH_SECONDS", "120"))
TRIALS = [1] if "--pilot" in sys.argv else range(1, 4)
rows = []
identity = None
signatures = {}
for trial in TRIALS:
    for rate in RATES:
        for mode in (MODES if trial % 2 else list(reversed(MODES))):
            label = f"{PREFIX}-{rate}-{mode}-{trial}"
            env = {**os.environ, "ERL_FLAGS": "+S 8:8", "BENCH_HSET_GROUP": mode,
                   "BENCH_CYCLES_PER_SECOND": str(rate), "BENCH_SECONDS": str(SECONDS),
                   "BENCH_WARMUP_SECONDS": "10", "BENCH_CLIENTS": "16", "BENCH_QUEUE_DEPTH": "8",
                   "BENCH_FIELDS": "4096", "BENCH_WRITE_TIMELINE": "0",
                   "BENCH_OUTPUT": f"bench/results/{label}.json"}
            print(f"START {label}", flush=True)
            with (RESULTS / f"{label}.log").open("w") as log:
                subprocess.run(["mise", "exec", "--", "mix", "run", "--no-start", "bench/offered_mixed_perf.exs"],
                               cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
            report = json.loads((RESULTS / f"{label}.json").read_text())
            if (report["errors"] or "write_timeline" in report or report["rate"] != rate
                    or report["seconds"] != SECONDS or report["mode"] != mode
                    or report["offered"] != rate * SECONDS
                    or report["completed"] + report["dropped"] != report["offered"]
                    or report["maximum_queued"] > 16 * 8
                    or report["hset_coalescing_enabled"] != (mode != "direct")
                    or report["compaction_failures"]):
                raise ValueError("invalid/unmatched offered-load report")
            if identity is None:
                identity = report["publication_identity"]
            if identity != report["publication_identity"]:
                raise ValueError("shared source/runtime changed")
            signature = (report["backend_md5"], report.get("hset_policy_source"))
            if signatures.setdefault(mode, signature) != signature:
                raise ValueError("variant source changed")
            rows.append({"rate": rate, "mode": mode, "trial": trial,
                         **{key: report[key] for key in ("offered", "completed", "completed_in_window", "dropped",
                            "maximum_queued", "drain_seconds", "compactions", "quiet_compactions", "maintenance", "histograms")}})
            print(f"PASS {label}", flush=True)
summary = {"trials": rows, "rates": {}, "seconds": SECONDS, "publication_identity": identity}
for rate in RATES:
    summary["rates"][rate] = {}
    for mode in MODES:
        selected = [row for row in rows if row["rate"] == rate and row["mode"] == mode]
        metrics = {key: [row[key] for row in selected] for key in ("completed_in_window", "dropped", "drain_seconds")}
        for operation in ("write_service_us", "write_total_us", "cycle_total_us", "client_queue_us", "kv_read_us", "hash_read_us"):
            for q in ("p50", "p95", "p99", "p999", "max"):
                metrics[f"{operation}.{q}"] = [row["histograms"][operation][q] for row in selected]
        summary["rates"][rate][mode] = {key: {"median": median(values), "min": min(values), "max": max(values)}
                                         for key, values in metrics.items()}
(RESULTS / f"{PREFIX}-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary["rates"], indent=2))
