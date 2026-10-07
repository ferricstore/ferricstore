"""Matched fixed offered-load metadata routing controls; durable write policy held constant."""
import json
import os
import subprocess
import sys
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"
PREFIX = os.environ.get("BENCH_GATE_PREFIX", "metadata-route")
SECONDS = int(os.environ.get("BENCH_SECONDS", "120"))
TRIALS = [1] if "--pilot" in sys.argv else range(1, 4)
rows, signatures = [], {}
identity = None
for trial in TRIALS:
    for rate in (100, 200, 300):
        for route in (("server", "raw") if trial % 2 else ("raw", "server")):
            label = f"{PREFIX}-{rate}-{route}-{trial}"
            env = {**os.environ, "ERL_FLAGS": "+S 8:8", "BENCH_METADATA_ROUTE": route,
                   "BENCH_HSET_GROUP": "direct", "BENCH_CYCLES_PER_SECOND": str(rate),
                   "BENCH_SECONDS": str(SECONDS), "BENCH_WARMUP_SECONDS": "10", "BENCH_CLIENTS": "16",
                   "BENCH_QUEUE_DEPTH": "8", "BENCH_FIELDS": "4096", "BENCH_WRITE_TIMELINE": "0",
                   "BENCH_OUTPUT": f"bench/results/{label}.json"}
            print(f"START {label}", flush=True)
            with (RESULTS / f"{label}.log").open("w") as log:
                subprocess.run(["mise", "exec", "--", "mix", "run", "--no-start", "bench/offered_mixed_perf.exs"],
                               cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600)
            report = json.loads((RESULTS / f"{label}.json").read_text())
            if (report["errors"] or "write_timeline" in report or report["metadata_route"] != route
                    or report["mode"] != "direct" or report["hset_coalescing_enabled"]
                    or report["offered"] != rate * SECONDS
                    or report["completed"] + report["dropped"] != report["offered"]
                    or report["compaction_failures"]):
                raise ValueError("invalid metadata routing control")
            if identity is None:
                identity = report["publication_identity"]
            if identity != report["publication_identity"]:
                raise ValueError("shared source/runtime changed")
            signature = (report["metadata_source"], report["promotion_source"], report["segment_log_md5"], report["promotion_md5"])
            if signatures.setdefault(route, signature) != signature:
                raise ValueError("variant source changed")
            rows.append({"rate": rate, "route": route, "trial": trial,
                         **{key: report[key] for key in ("completed_in_window", "dropped", "drain_seconds",
                            "compactions", "quiet_compactions", "maintenance", "histograms")}})
            print(f"PASS {label}", flush=True)
summary = {"trials": rows, "rates": {}, "seconds": SECONDS, "publication_identity": identity}
for rate in (100, 200, 300):
    summary["rates"][rate] = {}
    for route in ("server", "raw"):
        selected = [row for row in rows if row["rate"] == rate and row["route"] == route]
        metrics = {key: [row[key] for row in selected] for key in ("completed_in_window", "dropped", "drain_seconds")}
        for metric in ("write_service_us", "write_total_us", "client_queue_us"):
            for q in ("p50", "p95", "p99", "p999", "max"):
                metrics[f"{metric}.{q}"] = [row["histograms"][metric][q] for row in selected]
        summary["rates"][rate][route] = {key: {"median": median(values), "min": min(values), "max": max(values)}
                                          for key, values in metrics.items()}
(RESULTS / f"{PREFIX}-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary["rates"], indent=2))
