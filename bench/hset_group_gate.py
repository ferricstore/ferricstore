"""Matched fresh-VM foreground group-commit gate; no profiled timings pooled."""
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"
PREFIX = os.environ.get("BENCH_GATE_PREFIX", "hset-group")
CASES = {"saturated": (120, 16, 0), "paced": (60, 16, 100), "low_load": (30, 4, 0)}
TRIALS = ["final"] if "--final-source" in sys.argv else range(1, 4)
for ordinal, trial in enumerate(TRIALS, 1):
    for mode in (("direct", "coalesced") if ordinal % 2 else ("coalesced", "direct")):
        for case, (seconds, clients, cycle) in CASES.items():
            label = f"{PREFIX}-{case}-{mode}-{trial}"
            if (RESULTS / f"{label}.json").exists():
                print(f"EXISTING {label}", flush=True)
                continue
            env = {**os.environ, "ERL_FLAGS": "+S 8:8", "BENCH_HSET_GROUP": mode,
                   "BENCH_SECONDS": str(seconds), "BENCH_CLIENTS": str(clients),
                   "BENCH_CYCLE_MS": str(cycle), "BENCH_TRIAL": str(trial),
                   "BENCH_PROFILE": "0", "BENCH_METRICS": "0", "BENCH_REQUEST_SPANS": "0",
                   "BENCH_COMPACTION_ADMISSION": "parallel", "BENCH_COMPACTION_SYNC": "page",
                   "BENCH_SINGLE_HSET": "atomic", "BENCH_PROMOTED_READ": "protected",
                   "BENCH_OUTPUT": f"bench/results/{label}.json"}
            print(f"START {label}", flush=True)
            with (RESULTS / f"{label}.log").open("w") as log:
                subprocess.run(["mise", "exec", "--", "mix", "run", "--no-start", "bench/sustained_mixed_perf.exs"],
                               cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
            print(f"PASS {label}", flush=True)


def histogram(windows):
    bins = Counter()
    for row in windows:
        bins.update({int(value): n for value, n in row["buckets"].items()})
    count = sum(bins.values())
    result = {"count": count, "max_us": max(row["max_us"] for row in windows)}
    for label, q in (("p50_us", .5), ("p95_us", .95), ("p99_us", .99), ("p999_us", .999)):
        seen = 0
        for value, n in sorted(bins.items()):
            seen += n
            if seen >= count * q:
                result[label] = value
                break
    return result


rows, signatures = [], {}
identity = None
for case, (seconds, clients, cycle) in CASES.items():
    for mode in ("direct", "coalesced"):
        for trial in TRIALS:
            report = json.loads((RESULTS / f"{PREFIX}-{case}-{mode}-{trial}.json").read_text())
            if (report["errors"] or report["hset_group"] != mode or report["seconds"] != seconds
                    or report["clients"] != clients or report["cycle_period_ms"] != cycle
                    or report["single_hset_variant"] != "atomic" or report["compaction_sync"] != "page"
                    or report["compaction_admission"] != "parallel" or report["request_spans_enabled"]
                    or report.get("hset_coalescing_enabled", mode == "coalesced") != (mode == "coalesced")
                    or "profile" in report or "waraft_metrics" in report):
                raise ValueError("invalid/unmatched group-commit gate")
            if identity is None:
                identity = report["publication_identity"]
            if identity != report["publication_identity"]:
                raise ValueError("shared source or runtime changed")
            signature = (report["hset_group_source"], report["backend_beam_md5"], report["promoted_beam_md5"])
            if signatures.setdefault(mode, signature) != signature:
                raise ValueError("variant source/compiled module changed")
            operations = {kind: histogram([row for row in report["windows"] if row["operation"] == kind])
                          for kind in ("hash_write", "hash_read", "kv_read", "cycle")}
            rows.append({"case": case, "mode": mode, "trial": trial, "operations": operations,
                         "ops_per_second": sum(operations[k]["count"] for k in ("hash_write", "hash_read", "kv_read"))
                                           / report["measured_elapsed_seconds"],
                         "compactions": sum(event["event"] == "compaction" for event in report["events"]),
                         "quiet_compactions": len(report["after_quiet_events"]), "maintenance": report["compaction_status"]})

summary = {"trials": rows, "cases": {}, "publication_identity": identity}
for case in CASES:
    summary["cases"][case] = {}
    for mode in ("direct", "coalesced"):
        selected = [row for row in rows if row["case"] == case and row["mode"] == mode]
        metrics = {"ops_per_second": [row["ops_per_second"] for row in selected]}
        for kind in ("hash_write", "hash_read", "kv_read", "cycle"):
            for metric in ("p50_us", "p95_us", "p99_us", "p999_us", "max_us"):
                metrics[f"{kind}_{metric}"] = [row["operations"][kind][metric] for row in selected]
        summary["cases"][case][mode] = {key: {"median": median(values), "min": min(values), "max": max(values)}
                                       for key, values in metrics.items()}
(RESULTS / f"{PREFIX}-gate-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary["cases"], indent=2))
