"""Serial matched read/write matrix for the public atomic single-field HSET path."""
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"
VARIANTS = {
    "legacy_serialized": ("baseline", "legacy"),
    "legacy_cached": ("protected", "legacy"),
    "atomic_cached": ("protected", "atomic"),
}
COHORTS = {"saturated": (120, 0), "paced": (60, 100)}
final_source = "--final-source" in sys.argv or "--summarize-final" in sys.argv
trials = ["final"] if final_source else range(1, 4)

if "--summarize-only" not in sys.argv and "--summarize-final" not in sys.argv:
    for trial in trials:
        order = list(VARIANTS) if final_source or trial % 2 else list(reversed(VARIANTS))
        for variant in order:
            read, write = VARIANTS[variant]
            for cohort, (seconds, cycle) in COHORTS.items():
                label = f"saturated-write-{cohort}-{variant}-{trial}"
                if (RESULTS / f"{label}.json").exists():
                    if "--resume" not in sys.argv:
                        raise ValueError(f"report already exists: {label}; use --resume")
                    print(f"EXISTING {label} (validated during aggregation)", flush=True)
                    continue
                env = {**os.environ,
                    "ERL_FLAGS": "+S 8:8", "BENCH_SINGLE_HSET": write,
                    "BENCH_PROMOTED_READ": read, "BENCH_TRIAL": str(trial),
                    "BENCH_SECONDS": str(seconds), "BENCH_CYCLE_MS": str(cycle),
                    "BENCH_CLIENTS": "16", "BENCH_FIELDS": "4096", "BENCH_WARMUP_SECONDS": "10",
                    "BENCH_COMPACTION": "auto", "BENCH_PROFILE": "0",
                    "BENCH_METRICS": "0", "BENCH_REQUEST_SPANS": "0",
                    "BENCH_OUTPUT": f"bench/results/{label}.json"}
                print(f"START {label}", flush=True)
                log_path = RESULTS / f"{label}.log"
                attempt = 0
                while log_path.exists():
                    attempt += 1
                    log_path = RESULTS / f"{label}.attempt-{attempt}.log"
                with log_path.open("w") as log:
                    subprocess.run(["mise", "exec", "--", "mix", "run", "--no-start",
                                    "bench/sustained_mixed_perf.exs"], cwd=ROOT, env=env,
                                   stdout=log, stderr=subprocess.STDOUT, check=True)
                print(f"PASS {label}", flush=True)


def percentiles(windows):
    bins = Counter()
    for window in windows:
        bins.update({int(value): n for value, n in window["buckets"].items()})
    count = sum(bins.values())
    stats = {"count": count, "max_us": max(window["max_us"] for window in windows)}
    for q in (0.5, 0.95, 0.99):
        seen = 0
        for value, n in sorted(bins.items()):
            seen += n
            if seen >= count * q:
                stats[f"p{int(q * 100)}_us"] = value
                break
    return stats


identity = None
sources = {}
rows = []
for cohort, (seconds, cycle) in COHORTS.items():
    for variant, (read, write) in VARIANTS.items():
        for trial in trials:
            label = f"saturated-write-{cohort}-{variant}-{trial}"
            report = json.loads((RESULTS / f"{label}.json").read_text())
            if (report["errors"] or report["promoted_read_variant"] != read
                    or report["single_hset_variant"] != write or report["trial"] != str(trial)
                    or report["seconds"] != seconds or report["cycle_period_ms"] != cycle
                    or report["mode"] != "auto" or report["clients"] != 16
                    or report["fields_per_hash"] != 4096 or report["warmup_seconds"] != 10
                    or report["request_spans_enabled"] or "profile" in report or "waraft_metrics" in report):
                raise ValueError(f"invalid/unmatched report: {label}")
            current_identity = report["publication_identity"]
            if identity is None:
                identity = current_identity
            if identity != current_identity:
                raise ValueError("shared publication source, compiled modules or runtime changed")
            signatures = (report["router_source"], report["router_beam_md5"],
                          report["hash_source"], report["hash_beam_md5"])
            if sources.setdefault(variant, signatures) != signatures:
                raise ValueError("variant source or compiled modules changed")
            operations = {kind: percentiles([window for window in report["windows"]
                                            if window["operation"] == kind])
                          for kind in ("hash_write", "hash_read", "kv_read", "cycle")}
            rows.append({"cohort": cohort, "variant": variant, "trial": trial,
                         "operations": operations,
                         "ops_per_second": sum(operations[kind]["count"]
                                               for kind in ("hash_write", "hash_read", "kv_read"))
                                           / report["measured_elapsed_seconds"],
                         "compactions": sum(event["event"] == "compaction" for event in report["events"]),
                         "compaction_failures": sum(event["event"] == "compaction_failed" for event in report["events"])})

summary = {"trials": rows, "cohorts": {}, "publication_identity": identity,
           "checksums": {variant: {"router": source[1], "hash": source[3]} for variant, source in sources.items()}}
for cohort in COHORTS:
    summary["cohorts"][cohort] = {}
    for variant in VARIANTS:
        selected = [row for row in rows if row["cohort"] == cohort and row["variant"] == variant]
        values = {"ops_per_second": [row["ops_per_second"] for row in selected]}
        for kind in ("hash_write", "hash_read", "kv_read", "cycle"):
            for metric in ("p50_us", "p95_us", "p99_us", "max_us"):
                values[f"{kind}_{metric}"] = [row["operations"][kind][metric] for row in selected]
        summary["cohorts"][cohort][variant] = {
            metric: {"median": median(samples), "min": min(samples), "max": max(samples)}
            for metric, samples in values.items()}
output = "saturated-write-gate-final-summary.json" if final_source else "saturated-write-gate-summary.json"
(RESULTS / output).write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary["cohorts"], indent=2))
