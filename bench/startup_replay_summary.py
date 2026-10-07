"""Summarize the completed startup and live-request comparisons."""

import json
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent


def stats(values):
    return {"median": median(values), "min": min(values), "max": max(values)}


def main():
    startup = json.loads((ROOT / "bench/results/startup-replay-comparison.json").read_text())
    live = json.loads((ROOT / "bench/results/startup-replay-live-comparison.json").read_text())
    if startup["image_ids"] != live["image_ids"]:
        raise ValueError("image mismatch between startup and request runs")

    reference = startup["results"][0]["startup"]["keydir_fingerprints"]
    comparisons = []
    for run in startup["results"]:
        fingerprints = run["startup"]["keydir_fingerprints"]
        comparisons.append({"variant": run["variant"], "trial": run["trial"],
                            "key_expiry_match": fingerprints == reference,
                            "differing_shards": [i for i, (a, b) in enumerate(zip(fingerprints, reference)) if a != b],
                            "counts_by_shard": [item["count"] for item in fingerprints],
                            "rows": sum(item["count"] for item in fingerprints)})

    result = {"image_ids": startup["image_ids"], "keydir_comparisons": comparisons,
              "startup": {}, "live": {}}
    for variant in ("baseline", "candidate"):
        runs = [row for row in startup["results"] if row["variant"] == variant]
        if sorted(row["trial"] for row in runs) != [1, 2, 3]:
            raise ValueError("expected three completed startup runs per variant")
        result["startup"][variant] = {
            "ready_seconds": stats([row["startup"]["ready_ms"] / 1000 for row in runs]),
            "peak_gib": stats([row["startup"]["startup_cgroup_peak_bytes"] / 1024**3 for row in runs]),
            "phases": {phase: stats([row["startup"]["phases"][phase]["sum_ms"] / 1000 for row in runs])
                       for phase in runs[0]["startup"]["phases"]},
        }
        requests = [row for row in live["results"] if row["variant"] == variant]
        if sorted(row["trial"] for row in requests) != [1, 2, 3]:
            raise ValueError("expected three completed request runs per variant")
        result["live"][variant] = {}
        for concurrency in (1, 16):
            for scenario in ("put_get", "empty_flow_claim"):
                entries = [entry for run in requests for entry in run["live"]
                           if entry["concurrency"] == concurrency and entry["scenario"] == scenario]
                result["live"][variant][f"{scenario}/{concurrency}"] = {
                    metric: stats([entry[metric] for entry in entries])
                    for metric in ("ops_per_second", "p95_us", "p99_us")
                }

    confirm_path = ROOT / "bench/results/startup-replay-live-confirm.json"
    if confirm_path.exists():
        confirm = json.loads(confirm_path.read_text())
        result["long_flow_confirmation"] = {}
        for variant in ("baseline", "candidate"):
            rows = [entry for run in confirm["results"] if run["variant"] == variant
                    for entry in run["live"]]
            result["long_flow_confirmation"][variant] = {
                metric: stats([row[metric] for row in rows])
                for metric in ("ops_per_second", "p95_us", "p99_us")}

    diagnostic_path = ROOT / "bench/results/startup-replay-fingerprint-check.json"
    if diagnostic_path.exists():
        diagnostic = json.loads(diagnostic_path.read_text())
        runs = diagnostic["results"]
        active = lambda row: [(shard["live_count"], shard["live_key_expiry_digest"])
                              for shard in row["startup"]["keydir_fingerprints"]]
        result["expiry_diagnostic"] = {
            "live_key_expiry_match": all(active(row) == active(runs[0]) for row in runs),
            "runs": [{"variant": row["variant"],
                      "counts_by_shard": [shard["live_count"] for shard in row["startup"]["keydir_fingerprints"]],
                      "live_count": sum(shard["live_count"] for shard in row["startup"]["keydir_fingerprints"]),
                      "expired_count": sum(shard["expired_count"] for shard in row["startup"]["keydir_fingerprints"])}
                     for row in runs]}

    quiet_path = ROOT / "bench/results/startup-replay-quiescent-check.json"
    if quiet_path.exists():
        quiet = json.loads(quiet_path.read_text())["results"]
        result["quiescent_diagnostic"] = {
            "fingerprints_match": all(row["startup"]["keydir_fingerprints"] == quiet[0]["startup"]["keydir_fingerprints"] for row in quiet),
            "runs": [{"variant": row["variant"], "ready_ms": row["startup"]["ready_ms"],
                      "rows": sum(s["count"] for s in row["startup"]["keydir_fingerprints"]),
                      "phases": {name: value["sum_ms"] for name, value in row["startup"]["phases"].items()
                                 if name.endswith(("recover_segment_projected_keydir", "reconcile_flow_lmdb", "flow_history_projector_recover", "build_state"))}}
                     for row in quiet]}

    path = ROOT / "bench/results/startup-replay-summary.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    for variant, data in result["startup"].items():
        print(variant, "ready seconds", data["ready_seconds"], "peak GiB", data["peak_gib"])
        for phase in ("recover_segment_projected_keydir", "reconcile_flow_lmdb"):
            print(variant, phase, data["phases"][f"ferricstore:waraft:storage:startup_phase:{phase}"])
    print("Key/expiry comparisons:", json.dumps(comparisons))
    print("Request measurements:", json.dumps(result["live"], indent=2))
    print("Long Flow confirmation:", json.dumps(result.get("long_flow_confirmation"), indent=2))
    print("Expiry diagnostic:", json.dumps(result.get("expiry_diagnostic")))
    print("Quiescent diagnostic:", json.dumps(result.get("quiescent_diagnostic"), indent=2))


if __name__ == "__main__":
    main()
