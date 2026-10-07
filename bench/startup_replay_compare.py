"""Compare fresh clones of the isolated 16-shard recovery fixture.

Build the baseline and candidate images before running. The source volume is
mounted read-only; every timed run gets a newly copied, disposable volume.
"""

import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent
DOCKER = os.environ.get("DOCKER", "/Applications/Docker.app/Contents/Resources/bin/docker")
SOURCE = "ferricstore-offset-consistent-20260923"
IMAGES = {
    "baseline": "ferricstore-startup-review:baseline-20260928",
    "candidate": "ferricstore-startup-review:raw-metadata-20260928",
}
MODE = os.environ.get("BENCH_MODE", "startup")
if MODE not in ("startup", "live"):
    raise ValueError("BENCH_MODE must be startup or live")
OUTPUT = ROOT / os.environ.get("BENCH_OUTPUT", "bench/results/startup-replay-comparison.json" if MODE == "startup"
                               else "bench/results/startup-replay-live-comparison.json")
LOGS = ROOT / "bench/output/startup-replay"


def docker(*args, check=True, timeout=900):
    return subprocess.run([DOCKER, *args], check=check, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)


def interrupted(_signum, _frame):
    raise KeyboardInterrupt


def run_one(variant, trial):
    name = f"ferricstore-startup-review-{MODE}-{variant}-{trial}-{uuid.uuid4().hex[:8]}"
    volume = name + "-data"
    result = {"variant": variant, "trial": trial, "image": IMAGES[variant], "mode": MODE}
    try:
        docker("volume", "create", "--label", "purpose=ferricstore-startup-review", volume)
        copied = time.monotonic()
        if MODE == "startup":
            docker("run", "--rm", "--network", "none", "--user", "0",
                   "--mount", f"type=volume,source={SOURCE},target=/source,readonly",
                   "--mount", f"type=volume,source={volume},target=/clone",
                   "--entrypoint", "/bin/sh", IMAGES["baseline"], "-c",
                   "cp -a --reflink=auto /source/. /clone/ && sync -f /clone")
        result["clone_seconds"] = time.monotonic() - copied
        print(f"CLONED {variant}/{trial} in {result['clone_seconds']:.1f}s", flush=True)
        time.sleep(3)
        docker("run", "--detach", "--name", name, "--hostname", "inspeactor-ferricstore",
               "--memory", "6g", "--network", "none",
               "--mount", f"type=volume,source={volume},target=/data",
               "--mount", f"type=bind,source={ROOT / 'bench/startup_replay_observer.exs'},target=/observer.exs,readonly",
               "--env", "FERRICSTORE_SHARD_COUNT=16",
               "--env", f"BENCH_MODE={MODE}",
               "--env", f"BENCH_FLOW_CONFIRM={os.environ.get('BENCH_FLOW_CONFIRM', '0')}",
               "--env", f"BENCH_CAPTURE_KEYS={os.environ.get('BENCH_CAPTURE_KEYS', '0')}",
               "--env", f"BENCH_QUIESCENT={os.environ.get('BENCH_QUIESCENT', '0')}",
               "--env", "RELEASE_NODE=ferricstore@inspeactor-ferricstore",
               "--env", "ERL_AFLAGS=-sname ferricstore@inspeactor-ferricstore", IMAGES[variant],
               "bin/ferricstore", "eval", 'Code.eval_file("/observer.exs")')
        started = time.monotonic()
        printed_startup = False
        while time.monotonic() - started < 600:
            time.sleep(2)
            captured = docker("logs", name)
            logs = captured.stdout + captured.stderr
            if not printed_startup:
                for line in logs.splitlines():
                    if line.startswith("STARTUP_BENCH "):
                        startup = json.loads(line.removeprefix("STARTUP_BENCH "))
                        print(f"READY {variant}/{trial}: {startup['ready_ms'] / 1000:.3f}s", flush=True)
                        printed_startup = True
            for line in logs.splitlines():
                if line.startswith("STARTUP_BENCH_COMPLETE "):
                    result.update(json.loads(line.removeprefix("STARTUP_BENCH_COMPLETE ")))
                    LOGS.mkdir(parents=True, exist_ok=True)
                    (LOGS / f"{OUTPUT.stem}-{variant}-{trial}.log").write_text(logs)
                    if os.environ.get("BENCH_CAPTURE_KEYS") == "1":
                        target = LOGS / f"{OUTPUT.stem}-{variant}-{trial}.tsv"
                        docker("cp", f"{name}:/data/benchmark-keydir.tsv", str(target))
                    state = json.loads(docker("inspect", name).stdout)[0]["State"]
                    if state["OOMKilled"]:
                        raise RuntimeError("benchmark container OOM-killed")
                    print(json.dumps({"mode": MODE, "variant": variant, "trial": trial,
                                      "ready_seconds": result["startup"]["ready_ms"] / 1000,
                                      "peak_gib": result["startup"]["startup_cgroup_peak_bytes"] / 1024**3,
                                      "live": result["live"]}), flush=True)
                    return result
            state = json.loads(docker("inspect", name).stdout)[0]["State"]
            if not state["Running"]:
                raise RuntimeError(f"benchmark exited without results: {state}\n{logs[-12000:]}")
        raise TimeoutError(f"benchmark startup/request timeout: {name}")
    finally:
        docker("rm", "--force", name, check=False)
        docker("volume", "rm", volume, check=False)


def main():
    signal.signal(signal.SIGTERM, interrupted)
    docker("volume", "inspect", SOURCE)
    image_ids = {name: json.loads(docker("image", "inspect", tag).stdout)[0]["Id"]
                 for name, tag in IMAGES.items()}
    data = json.loads(OUTPUT.read_text()) if OUTPUT.exists() else {
        "source_volume": SOURCE if MODE == "startup" else None, "mode": MODE,
        "image_ids": image_ids, "memory_limit_bytes": 6 * 1024**3,
        "fresh_clone_each_run": MODE == "startup", "fresh_data_each_run": True, "results": []}
    if data["image_ids"] != image_ids:
        raise RuntimeError("existing results use different images; choose a fresh results file")
    completed = {(r["variant"], r["trial"]) for r in data["results"]}
    order = os.environ.get("BENCH_RUNS", "baseline:1 candidate:1 candidate:2 baseline:2 baseline:3 candidate:3")
    for spec in order.split():
        variant, trial_text = spec.split(":")
        trial = int(trial_text)
        if (variant, trial) in completed:
            continue
        data["results"].append(run_one(variant, trial))
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_text(json.dumps(data, indent=2) + "\n")


if __name__ == "__main__":
    main()
