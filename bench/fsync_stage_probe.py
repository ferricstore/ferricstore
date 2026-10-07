"""Isolated OS append/fsync probe; component diagnostics, not database throughput."""
import json
import fcntl
import os
import platform
import struct
import tempfile
import threading
import time
import zlib
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PARENT = Path(os.environ.get("BENCH_DATA_PARENT", "/var/folders/n3/4p13zj5n0kjbs8xk2qb4jq080000gn/T/opencode"))
SECONDS = int(os.environ.get("BENCH_SECONDS", "60"))
WORKERS = int(os.environ.get("BENCH_WORKERS", "4"))
CYCLE_MS = int(os.environ.get("BENCH_CYCLE_MS", "20"))
MODE = os.environ.get("BENCH_FD_MODE", "reopen")
SYNC = os.environ.get("BENCH_SYNC", "full" if platform.system() == "Darwin" else "posix")
if not PARENT.is_dir() or MODE not in ("reopen", "persistent"):
    raise ValueError("existing scratch parent and reopen/persistent mode required")
if SYNC == "full" and hasattr(fcntl, "F_FULLFSYNC"):
    sync_file = lambda fd: fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
elif SYNC == "posix":
    sync_file = os.fsync
else:
    raise ValueError("unsupported synchronization mode")


def record(stats, stage, ns):
    us = (ns + 999) // 1000
    bucket = us if us < 1000 else ((us + 999) // 1000) * 1000
    bins, maximum = stats.setdefault(stage, (Counter(), 0))
    bins[bucket] += 1
    stats[stage] = (bins, max(maximum, us))


def summarize(stats):
    result = {}
    for stage, (bins, maximum) in stats.items():
        count = sum(bins.values())
        values = {"count": count, "max_us": maximum}
        for q in (0.5, 0.95, 0.99):
            seen = 0
            for us, n in sorted(bins.items()):
                seen += n
                if seen >= count * q:
                    values[f"p{int(q * 100)}_us"] = us
                    break
        result[stage] = values
    return result


barrier = threading.Barrier(WORKERS + 1)
clock = {}
with tempfile.TemporaryDirectory(prefix="fsync-stage-", dir=PARENT) as directory:
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)

    def worker(index):
        paths = [Path(directory) / f"worker-{index}-{kind}.log" for kind in ("wal", "payload")]
        fds = [os.open(path, flags, 0o600) for path in paths] if MODE == "persistent" else []
        key = f"probe:{index}".encode()
        body = struct.pack("<QQHI", 0, 0, len(key), 4096) + key + bytes(4096)
        data = struct.pack("<I", zlib.crc32(body)) + body
        stats = {}
        slow = []
        cycles = 0
        barrier.wait()
        target = clock["start_ns"] + index * CYCLE_MS * 1_000_000 // WORKERS
        try:
            while time.monotonic_ns() < clock["deadline_ns"]:
                if CYCLE_MS:
                    delay = target - time.monotonic_ns()
                    if delay > 0:
                        time.sleep(delay / 1e9)
                begin = time.monotonic_ns()
                if begin >= clock["deadline_ns"]:
                    break
                for i, path in enumerate(paths):
                    started = time.monotonic_ns()
                    fd = fds[i] if fds else os.open(path, flags, 0o600)
                    for _ in range(3):
                        os.fstat(fd)
                    opened = time.monotonic_ns()
                    pending = memoryview(data)
                    while pending:
                        n = os.write(fd, pending)
                        if n <= 0:
                            raise OSError("append made no progress")
                        pending = pending[n:]
                    written = time.monotonic_ns()
                    sync_file(fd)
                    synced = time.monotonic_ns()
                    if not fds:
                        os.close(fd)
                    closed = time.monotonic_ns()
                    for stage, elapsed in (("open_metadata", opened - started),
                                           ("write", written - opened),
                                           ("fsync", synced - written),
                                           ("close", closed - synced)):
                        record(stats, stage, elapsed)
                    if synced - written >= 100_000_000:
                        slow.append({"worker": index, "file": i,
                                     "started_ns": written, "fsync_us": (synced - written) // 1000})
                        slow = sorted(slow, key=lambda row: row["fsync_us"], reverse=True)[:32]
                record(stats, "cycle", time.monotonic_ns() - begin)
                cycles += 1
                target = max(target, begin) + CYCLE_MS * 1_000_000
            for path in paths:
                if path.stat().st_size != cycles * len(data):
                    raise ValueError("append length mismatch")
            return {"stats": stats, "cycles": cycles, "slow": slow}
        finally:
            for fd in fds:
                os.close(fd)

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(worker, i) for i in range(WORKERS)]
        clock["start_ns"] = time.monotonic_ns()
        clock["deadline_ns"] = clock["start_ns"] + SECONDS * 1_000_000_000
        barrier.wait()
        reports = [future.result() for future in futures]
    elapsed = (time.monotonic_ns() - clock["start_ns"]) / 1e9

combined = {}
for report in reports:
    for stage, (bins, maximum) in report["stats"].items():
        existing, old_max = combined.setdefault(stage, (Counter(), 0))
        existing.update(bins)
        combined[stage] = (existing, max(old_max, maximum))
result = {"mode": MODE, "seconds": SECONDS, "workers": WORKERS, "cycle_ms": CYCLE_MS,
          "sync_mode": SYNC,
          "cycles": sum(report["cycles"] for report in reports), "elapsed_seconds": elapsed,
          "component_only": True, "platform": platform.platform(), "errors": 0,
          "stages": summarize(combined), "slow_fsyncs": [row for report in reports for row in report["slow"]]}
output = os.environ.get("BENCH_OUTPUT", f"bench/results/fsync-stage-{MODE}.json")
(ROOT / output).write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps({key: value for key, value in result.items() if key != "slow_fsyncs"}, indent=2))
