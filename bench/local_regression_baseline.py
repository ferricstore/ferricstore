#!/usr/bin/env python3
"""Run FerricStore local regression baselines.

This runner exists to keep benchmark shape stable across cleanup/refactor work:

* memtier SET/GET baseline uses Ferric protocol and the historical 200 clients x 4
  threads x pipeline 50 shape.
* DBOS-style Flow baseline delegates to the Python SDK benchmark script with
  the optimized queue settings we use for release checks.

It intentionally does not benchmark the Ferric protocol. Native protocol
needs a dedicated SDK/benchmark client so results are not mixed with another protocol.
"""

from __future__ import annotations

import argparse
import ast
import datetime as _dt
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import sys
import time
from typing import Any
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = Path("/tmp")
DEFAULT_SDK_REPO = Path("/Users/yoavgea/repos/ferricstore-python")

LOCAL_BASELINES = {
    "memtier_set": "756,799/s, p50 52.479 ms, p99 70.143 ms",
    "memtier_get": "5,102,710/s, p50 7.743 ms, p99 11.455 ms",
    "dbos_e2e": "73,355 flows/s, create 84,826/s, process 73,363/s",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local FerricStore memtier and DBOS-style baselines."
    )
    parser.add_argument("--suite", choices=("all", "memtier", "dbos"), default="all")
    parser.add_argument("--url", default="ferric://127.0.0.1:6388")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--start-server", action="store_true")
    parser.add_argument("--server-repo", type=Path, default=ROOT)
    parser.add_argument("--python-sdk-repo", type=Path, default=DEFAULT_SDK_REPO)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--server-shards", type=int, default=16)
    parser.add_argument("--server-log-level", default="warning")
    parser.add_argument("--server-start-timeout-s", type=float, default=60.0)
    parser.add_argument("--dry-run", action="store_true")

    parser.add_argument("--memtier-bin", default="memtier_benchmark")
    parser.add_argument("--memtier-clients", type=int, default=200)
    parser.add_argument("--memtier-threads", type=int, default=4)
    parser.add_argument("--memtier-pipeline", type=int, default=50)
    parser.add_argument("--memtier-test-time", type=int, default=30)
    parser.add_argument("--memtier-data-size", type=int, default=256)
    parser.add_argument("--memtier-key-maximum", type=int, default=1_000_000)

    parser.add_argument("--flows", type=int, default=1_000_000)
    parser.add_argument("--dbos-workers", type=int, default=16)
    parser.add_argument("--dbos-producers", type=int, default=32)
    parser.add_argument("--dbos-partitions", type=int, default=16)
    parser.add_argument("--claim-batch-size", type=int, default=500)
    parser.add_argument("--claim-partition-batch-size", type=int, default=4)
    parser.add_argument("--create-batch-size", type=int, default=500)
    parser.add_argument("--complete-async-depth", type=int, default=4)
    parser.add_argument("--transport", default="many")
    parser.add_argument(
        "--adaptive-producer-backpressure",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Enable producer latency protection for DBOS runs. Disabled by default "
            "because this regression runner measures max-throughput baseline."
        ),
    )
    return parser.parse_args()


def resolve_endpoint(args: argparse.Namespace) -> tuple[str, int]:
    parsed = urlparse(args.url)
    host = args.host or parsed.hostname or "127.0.0.1"
    port = args.port or parsed.port or 6379
    return host, port


def shell_join(argv: list[str | os.PathLike[str]]) -> str:
    return " ".join(shlex.quote(str(part)) for part in argv)


def output_dir(args: argparse.Namespace) -> Path:
    if args.output_dir is not None:
        return args.output_dir

    stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return DEFAULT_OUTPUT_ROOT / f"ferricstore-local-baseline-{stamp}"


def port_is_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.25)
        return sock.connect_ex((host, port)) == 0


def wait_for_port(host: str, port: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if port_is_open(host, port):
            return
        time.sleep(0.1)

    raise RuntimeError(f"server did not open {host}:{port} within {timeout_s:.1f}s")


def start_server(
    args: argparse.Namespace,
    out_dir: Path,
    group_name: str,
    host: str,
    port: int,
) -> subprocess.Popen[Any]:
    if port_is_open(host, port):
        raise RuntimeError(
            f"{host}:{port} is already open; stop it or omit --start-server"
        )

    data_dir = out_dir / f"server-data-{group_name}"
    data_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / f"server-{group_name}.log"
    log_file = log_path.open("wb")

    env = os.environ.copy()
    env.update(
        {
            "MIX_ENV": "prod",
            "FERRICSTORE_PORT": str(port),
            "FERRICSTORE_DATA_DIR": str(data_dir),
            "FERRICSTORE_LOG_LEVEL": args.server_log_level,
            "FERRICSTORE_SHARD_COUNT": str(args.server_shards),
            "FERRICSTORE_PROTECTED_MODE": "false",
        }
    )

    proc = subprocess.Popen(
        ["mix", "run", "--no-halt"],
        cwd=args.server_repo,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )

    try:
        wait_for_port(host, port, args.server_start_timeout_s)
    except Exception:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        raise

    return proc


def stop_server(proc: subprocess.Popen[Any] | None) -> None:
    if proc is None:
        return

    if proc.poll() is not None:
        return

    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=15)


def run_command(
    name: str,
    argv: list[str],
    cwd: Path,
    out_dir: Path,
    commands: dict[str, str],
    dry_run: bool,
) -> str:
    command = shell_join(argv)
    commands[name] = f"cd {cwd}\n{command}"

    log_path = out_dir / f"{name}.log"
    if dry_run:
        log_path.write_text(f"[dry-run]\n{command}\n", encoding="utf-8")
        return ""

    with log_path.open("wb") as log_file:
        completed = subprocess.run(
            argv,
            cwd=cwd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            check=False,
        )

    text = log_path.read_text(encoding="utf-8", errors="replace")
    if completed.returncode != 0:
        raise RuntimeError(
            f"{name} failed with exit code {completed.returncode}; see {log_path}"
        )

    return text


def memtier_command(
    args: argparse.Namespace,
    host: str,
    port: int,
    command: str,
) -> list[str]:
    return [
        args.memtier_bin,
        "-s",
        host,
        "-p",
        str(port),
        "--protocol=resp3",
        "--clients",
        str(args.memtier_clients),
        "--threads",
        str(args.memtier_threads),
        "--pipeline",
        str(args.memtier_pipeline),
        "--test-time",
        str(args.memtier_test_time),
        "--command",
        command,
        "--command-key-pattern",
        "R",
        "--key-minimum",
        "1",
        "--key-maximum",
        str(args.memtier_key_maximum),
        "--hide-histogram",
    ]


def memtier_set_command(args: argparse.Namespace, host: str, port: int) -> list[str]:
    argv = memtier_command(args, host, port, "SET bench:__key__ __data__")
    argv.extend(["--data-size", str(args.memtier_data_size)])
    return argv


def memtier_get_command(args: argparse.Namespace, host: str, port: int) -> list[str]:
    return memtier_command(args, host, port, "GET bench:__key__")


def dbos_command(args: argparse.Namespace) -> list[str]:
    venv_python = args.python_sdk_repo / ".venv" / "bin" / "python"
    python = str(venv_python if venv_python.exists() else Path(sys.executable))

    argv = [
        python,
        "examples/dbos_style_benchmark.py",
        "--url",
        args.url,
        "--mode",
        "queued",
        "--transport",
        args.transport,
        "--queued-shape",
        "live",
        "--partition-mode",
        "auto",
        "--worker-mode",
        "polling",
        "--worker-api",
        "lowlevel",
        "--flows",
        str(args.flows),
        "--workers",
        str(args.dbos_workers),
        "--producers",
        str(args.dbos_producers),
        "--partitions",
        str(args.dbos_partitions),
        "--claim-batch-size",
        str(args.claim_batch_size),
        "--claim-partition-batch-size",
        str(args.claim_partition_batch_size),
        "--create-batch-size",
        str(args.create_batch_size),
        "--complete-async-depth",
        str(args.complete_async_depth),
        "--server-shards",
        str(args.server_shards),
        "--claim-job-only",
    ]
    if args.adaptive_producer_backpressure:
        argv.append("--adaptive-producer-backpressure")
    else:
        argv.append("--no-adaptive-producer-backpressure")
    return argv


def parse_memtier_totals(text: str) -> str | None:
    for line in reversed(text.splitlines()):
        if "Totals" in line:
            return " ".join(line.split())
    return None


def parse_dbos_result(text: str) -> dict[str, Any] | None:
    for line in reversed(text.splitlines()):
        stripped = line.strip()
        if not stripped.startswith("{") or not stripped.endswith("}"):
            continue

        try:
            parsed = ast.literal_eval(stripped)
        except (SyntaxError, ValueError):
            continue

        if isinstance(parsed, dict):
            return parsed

    return None


def format_dbos_summary(result: dict[str, Any] | None) -> str:
    if not result:
        return "not parsed"

    fields = [
        "e2e_workflows_per_s",
        "create_flows_per_s",
        "process_workflows_per_s",
        "total_s",
        "create_s",
        "process_s",
        "rss_max_gb",
    ]
    parts = []
    for field in fields:
        if field in result:
            parts.append(f"{field}={result[field]}")
    return ", ".join(parts) if parts else str(result)


def write_summary(
    out_dir: Path,
    args: argparse.Namespace,
    commands: dict[str, str],
    parsed: dict[str, Any],
) -> None:
    lines = [
        "# FerricStore local regression baseline run",
        "",
        f"- Time: {_dt.datetime.now().isoformat(timespec='seconds')}",
        f"- FerricStore repo: `{args.server_repo}`",
        f"- Python SDK repo: `{args.python_sdk_repo}`",
        f"- Output dir: `{out_dir}`",
        f"- Server shards: `{args.server_shards}`",
        f"- Suite: `{args.suite}`",
        f"- Server managed by runner: `{args.start_server}`",
        "",
        "## Historical local baseline",
        "",
        f"- memtier SET: {LOCAL_BASELINES['memtier_set']}",
        f"- memtier GET: {LOCAL_BASELINES['memtier_get']}",
        f"- DBOS-style Flow: {LOCAL_BASELINES['dbos_e2e']}",
        "",
        "## Current run summary",
        "",
        f"- memtier SET totals: {parsed.get('memtier_set') or 'not run/not parsed'}",
        f"- memtier GET totals: {parsed.get('memtier_get') or 'not run/not parsed'}",
        f"- DBOS-style Flow: {format_dbos_summary(parsed.get('dbos'))}",
        "",
        "## Commands",
        "",
    ]

    for name, command in commands.items():
        lines.extend([f"### {name}", "", "```bash", command, "```", ""])

    lines.extend(
        [
            "## Notes",
            "",
            "- Benchmarks use the Ferric protocol data plane.",
            "- DBOS-style Flow uses the Python SDK benchmark path.",
            "- Native protocol needs a dedicated SDK/client adapter before it can be compared fairly.",
            "",
        ]
    )

    (out_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_memtier(
    args: argparse.Namespace,
    out_dir: Path,
    commands: dict[str, str],
    host: str,
    port: int,
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    set_text = run_command(
        "memtier-set",
        memtier_set_command(args, host, port),
        args.server_repo,
        out_dir,
        commands,
        args.dry_run,
    )
    results["memtier_set"] = parse_memtier_totals(set_text)

    get_text = run_command(
        "memtier-get",
        memtier_get_command(args, host, port),
        args.server_repo,
        out_dir,
        commands,
        args.dry_run,
    )
    results["memtier_get"] = parse_memtier_totals(get_text)
    return results


def run_dbos(
    args: argparse.Namespace,
    out_dir: Path,
    commands: dict[str, str],
) -> dict[str, Any]:
    text = run_command(
        "dbos-style-flow",
        dbos_command(args),
        args.python_sdk_repo,
        out_dir,
        commands,
        args.dry_run,
    )
    return {"dbos": parse_dbos_result(text)}


def run_with_optional_server(
    args: argparse.Namespace,
    out_dir: Path,
    group_name: str,
    callback: Any,
    host: str,
    port: int,
    commands: dict[str, str],
) -> dict[str, Any]:
    if args.dry_run:
        return callback()

    proc: subprocess.Popen[Any] | None = None
    try:
        if args.start_server:
            proc = start_server(args, out_dir, group_name, host, port)
        return callback()
    finally:
        stop_server(proc)


def main() -> int:
    args = parse_args()
    host, port = resolve_endpoint(args)
    out_dir = output_dir(args)
    out_dir.mkdir(parents=True, exist_ok=True)

    commands: dict[str, str] = {}
    parsed: dict[str, Any] = {}

    if args.suite in ("all", "memtier"):
        parsed.update(
            run_with_optional_server(
                args,
                out_dir,
                "memtier",
                lambda: run_memtier(args, out_dir, commands, host, port),
                host,
                port,
                commands,
            )
        )

    if args.suite in ("all", "dbos"):
        parsed.update(
            run_with_optional_server(
                args,
                out_dir,
                "dbos",
                lambda: run_dbos(args, out_dir, commands),
                host,
                port,
                commands,
            )
        )

    write_summary(out_dir, args, commands, parsed)
    print(f"summary: {out_dir / 'summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
