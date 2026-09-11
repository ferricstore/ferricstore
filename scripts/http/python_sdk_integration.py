#!/usr/bin/env python3
"""Exercise the real Python SDK against the in-process FerricStore HTTP server."""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
import pathlib
import re
import ssl
import threading
import urllib.error
import urllib.request
import uuid
from typing import Any
from urllib.parse import urlsplit

import ferricstore
from ferricstore import (
    AsyncFlowClient,
    FerricStoreError,
    FlowClient,
    HttpError,
    JsonCodec,
)

HTTP_URL = os.environ["FERRICSTORE_HTTP_INTEGRATION_URL"]
USERNAME = os.environ["FERRICSTORE_HTTP_INTEGRATION_USERNAME"]
PASSWORD = os.environ["FERRICSTORE_HTTP_INTEGRATION_PASSWORD"]
KEY_PREFIX = os.environ.get("FERRICSTORE_HTTP_INTEGRATION_KEY_PREFIX", "python:http")
CA_FILE = os.environ.get("FERRICSTORE_HTTP_INTEGRATION_CA_FILE")
SSL_CONTEXT = (
    ssl.create_default_context(cafile=CA_FILE) if HTTP_URL.startswith("https://") else None
)


def validate_loopback_url() -> None:
    parsed = urlsplit(HTTP_URL)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise ValueError("integration URL must use HTTP(S) on loopback")


def fetch(path: str) -> tuple[int, str]:
    try:
        # The base URL is restricted to HTTP(S) loopback by validate_loopback_url().
        with urllib.request.urlopen(  # nosec B310
            f"{HTTP_URL}{path}", timeout=5, context=SSL_CONTEXT
        ) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def post_count(metrics: str) -> int:
    pattern = re.compile(
        r'^ferricstore_http_requests_total\{method="POST",status="200"\} (\d+)$',
        re.MULTILINE,
    )
    match = pattern.search(metrics)
    return int(match.group(1)) if match else 0


def client(**options: Any) -> FlowClient:
    return FlowClient.from_url(
        HTTP_URL,
        username=USERNAME,
        password=PASSWORD,
        timeout=10,
        ssl_context=SSL_CONTEXT,
        **options,
    )


def run_sync_json() -> None:
    key = f"{KEY_PREFIX}:sync:{uuid.uuid4().hex}"
    value = b"\x00\xffpython-http\x80"
    sdk = client(max_connections=1)
    try:
        assert sdk.ping() == b"PONG"
        assert sdk.kv_set(key, value) == b"OK"
        assert sdk.kv_get(key) == value

        pipeline_key = f"{key}:pipeline"
        result = (
            sdk.pipeline()
            .command("SET", pipeline_key, value)
            .command("GET", pipeline_key)
            .execute()
        )
        assert result == [b"OK", value]

        try:
            sdk.command("SET")
        except FerricStoreError as error:
            assert "argument" in str(error).lower()
        else:
            raise AssertionError("malformed SET did not produce an SDK domain error")

        try:
            sdk.command("MULTI")
        except FerricStoreError:
            pass
        else:
            raise AssertionError("connection-affine MULTI was accepted over HTTP")

        try:
            sdk.kv_set(f"outside:{uuid.uuid4().hex}", value)
        except FerricStoreError as error:
            assert "noperm" in str(error).lower() or "permission" in str(error).lower()
        else:
            raise AssertionError("FerricStore ACL allowed an out-of-scope key")
    finally:
        sdk.close()


def run_compact() -> None:
    key = f"{KEY_PREFIX}:compact:{uuid.uuid4().hex}"
    value = bytes(range(256))
    sdk = client(compact=True, max_connections=1)
    try:
        assert sdk.kv_set(key, value) == b"OK"
        assert sdk.kv_get(key) == value
    finally:
        sdk.close()


def run_compact_workflow() -> None:
    flow_id = f"{KEY_PREFIX}:compact-flow:{uuid.uuid4().hex}"
    sdk = client(compact=True, codec=JsonCodec(), max_connections=1)
    try:
        job = sdk.start_and_claim(
            flow_id,
            type="python-http-compact",
            initial_state="queued",
            worker="python-http-compact-worker",
            partition_key=flow_id,
            payload={"compact": True},
        )
        assert job.id == flow_id
        assert job.lease_token
        assert job.fencing_token > 0
        assert sdk.complete(
            job.id,
            lease_token=job.lease_token,
            fencing_token=job.fencing_token,
            partition_key=job.partition_key,
            result={"compact": True},
        )
    finally:
        sdk.close()


def run_coalescing() -> None:
    calls = 12
    barrier = threading.Barrier(calls)
    before = post_count(fetch("/metrics")[1])
    sdk = client(
        max_connections=2,
        coalesce_window_ms=30,
        coalesce_max_items=calls,
    )

    def execute(_index: int) -> bytes:
        barrier.wait(timeout=5)
        return sdk.ping()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=calls) as executor:
            assert list(executor.map(execute, range(calls))) == [b"PONG"] * calls
    finally:
        sdk.close()

    after = post_count(fetch("/metrics")[1])
    assert after - before == 1, f"coalescing used {after - before} HTTP requests"


def sdk_workflow_commands() -> set[str]:
    """Discover exact workflow command literals exposed by this SDK checkout."""
    package = pathlib.Path(ferricstore.__file__).resolve().parent
    commands: set[str] = set()
    pattern = re.compile(r'''["'](FLOW\.[A-Z0-9_.]+)["']''')
    for source in package.rglob("*.py"):
        commands.update(pattern.findall(source.read_text(encoding="utf-8")))
    return commands


def run_workflow_command_surface() -> int:
    """Prove every SDK workflow command is recognized by the HTTP path.

    Commands are intentionally sent without arguments. Most should return their
    normal command-specific arity error; structured-only commands are rejected
    by their SDK payload builder before I/O; zero-argument reads may succeed. An
    unknown-command or HTTP-boundary rejection proves a coverage bug. Valid
    state-machine semantics are covered by the real SDK workflow scenarios.
    """
    commands = sdk_workflow_commands()
    assert len(commands) >= 67, f"unexpectedly small SDK workflow surface: {len(commands)}"

    sdk = client(max_connections=1)
    failures: list[str] = []
    try:
        for command in sorted(commands):
            try:
                sdk.command(command)
            except FerricStoreError as error:
                message = str(error).lower()
                if "unknown command" in message or "unsupported over stateless http" in message:
                    failures.append(f"{command}: {error}")
    finally:
        sdk.close()

    assert not failures, "workflow commands rejected by HTTP:\n" + "\n".join(failures)
    return len(commands)


async def run_async() -> None:
    key = f"{KEY_PREFIX}:async:{uuid.uuid4().hex}"
    value = b"async-value"
    sdk = AsyncFlowClient.from_url(
        HTTP_URL,
        username=USERNAME,
        password=PASSWORD,
        timeout=10,
        max_connections=2,
        ssl_context=SSL_CONTEXT,
    )
    try:
        assert await sdk.kv_set(key, value) == b"OK"
        assert await sdk.kv_get(key) == value
        pipeline = await sdk.pipeline().command("PING").command("PING").execute()
        assert pipeline == [b"PONG", b"PONG"]
    finally:
        await sdk.close()


def run_auth_failure() -> None:
    sdk = FlowClient.from_url(
        HTTP_URL,
        username=USERNAME,
        password=f"{PASSWORD}-invalid",
        timeout=5,
        ssl_context=SSL_CONTEXT,
    )
    try:
        try:
            sdk.ping()
        except HttpError as error:
            assert error.status_code == 401
            assert error.error_code == "unauthenticated"
            assert error.safe_to_retry is False
        else:
            raise AssertionError("invalid Basic credentials were accepted")
    finally:
        sdk.close()


def main() -> None:
    validate_loopback_url()
    assert fetch("/health") == (200, '{"status":"ok"}')
    assert fetch("/ready") == (200, '{"status":"ready"}')
    run_sync_json()
    run_compact()
    run_compact_workflow()
    run_coalescing()
    workflow_commands = run_workflow_command_surface()
    asyncio.run(run_async())
    run_auth_failure()
    status, metrics = fetch("/metrics")
    assert status == 200
    assert "ferricstore_http_requests_total" in metrics
    assert "ferricstore_http_in_flight_requests" in metrics
    print(
        json.dumps(
            {
                "status": "ok",
                "suite": "python_sdk_http_integration",
                "workflow_commands": workflow_commands,
                "workflow_command_names": sorted(sdk_workflow_commands()),
            }
        )
    )


if __name__ == "__main__":
    main()
