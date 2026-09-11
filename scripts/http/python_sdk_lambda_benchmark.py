#!/usr/bin/env python3
# ruff: noqa: BLE001 - benchmark workers must capture and report arbitrary SDK failures
"""Benchmark Lambda-shaped Python SDK traffic against FerricStore HTTP."""

from __future__ import annotations

import base64
import concurrent.futures
import http.client
import json
import math
import multiprocessing
import os
import queue
import ssl
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass
from functools import partial
from typing import Any
from urllib.parse import urlsplit

from ferricstore import FlowClient, JsonCodec
from ferricstore.http_transport import JsonHttpTransport

HTTP_URL = os.environ["FERRICSTORE_HTTP_BENCH_URL"]
NATIVE_URL = os.environ.get("FERRICSTORE_HTTP_BENCH_NATIVE_URL", "ferric://127.0.0.1:6388")
GROUPS = int(os.environ.get("FERRICSTORE_LAMBDA_GROUPS", "8"))
ENVIRONMENTS_PER_GROUP = int(os.environ.get("FERRICSTORE_LAMBDA_ENVS_PER_GROUP", "8"))
WARM_REQUESTS = int(os.environ.get("FERRICSTORE_LAMBDA_WARM_REQUESTS", "250"))
COLD_REQUESTS = int(os.environ.get("FERRICSTORE_LAMBDA_COLD_REQUESTS", "4"))
REQUEST_TIMEOUT = float(os.environ.get("FERRICSTORE_LAMBDA_REQUEST_TIMEOUT", "10"))
MODE = os.environ.get("FERRICSTORE_LAMBDA_BENCH_MODE", "all")
EXECUTION_MODEL = os.environ.get("FERRICSTORE_LAMBDA_EXECUTION_MODEL", "processes")
COMPACT = os.environ.get("FERRICSTORE_LAMBDA_COMPACT", "false").lower() == "true"
WORKLOAD = os.environ.get("FERRICSTORE_LAMBDA_BENCH_WORKLOAD", "ping")
MIN_HTTP_NATIVE_RATIO = float(
    os.environ.get("FERRICSTORE_LAMBDA_MIN_HTTP_NATIVE_RATIO", "0")
)
TRANSPORTS = {
    item.strip()
    for item in os.environ.get("FERRICSTORE_LAMBDA_BENCH_TRANSPORTS", "http").split(",")
    if item.strip()
}
CA_FILE = os.environ.get("FERRICSTORE_HTTP_BENCH_CA_FILE")
SSL_CONTEXT = (
    ssl.create_default_context(cafile=CA_FILE) if HTTP_URL.startswith("https://") else None
)


@dataclass(frozen=True)
class Result:
    scenario: str
    workload: str
    groups: int
    environments_per_group: int
    concurrency: int
    requests: int
    commands: int
    errors: int
    duration_seconds: float
    requests_per_second: float
    commands_per_second: float
    latency_ms_p50: float
    latency_ms_p95: float
    latency_ms_p99: float
    latency_ms_max: float


def positive(value: int, name: str) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def credentials(group: int) -> tuple[str, str]:
    return f"lambda-group-{group}", f"lambda-group-{group}-password"


def wait_for_server() -> None:
    deadline = time.monotonic() + 30
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            # The base URL is restricted to HTTP(S) loopback in main().
            with urllib.request.urlopen(  # nosec B310
                f"{HTTP_URL}/ready",
                timeout=2,
                context=SSL_CONTEXT,
            ) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError) as error:
            last_error = error
        time.sleep(0.1)
    raise RuntimeError(f"HTTP server did not become ready: {last_error}")


def http_client(group: int, *, http2: bool = False) -> FlowClient:
    username, password = credentials(group)
    return FlowClient.from_url(
        HTTP_URL,
        codec=JsonCodec(),
        username=username,
        password=password,
        ssl_context=SSL_CONTEXT,
        timeout=REQUEST_TIMEOUT,
        max_connections=1,
        max_concurrent_requests=1,
        http2=http2,
        compact=COMPACT,
    )


def native_client(group: int) -> FlowClient:
    username, password = credentials(group)
    return FlowClient.from_url(
        NATIVE_URL,
        codec=JsonCodec(),
        username=username,
        password=password,
        timeout=REQUEST_TIMEOUT,
        max_connections=1,
    )


class TransportPingClient:
    def __init__(self, group: int) -> None:
        username, password = credentials(group)
        self._transport = JsonHttpTransport(
            HTTP_URL,
            username=username,
            password=password,
            ssl_context=SSL_CONTEXT,
            timeout=REQUEST_TIMEOUT,
            max_connections=1,
        )

    def ping(self) -> str:
        _status, response = self._transport.request_json(
            "POST",
            "/v1/commands",
            body={"commands": [["PING"]]},
        )
        return response["results"][0]["value"]

    def close(self) -> None:
        self._transport.close()


class HttpxPingClient:
    def __init__(self, group: int) -> None:
        import httpx

        username, password = credentials(group)
        authorization = base64.b64encode(f"{username}:{password}".encode()).decode()
        self._client = httpx.Client(
            base_url=HTTP_URL,
            headers={"authorization": f"Basic {authorization}"},
            verify=SSL_CONTEXT,
            timeout=REQUEST_TIMEOUT,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )

    def ping(self) -> str:
        response = self._client.post(
            "/v1/commands",
            content=b'{"commands":[["PING"]]}',
            headers={"content-type": "application/json"},
        )
        response.raise_for_status()
        return response.json()["results"][0]["value"]

    def close(self) -> None:
        self._client.close()


class HttpClientPingClient:
    def __init__(self, group: int) -> None:
        parsed = urlsplit(HTTP_URL)
        username, password = credentials(group)
        authorization = base64.b64encode(f"{username}:{password}".encode()).decode()
        connection_type = (
            http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        )
        kwargs = {"context": SSL_CONTEXT} if parsed.scheme == "https" else {}
        self._connection = connection_type(
            parsed.hostname,
            parsed.port,
            timeout=REQUEST_TIMEOUT,
            **kwargs,
        )
        self._headers = {
            "accept": "application/json",
            "authorization": f"Basic {authorization}",
            "content-type": "application/json",
        }

    def ping(self) -> str:
        self._connection.request(
            "POST",
            "/v1/commands",
            body=b'{"commands":[["PING"]]}',
            headers=self._headers,
        )
        response = self._connection.getresponse()
        raw = response.read()
        if response.status != 200:
            raise RuntimeError(f"HTTP status {response.status}: {raw!r}")
        return json.loads(raw)["results"][0]["value"]

    def close(self) -> None:
        self._connection.close()


def transport_client(group: int) -> TransportPingClient:
    return TransportPingClient(group)


def httpx_client(group: int) -> HttpxPingClient:
    return HttpxPingClient(group)


def httpclient_client(group: int) -> HttpClientPingClient:
    return HttpClientPingClient(group)


def execute_ping(client: Any) -> None:
    response = client.ping()
    if response not in {b"PONG", "PONG"}:
        raise RuntimeError(f"unexpected PING response: {response!r}")


def workload_key(environment: int) -> str:
    group = environment % GROUPS
    return f"lambda:{group}:environment:{environment}"


def workload_value(environment: int) -> bytes:
    return f"value-{environment}".encode()


def workflow_id(environment: int, request: int | None = None) -> str:
    group = environment % GROUPS
    suffix = "seed" if request is None else f"{os.getpid()}:{request}:{time.time_ns()}"
    return f"lambda:{group}:environment:{environment}:flow:{suffix}"


def create_workflow(client: FlowClient, flow_id: str) -> None:
    client.create(
        flow_id,
        type="lambda-benchmark",
        state="queued",
        partition_key=flow_id,
        payload={"benchmark": True},
        idempotent=True,
    )


def prepare_workload(client: FlowClient, environment: int) -> None:
    """Warm the connection/auth path and seed deterministic read data."""
    execute_ping(client)
    if WORKLOAD == "read":
        response = client.command(
            "SET",
            workload_key(environment),
            workload_value(environment),
        )
        if response not in {b"OK", "OK"}:
            raise RuntimeError(f"unexpected SET preparation response: {response!r}")
    elif WORKLOAD == "flow_get":
        create_workflow(client, workflow_id(environment))


def execute_workload(client: FlowClient, environment: int, request: int) -> None:
    key = workload_key(environment)
    value = workload_value(environment)

    if WORKLOAD == "ping":
        execute_ping(client)
    elif WORKLOAD == "read":
        response = client.command("GET", key)
        if response != value:
            raise RuntimeError(f"unexpected GET response: {response!r}")
    elif WORKLOAD == "write":
        response = client.command("SET", key, value)
        if response not in {b"OK", "OK"}:
            raise RuntimeError(f"unexpected SET response: {response!r}")
    elif WORKLOAD == "mixed":
        if request % 2 == 0:
            response = client.command("SET", key, value)
            if response not in {b"OK", "OK"}:
                raise RuntimeError(f"unexpected mixed SET response: {response!r}")
        else:
            response = client.command("GET", key)
            if response != value:
                raise RuntimeError(f"unexpected mixed GET response: {response!r}")
    elif WORKLOAD == "batch":
        response = client.pipeline().command("SET", key, value).command("GET", key).execute()
        if response != [b"OK", value]:
            raise RuntimeError(f"unexpected batch response: {response!r}")
    elif WORKLOAD == "flow_get":
        flow_id = workflow_id(environment)
        record = client.get(flow_id, partition_key=flow_id)
        if record is None or record.id != flow_id:
            raise RuntimeError(f"unexpected FLOW.GET response: {record!r}")
    elif WORKLOAD == "flow_create":
        create_workflow(client, workflow_id(environment, request))
    elif WORKLOAD == "flow_start":
        flow_id = workflow_id(environment, request)
        job = client.start_and_claim(
            flow_id,
            type="lambda-benchmark",
            initial_state="queued",
            worker=f"lambda-worker-{environment}",
            partition_key=flow_id,
        )
        if job.id != flow_id or not job.lease_token or job.fencing_token <= 0:
            raise RuntimeError(f"unexpected FLOW.START_AND_CLAIM response: {job!r}")
    else:
        flow_id = workflow_id(environment, request)
        response = (
            client.pipeline()
            .command(
                "FLOW.CREATE",
                flow_id,
                "TYPE",
                "lambda-benchmark",
                "STATE",
                "queued",
                "PARTITION",
                flow_id,
                "IDEMPOTENT",
                "true",
            )
            .command("FLOW.GET", flow_id, "PARTITION", flow_id)
            .execute()
        )
        if len(response) != 2 or response[1] is None:
            raise RuntimeError(f"unexpected Flow batch response: {response!r}")


def client_for_transport(transport: str, group: int) -> FlowClient:
    factories = {
        "http": http_client,
        "http2": partial(http_client, http2=True),
        "http_transport": transport_client,
        "httpclient": httpclient_client,
        "httpx": httpx_client,
        "native": native_client,
    }
    return factories[transport](group)


def timed(operation: Callable[[], None]) -> tuple[float | None, str | None]:
    started_at = time.perf_counter()
    try:
        operation()
    except Exception as error:
        return None, f"{type(error).__name__}: {error}"
    return (time.perf_counter() - started_at) * 1_000, None


def warm_worker(
    environment: int,
    barrier: threading.Barrier,
    factory: Callable[[int], FlowClient],
) -> tuple[list[float], list[str]]:
    group = environment % GROUPS
    client: FlowClient | None = None
    latencies: list[float] = []
    errors: list[str] = []
    try:
        try:
            client = factory(group)
            prepare_workload(client, environment)
        except Exception as error:
            errors.append(f"warmup {type(error).__name__}: {error}")
        finally:
            barrier.wait(timeout=max(30, REQUEST_TIMEOUT * 2))

        if client is None or errors:
            return latencies, errors

        for request in range(WARM_REQUESTS):
            latency, error = timed(
                lambda request=request: execute_workload(client, environment, request)
            )
            if latency is not None:
                latencies.append(latency)
            if error is not None:
                errors.append(error)
    finally:
        if client is not None:
            client.close()
    return latencies, errors


def cold_worker(
    environment: int,
    barrier: threading.Barrier,
    factory: Callable[[int], FlowClient],
) -> tuple[list[float], list[str]]:
    group = environment % GROUPS
    latencies: list[float] = []
    errors: list[str] = []

    if WORKLOAD in {"read", "flow_get"}:
        bootstrap = factory(group)
        try:
            prepare_workload(bootstrap, environment)
        finally:
            bootstrap.close()

    barrier.wait(timeout=max(30, REQUEST_TIMEOUT * 2))

    for request in range(COLD_REQUESTS):

        def cold_request(request: int = request) -> None:
            sdk = factory(group)
            try:
                execute_workload(sdk, environment, request)
            finally:
                sdk.close()

        latency, error = timed(cold_request)
        if latency is not None:
            latencies.append(latency)
        if error is not None:
            errors.append(error)
    return latencies, errors


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def run_scenario(
    name: str,
    worker: Callable[[int, threading.Barrier], tuple[list[float], list[str]]],
    requests_per_environment: int,
) -> Result:
    concurrency = GROUPS * ENVIRONMENTS_PER_GROUP
    start_time: list[float] = []
    barrier = threading.Barrier(
        concurrency + 1,
        action=lambda: start_time.append(time.perf_counter()),
    )
    latencies: list[float] = []
    errors: list[str] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(worker, environment, barrier) for environment in range(concurrency)
        ]
        barrier.wait(timeout=max(30, REQUEST_TIMEOUT * 2))
        for future in concurrent.futures.as_completed(futures):
            worker_latencies, worker_errors = future.result()
            latencies.extend(worker_latencies)
            errors.extend(worker_errors)

    duration = time.perf_counter() - start_time[0]
    result = build_result(name, requests_per_environment, latencies, errors, duration)
    print_result(result, errors)
    return result


def process_worker(
    environment: int,
    transport: str,
    scenario: str,
    start_event: multiprocessing.synchronize.Event,
    ready_queue: multiprocessing.queues.Queue,
    result_queue: multiprocessing.queues.Queue,
) -> None:
    group = environment % GROUPS
    client: FlowClient | None = None
    latencies: list[float] = []
    errors: list[str] = []
    try:
        if scenario == "warm":
            try:
                client = client_for_transport(transport, group)
                prepare_workload(client, environment)
            except Exception as error:
                errors.append(f"warmup {type(error).__name__}: {error}")
        elif WORKLOAD in {"read", "flow_get"}:
            try:
                bootstrap = client_for_transport(transport, group)
                try:
                    prepare_workload(bootstrap, environment)
                finally:
                    bootstrap.close()
            except Exception as error:
                errors.append(f"setup {type(error).__name__}: {error}")

        ready_queue.put(environment)
        if not start_event.wait(timeout=max(60, REQUEST_TIMEOUT * 4)):
            errors.append("benchmark start timed out")
            return

        requests = WARM_REQUESTS if scenario == "warm" else COLD_REQUESTS
        for request in range(requests):
            if scenario == "warm":
                if client is None or errors:
                    break
                latency, error = timed(
                    lambda request=request: execute_workload(
                        client,
                        environment,
                        request,
                    )
                )
            else:

                def cold_request(request: int = request) -> None:
                    sdk = client_for_transport(transport, group)
                    try:
                        execute_workload(sdk, environment, request)
                    finally:
                        sdk.close()

                latency, error = timed(cold_request)

            if latency is not None:
                latencies.append(latency)
            if error is not None:
                errors.append(error)
    except BaseException as error:
        errors.append(f"worker {type(error).__name__}: {error}")
    finally:
        if client is not None:
            client.close()
        result_queue.put((environment, latencies, errors))


def run_process_scenario(
    name: str,
    transport: str,
    scenario: str,
    requests_per_environment: int,
) -> Result:
    concurrency = GROUPS * ENVIRONMENTS_PER_GROUP
    context = multiprocessing.get_context("spawn")
    start_event = context.Event()
    ready_queue = context.Queue()
    result_queue = context.Queue()
    processes = [
        context.Process(
            target=process_worker,
            args=(
                environment,
                transport,
                scenario,
                start_event,
                ready_queue,
                result_queue,
            ),
        )
        for environment in range(concurrency)
    ]
    latencies: list[float] = []
    errors: list[str] = []

    try:
        for process in processes:
            process.start()
        for _process in processes:
            ready_queue.get(timeout=120)

        started_at = time.perf_counter()
        start_event.set()
        for _process in processes:
            _environment, worker_latencies, worker_errors = result_queue.get(timeout=120)
            latencies.extend(worker_latencies)
            errors.extend(worker_errors)
        duration = time.perf_counter() - started_at

        for process in processes:
            process.join(timeout=5)
            if process.exitcode not in {0, None}:
                errors.append(f"worker process exited with status {process.exitcode}")
    except queue.Empty as error:
        raise RuntimeError("process-isolated benchmark worker timed out") from error
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)
        ready_queue.close()
        result_queue.close()

    result = build_result(name, requests_per_environment, latencies, errors, duration)
    print_result(result, errors)
    return result


def build_result(
    name: str,
    requests_per_environment: int,
    latencies: list[float],
    errors: list[str],
    duration: float,
) -> Result:
    concurrency = GROUPS * ENVIRONMENTS_PER_GROUP
    requests = concurrency * requests_per_environment
    commands_per_request = 2 if WORKLOAD in {"batch", "flow_batch"} else 1
    completed_requests = len(latencies)
    completed_commands = completed_requests * commands_per_request
    return Result(
        scenario=name,
        workload=WORKLOAD,
        groups=GROUPS,
        environments_per_group=ENVIRONMENTS_PER_GROUP,
        concurrency=concurrency,
        requests=requests,
        commands=requests * commands_per_request,
        errors=requests - len(latencies),
        duration_seconds=round(duration, 6),
        requests_per_second=round(completed_requests / duration, 2),
        commands_per_second=round(completed_commands / duration, 2),
        latency_ms_p50=round(percentile(latencies, 0.50), 3),
        latency_ms_p95=round(percentile(latencies, 0.95), 3),
        latency_ms_p99=round(percentile(latencies, 0.99), 3),
        latency_ms_max=round(max(latencies, default=math.nan), 3),
    )


def print_result(result: Result, errors: list[str]) -> None:
    print(json.dumps(asdict(result), sort_keys=True), flush=True)
    if errors:
        print(
            json.dumps({"scenario": result.scenario, "error_samples": errors[:5]}),
            flush=True,
        )


def check_http_native_ratio(results: list[Result]) -> None:
    if MIN_HTTP_NATIVE_RATIO == 0:
        return

    http = next(
        (result for result in results if result.scenario.startswith("http1_") and "_warm_" in result.scenario),
        None,
    )
    native = next(
        (
            result
            for result in results
            if result.scenario.startswith("direct_tcp_") and "_warm_" in result.scenario
        ),
        None,
    )
    if http is None or native is None:
        raise ValueError(
            "FERRICSTORE_LAMBDA_MIN_HTTP_NATIVE_RATIO requires warm http and native transports"
        )
    if native.requests_per_second <= 0:
        raise RuntimeError("native benchmark throughput must be positive for ratio comparison")

    ratio = http.requests_per_second / native.requests_per_second
    print(
        json.dumps(
            {
                "comparison": "warm_http1_to_direct_tcp",
                "workload": WORKLOAD,
                "ratio": round(ratio, 4),
                "minimum_ratio": MIN_HTTP_NATIVE_RATIO,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    if ratio < MIN_HTTP_NATIVE_RATIO:
        raise SystemExit(
            f"warm HTTP/native throughput ratio {ratio:.4f} is below "
            f"{MIN_HTTP_NATIVE_RATIO:.4f}"
        )


def main() -> None:
    parsed_url = urlsplit(HTTP_URL)
    if parsed_url.scheme not in {"http", "https"} or parsed_url.hostname not in {
        "127.0.0.1",
        "localhost",
    }:
        raise ValueError("benchmark HTTP URL must use HTTP(S) on loopback")
    positive(GROUPS, "FERRICSTORE_LAMBDA_GROUPS")
    positive(ENVIRONMENTS_PER_GROUP, "FERRICSTORE_LAMBDA_ENVS_PER_GROUP")
    positive(WARM_REQUESTS, "FERRICSTORE_LAMBDA_WARM_REQUESTS")
    positive(COLD_REQUESTS, "FERRICSTORE_LAMBDA_COLD_REQUESTS")
    if not math.isfinite(MIN_HTTP_NATIVE_RATIO) or not 0 <= MIN_HTTP_NATIVE_RATIO <= 1:
        raise ValueError("FERRICSTORE_LAMBDA_MIN_HTTP_NATIVE_RATIO must be between 0 and 1")
    if MODE not in {"all", "warm", "cold"}:
        raise ValueError("FERRICSTORE_LAMBDA_BENCH_MODE must be all, warm, or cold")
    if EXECUTION_MODEL not in {"processes", "threads"}:
        raise ValueError("FERRICSTORE_LAMBDA_EXECUTION_MODEL must be processes or threads")
    if WORKLOAD not in {
        "ping",
        "read",
        "write",
        "mixed",
        "batch",
        "flow_get",
        "flow_create",
        "flow_start",
        "flow_batch",
    }:
        raise ValueError(
            "FERRICSTORE_LAMBDA_BENCH_WORKLOAD must be ping, read, write, mixed, batch, "
            "flow_get, flow_create, flow_start, or flow_batch"
        )
    if not TRANSPORTS or not TRANSPORTS.issubset(
        {"http", "http2", "http_transport", "httpclient", "httpx", "native"}
    ):
        raise ValueError(
            "benchmark transports must contain http, http2, http_transport, httpclient, httpx, "
            "and/or native"
        )
    if WORKLOAD != "ping" and TRANSPORTS.intersection({"http_transport", "httpclient", "httpx"}):
        raise ValueError(
            "diagnostic transports support only the ping workload; use http, http2, or native"
        )

    wait_for_server()
    factories: dict[str, tuple[str, Callable[[int], FlowClient]]] = {
        "http": ("http1", http_client),
        "http2": ("http2", partial(http_client, http2=True)),
        "http_transport": ("http1_transport_only", transport_client),
        "httpclient": ("httpclient1_raw", httpclient_client),
        "httpx": ("httpx1_raw", httpx_client),
        "native": ("direct_tcp", native_client),
    }
    results: list[Result] = []

    for transport in sorted(TRANSPORTS):
        prefix, factory = factories[transport]
        if MODE in {"all", "warm"}:
            if EXECUTION_MODEL == "processes":
                results.append(
                    run_process_scenario(
                        f"{prefix}_{WORKLOAD}_warm_reused_client_processes",
                        transport,
                        "warm",
                        WARM_REQUESTS,
                    )
                )
            else:
                results.append(
                    run_scenario(
                        f"{prefix}_{WORKLOAD}_warm_reused_client_threads",
                        partial(warm_worker, factory=factory),
                        WARM_REQUESTS,
                    )
                )
        if MODE in {"all", "cold"}:
            if EXECUTION_MODEL == "processes":
                results.append(
                    run_process_scenario(
                        f"{prefix}_{WORKLOAD}_cold_new_client_processes",
                        transport,
                        "cold",
                        COLD_REQUESTS,
                    )
                )
            else:
                results.append(
                    run_scenario(
                        f"{prefix}_{WORKLOAD}_cold_new_client_threads",
                        partial(cold_worker, factory=factory),
                        COLD_REQUESTS,
                    )
                )

    if any(result.errors for result in results):
        raise SystemExit("Lambda benchmark completed with request errors")
    check_http_native_ratio(results)


if __name__ == "__main__":
    main()
