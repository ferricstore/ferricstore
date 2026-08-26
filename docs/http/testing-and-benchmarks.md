# Testing and benchmarks

## Test layers

The suite deliberately separates failures by ownership:

| Layer | What it proves |
| --- | --- |
| Unit | Binary markers, auth-provider contracts, configuration, error mapping, deadlines, admission, and bounded metrics |
| HTTP integration | JSON/MessagePack negotiation, malformed input, declared and streamed body limits, stalled uploads, methods, headers, keep-alive, TLS, and HTTP/2 ALPN |
| In-repository gateway integration | Shared authentication, ACL key scopes, ordered command results, stateless rejection before execution, batch limits, and password rotation |
| Python SDK integration | Unchanged sync/async command APIs, HTTPS Basic auth, pooled connections, pipelines, binary JSON, compact MessagePack, coalescing, SDK errors, and ACL rejection |
| Benchmarks | In-memory HTTP ceiling plus Lambda-shaped real-SDK warm/cold traffic |

The default HTTP suite includes the real in-process gateway and excludes only external SDK suites:

```bash
mix test apps/ferricstore_http/test
FERRICSTORE_PYTHON_SDK_PATH=../ferricstore-python \
  mix test apps/ferricstore_http/test/ferricstore_http/python_sdk_integration_test.exs \
  --include python_sdk_integration
```

The Python executable must have the SDK development extras installed. Set
`FERRICSTORE_PYTHON_EXECUTABLE` when that executable is not `python3`.

## In-memory HTTP/1.1 benchmark

This benchmark isolates listener, parsing, authentication-boundary, encoding, and keep-alive costs
from FerricStore storage work:

```bash
mix run --no-start bench/http/keepalive_benchmark.exs \
  --clients 50 \
  --requests-per-client 100 \
  --commands-per-request 1 \
  --warmup 5
```

Each worker represents one warm function environment with one reused HTTP/1.1 connection.
Large simultaneous connection bursts also depend on the host listen-backlog and file-descriptor
limits. `max_connections` and `acceptors` size the listener but cannot override kernel caps; tune the
host and distribute bursts across proxy instances when validating thousands of environments.

## Python SDK Lambda benchmark

This benchmark starts a TLS listener backed by the real in-process FerricStore gateways, creates one
ACL identity per function group, and runs concurrent function environments. Warm scenarios create
one SDK client per environment and reuse it. Cold scenarios create a client and TLS connection for
every request.

```bash
export FERRICSTORE_PYTHON_SDK_PATH=../ferricstore-python
export FERRICSTORE_PYTHON_EXECUTABLE=/path/to/python-with-sdk-dev-extras
export FERRICSTORE_LAMBDA_GROUPS=8
export FERRICSTORE_LAMBDA_ENVS_PER_GROUP=8
export FERRICSTORE_LAMBDA_WARM_REQUESTS=250
export FERRICSTORE_LAMBDA_COLD_REQUESTS=4
export FERRICSTORE_LAMBDA_BENCH_WORKLOAD=flow_get
export FERRICSTORE_LAMBDA_BENCH_TRANSPORTS=http,http2

mix run --no-start bench/http/python_sdk_lambda_benchmark.exs
```

Supported controls are:

| Variable | Default | Meaning |
| --- | --- | --- |
| `FERRICSTORE_LAMBDA_GROUPS` | `8` | Distinct ACL credential groups |
| `FERRICSTORE_LAMBDA_ENVS_PER_GROUP` | `8` | Concurrent function environments per group |
| `FERRICSTORE_LAMBDA_WARM_REQUESTS` | `250` | Timed requests per warm environment; keep the total large enough to avoid short-burst bias |
| `FERRICSTORE_LAMBDA_COLD_REQUESTS` | `4` | Timed requests per cold environment |
| `FERRICSTORE_LAMBDA_BENCH_MODE` | `all` | `warm`, `cold`, or `all` |
| `FERRICSTORE_LAMBDA_BENCH_WORKLOAD` | `ping` | KV: `ping`, seeded `read`, `write`, 50/50 `mixed`, or `batch`; Flow: seeded `flow_get`, unique `flow_create`, structured `flow_start`, or two-command `flow_batch` (`FLOW.CREATE` + `FLOW.GET`) |
| `FERRICSTORE_LAMBDA_EXECUTION_MODEL` | `processes` | `processes` isolates each simulated environment; `threads` is a conservative single-process/GIL diagnostic |
| `FERRICSTORE_LAMBDA_BENCH_TRANSPORTS` | `http` | Comma-separated `http`, `http2`, and/or `native`; `http_transport`, `httpclient`, and `httpx` are transport diagnostics |
| `FERRICSTORE_LAMBDA_COMPACT` | `false` | Use MessagePack for HTTP requests |
| `FERRICSTORE_LAMBDA_REQUEST_TIMEOUT` | `10` | Per-operation SDK timeout in seconds |
| `FERRICSTORE_LAMBDA_MIN_HTTP_NATIVE_RATIO` | `0` | Optional `0..1` warm HTTP/1-to-native throughput floor; requires both `http,native` and fails the run below the floor |
| `FERRICSTORE_HTTP_BENCH_BACKEND` | `ferricstore` | `ferricstore` measures the real gateway; `memory` isolates the HTTP/SDK ceiling |
| `FERRICSTORE_HTTP_BENCH_PROFILE` | `off` | `call_time` or `call_memory` enables OTP profiling; profiling results are not throughput results |

Every result is emitted as one JSON object containing the workload, concurrency, request, command,
and error counts, duration, requests and commands per second, and p50/p95/p99/max request latency.
The `batch` and `flow_batch` workloads count one HTTP request and two FerricStore commands per
operation. The Flow workloads use the real FerricStore gateway; they measure storage and workflow
execution as well as the HTTP transport. `flow_start` specifically covers the SDK's structured
native-opcode bridge. Diagnostic
transport implementations (`http_transport`, `httpclient`, and `httpx`) intentionally support only
PING; use `http`, `http2`, or `native` for data workloads. Any request error makes the benchmark exit
unsuccessfully. Compare runs on the same host and toolchain; these numbers are regression and
capacity-planning evidence, not a universal production limit. The isolated benchmark raises the
shared authentication limiter only to the configured concurrency plus headroom, preventing a burst
of successful simultaneous authentications from obscuring transport measurements. The normal HTTP
integration suite still exercises the production rate-limit response.

### Local optimization evidence

On the same 16-scheduler development host, with 64 process-isolated warm environments, TLS, Basic
authentication, the real FerricStore gateway, and 250 timed PING requests per environment:

| Measurement | Requests/second | p95 latency | Errors |
| --- | ---: | ---: | ---: |
| In-memory backend control | 15,697.73 | 7.27 ms | 0 |
| Real gateway before mailbox-free scoped leases | 6,809.21 | 30.22 ms | 0 |
| Real gateway after mailbox-free scoped leases, run 1 | 10,756.99 | 16.75 ms | 0 |
| Real gateway after mailbox-free scoped leases, run 2 | 10,877.12 | 16.62 ms | 0 |
| Current release candidate on Python 3.14, run 1 | 18,076.48 | 5.87 ms | 0 |
| Current release candidate on Python 3.14, run 2 | 17,008.15 | 6.53 ms | 0 |

The two matching post-change runs are about 59% faster than the sustained pre-change run. Later
samples on this workstation were rejected because unrelated long-running benchmark processes and a
virtual machine consumed multiple cores. Always inspect host contention, repeat the same scenario,
and retain controls; do not select the best sample from a noisy machine. The release-candidate rows
validate the complete TLS and Python SDK path on the newer toolchain; they are not a causal comparison
with the older scoped-lease measurements.

### Homogeneous command-gateway batch evidence

The Java SDK's real HTTP KV benchmark also covers the shared gateway and durable storage rather than
an in-memory backend. The following controlled A/B used the unchanged `0.11.11` worktree and the
candidate worktree on the same host, plain HTTP/1.1, 64 in-flight batches, 100 hot keys, and 16-byte
values. GET batches contained 1,000 commands and SET batches contained 500 commands. Each value is
the median of three zero-error five-second samples after the binary correctness probe. SET candidate
samples used a fresh server process so sustained writes from an earlier sample could not trigger
storage-batcher admission in a later sample. The benchmark-only authentication attempt allowance was
1,000 on both A/B servers, isolating the gateway and storage path from connection-startup auth bursts.

| Gateway | Runtime | Workload | Commands/second | p95 batch latency | Errors |
| --- | --- | --- | ---: | ---: | ---: |
| Unchanged `0.11.11` | Java 21 | GET | 76,495 | 1,965.9 ms | 0 |
| Homogeneous KV batch | Java 21 | GET | 762,626 | 130.6 ms | 0 |
| Unchanged `0.11.11` | Java 21 | SET | 7,657 | 4,194.8 ms | 0 |
| Homogeneous KV batch | Java 21 | SET | 647,858 | 55.1 ms | 0 |
| Homogeneous KV batch | Java 17 | GET | 768,617 | 138.0 ms | 0 |
| Homogeneous KV batch | Java 17 | SET | 677,255 | 51.4 ms | 0 |

The same candidate server then completed the 10,000-flow, three-step Java workflow benchmark with
zero execution or verification errors: 2,576 workflow completions/s on Java 21 and 2,608/s on Java
17. The workflow commands deliberately remain on their existing structured paths, so these runs are
a regression guard rather than evidence that the KV fast path accelerates Flow. These are local
regression measurements, not universal capacity claims; compare candidates on the same host and
retain the zero-error requirement.

With the production default of 10 authentication attempts per window, a fresh Java HTTP/1.1 client
opening 64 connections concurrently also intermittently received `429 rate_limited` for valid Basic
credentials. The retained throughput samples exclude that separate startup-admission effect; it
needs its own auth single-flight or queued-verification correction rather than a higher production
rate limit.
