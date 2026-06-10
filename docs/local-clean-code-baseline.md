# Local Clean-Code Baseline

Local baseline results for the `clean-code` branch on this machine.

These numbers are for regression tracking only. They are not public benchmark
claims because server and client run on the same Mac over loopback.

## Environment

```text
date: 2026-06-05
branch: clean-code
commit: 196987bd
server source: /Users/yoavgea/repos/ferricstore
server runtime: MIX_ENV=prod mix run --no-halt
container: no
os: macOS 26.3 25D125
kernel: Darwin 25.3.0 arm64
cpu: Apple M4 Max
logical cpu: 16
memory: 128 GiB
memtier: memtier_benchmark 2.3.0
```

Server configuration:

```bash
MIX_ENV=prod \
FERRICSTORE_PROTECTED_MODE=false \
FERRICSTORE_SHARD_COUNT=16 \
FERRICSTORE_DATA_DIR=/tmp/ferricstore-clean-code-baseline/data-kv-c200p50 \
FERRICSTORE_PORT=6379 \
mix run --no-halt
```

## memtier Baseline: c=200, pipeline=50

This baseline uses the same high-concurrency shape used by the historical Azure
memtier runs:

```text
--clients=200 --threads=4 --pipeline=50
```

In `memtier_benchmark`, `--clients=200 --threads=4` means 200 connections per
thread, so this is 800 total client connections. With pipeline depth 50, the
run has up to 40,000 in-flight requests.

### SET

Command:

```bash
memtier_benchmark -s 127.0.0.1 -p 6379 \
  --protocol=resp3 \
  --clients=200 --threads=4 \
  --pipeline=50 \
  --test-time=30 \
  --command="SET bench:__key__ __data__" \
  --command-key-pattern=R \
  --key-minimum=1 --key-maximum=1000000 \
  --data-size=256 \
  --hide-histogram
```

Result:

| Operation | Ops/sec | Avg latency | p50 latency | p99 latency | p99.9 latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| SET | 756,799/s | 52.835 ms | 52.479 ms | 70.143 ms | 78.335 ms |

### GET

GET was run after the SET benchmark against the populated `bench:*` keyspace.

Command:

```bash
memtier_benchmark -s 127.0.0.1 -p 6379 \
  --protocol=resp3 \
  --clients=200 --threads=4 \
  --pipeline=50 \
  --test-time=30 \
  --command="GET bench:__key__" \
  --command-key-pattern=R \
  --key-minimum=1 --key-maximum=1000000 \
  --hide-histogram
```

Result:

| Operation | Ops/sec | Avg latency | p50 latency | p99 latency | p99.9 latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 5,102,710/s | 7.833 ms | 7.743 ms | 11.455 ms | 13.695 ms |

## Summary

```text
SET: 756,799/s, p50 52.479 ms, p99 70.143 ms
GET: 5,102,710/s, p50 7.743 ms, p99 11.455 ms
```

Raw outputs were written locally during the run:

```text
/tmp/ferricstore-clean-code-baseline/memtier_set_c200_p50.txt
/tmp/ferricstore-clean-code-baseline/memtier_get_c200_p50.txt
```

## DBOS-Style FerricFlow Baseline

The DBOS-style benchmark was run from the Python SDK repository against the same
source server style, with FerricStore running from this branch and not Docker.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate

python examples/dbos_style_benchmark.py \
  --mode queued \
  --transport many \
  --flows 1000000 \
  --server-shards 16
```

Result:

| Metric | Value |
| --- | ---: |
| Flows | 1,000,000 |
| Created | 1,000,000 |
| Completed | 1,000,000 |
| Duplicate completions | 0 |
| Workers | 16 |
| Producers | 32 |
| Partitions | 16 |
| Claim batch size | 500 |
| Create batch size | 500 |
| Transport | `many` |
| Create time | 11.789 s |
| Process time | 13.631 s |
| Total time | 13.632 s |
| Create rate | 84,826 flows/s |
| Process rate | 73,363 flows/s |
| End-to-end rate | 73,355 flows/s |
| Claim calls | 3,370 |
| Empty claims | 1,321 |
| Average claim batch | 296.74 |
| Max claim batch | 500 |

Summary:

```text
DBOS-style queued live: 73,355 flows/s e2e
created: 1,000,000
completed: 1,000,000
duplicate completions: 0
```

Raw output:

```text
/tmp/ferricstore-clean-code-baseline/dbos_1m.txt
```

## Repeatable runner

The same benchmark shape can be run from the repo with:

```bash
python3 bench/local_regression_baseline.py --start-server --suite all
```

Dry-run the exact commands without starting the server or running long benchmarks:

```bash
python3 bench/local_regression_baseline.py --dry-run
```

This runner keeps the RESP/memtier and DBOS-style Python SDK baselines stable. It does not benchmark the native TCP protocol until the SDK has a native transport adapter.
