# FerricStore v0.3.7 — erpc quorum-write batching strategies

- **Date (UTC)**: 2026-04-29 ~11:54
- **Server**: Azure `Standard_L4as_v4` (4 vCPU, NVMe)
- **Client**: Azure `Standard_D2as_v4` (separate VM, same VNet)
- **OS tuning**: hugepages=512, THP=never
- **BEAM tuning**: `ERL_FLAGS="+sbt db +sbwt very_short +swt very_low +K true +A 128"`
- **Protocol**: erpc (Erlang distribution) — client BEAM → server BEAM
- **Workload**: SET to default namespace (Raft + fsync), 256B values, 15s per cell
- **NIFs**: built from v0.3.7 source (cargo build --release, Rust 1.91)
- **Data dir wiped + server restarted before the bench** (single fresh state, no cross-cell pollution)

## Strategy 1: single SET per erpc call (no batching)

| workers | ops/sec | p50 (µs) | p99 (µs) | p99.9 (µs) |
|---|---:|---:|---:|---:|
| 1 | 80 | 13,129 | 21,570 | 38,663 |
| 10 | 459 | 20,673 | 46,654 | 61,408 |
| 50 | 1,242 | 37,551 | 90,839 | 113,049 |
| 100 | 1,976 | 47,684 | 111,321 | 144,636 |

**Per-request true latency floor: ~13ms p50** (single worker = no queueing). That's one Raft round-trip + one fsync.

## Strategy 2: batch_set, vary batch size (50 workers)

| batch | ops/sec | calls/sec | p50 (µs) | p99 (µs) | p99.9 (µs) |
|---|---:|---:|---:|---:|---:|
| 10 | 8,927 | 893 | 53,080 | 104,121 | 119,718 |
| 50 | 27,593 | 552 | 89,530 | 164,102 | 191,196 |
| 100 | 40,793 | 408 | 113,928 | 524,675 | 698,016 |
| 500 | 103,500 | 207 | 227,693 | 471,489 | 528,662 |
| **1000** | **142,800** | 143 | 343,363 | 664,733 | 698,465 |

Bigger batches → more amortization across each Raft commit's fsync.

## Strategy 3: batch=100, vary workers

| workers | ops/sec | p50 (µs) | p99 (µs) | p99.9 (µs) |
|---|---:|---:|---:|---:|
| 1 | 3,113 | 31,840 | 63,726 | 69,181 |
| 10 | 16,040 | 60,657 | 107,957 | 143,889 |
| 50 | 42,127 | 118,764 | 226,781 | 261,931 |
| 100 | 53,340 | 181,602 | 377,215 | 448,824 |
| 200 | 66,527 | 297,910 | 547,246 | 656,103 |

## Headline

| metric | value |
|---|---:|
| **Peak quorum throughput** | **142,800 ops/sec** (batch=1000, 50 workers) |
| Best mid-batch | 103,500 ops/sec (batch=500, 50 workers) |
| Single-request p50 latency floor | 13 ms (1 worker, 1 op per call) |

## Comparison against RESP3/TCP

The same v0.3.7 code via memtier RESP3 measured:
- c=200 p=50 → 99,290 ops/sec, p50 181ms
- c=50 p=10 → 13,697 ops/sec, p50 137ms

erpc batch_set b=1000 w=50 → 142K ops/sec, p50 343ms.

erpc lets you go higher because:
1. **No RESP serialization overhead** on either end
2. **Single erpc call carries the whole batch as a list** — Erlang term wire format
3. **The state machine sees one `:batch` ra command** instead of N separate pipelined commands

## Latency reality check (Little's Law)

The p50 numbers in batched runs are **per-call latency**, not per-key. Key-level p50:
- batch=1000, ops/sec=142.8K: per-batch p50 = 343ms → per-key implied = 343ms / 1000 = **0.34ms/key** at saturation. The store actually commits each key very fast — the user-visible latency depends on batch size.
- batch=10 single: 53ms / 10 = **5.3ms per key** at the queue saturation point.
- single set, w=1: **13ms** is the true server response time (one Raft commit + fsync, no contention).

## Files

- `erpc_quorum_strategies_20260429_115447.csv` — raw per-strategy results
