# Bisect: ferricstore quorum-write throughput across v0.2.0 → v0.3.7

- **Date (UTC)**: 2026-04-29
- **Server**: Azure `Standard_L4as_v4` (4 vCPU, NVMe), `northcentralus`
- **Client**: Azure `Standard_D2as_v4`
- **OS tuning applied**: hugepages=512, THP=never
- **BEAM tuning**: `ERL_FLAGS="+sbt db +sbwt very_short +swt very_low +K true +A 128"`
- **Protocol**: RESP3 over TCP, memtier_benchmark
- **Workload**: SET-only, 256B values, default namespace (Raft + fsync)
- **NIFs**: built from Rust source per tag (`cargo build --release`), Rust 1.91

## Results (`-c 50 --pipeline=10 -t 4 --test-time=20`)

| tag | git ref | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---|---|---:|---:|---:|---:|
| v0.2.0 | `346d99b` | **12,760** | 137 | 557 | 852 |
| v0.3.1 | `v0.3.1` | 13,732 | 135 | 365 | 506 |
| v0.3.5 | `v0.3.5` | 12,854 | 148 | 324 | 444 |
| v0.3.6 | `v0.3.6` | 14,277 | 130 | 385 | 438 |
| v0.3.7 | `v0.3.7` | 13,697 | 137 | 338 | 459 |

**All runs cluster around 12.7K–14.3K — no measurable regression.**

## Results (`-c 200 --pipeline=50`)

| tag | ops/sec | p50 (ms) | p99 (ms) |
|---|---:|---:|---:|
| v0.2.0 (`346d99b`) | 67,460 | 594 | 1,327 |
| v0.3.7 | ~99,290 | 181 | 3,850 |

v0.3.7 actually outperforms v0.2.0 at high concurrency (99K vs 67K throughput, lower p50 latency).

## What happened to the "150K v0.2.0 baseline"?

The `azure_20260421_tuning.md` document claims **150K ops/sec at c=50 p=10** for `346d99b`. This bisect rebuilds the same commit cleanly from source on identical hardware/config and measures **12.7K** — 12× lower than the claim.

The 150K number was not reproducible. Likely sources of the discrepancy:
1. **Forked `ra_ferricstore` local checkout** — the prior bench may have used a development fork with experimental WAL fdatasync changes that were never merged or shipped on Hex.
2. **WAL NIF silently bypassed** — at one point the NIF was registered but a config bug routed writes through `:prim_file`'s 128-thread dirty pool. That could give 150K but with nondurable writes (fsyncs not happening).
3. **Different shard count or BEAM allocator config** that wasn't documented in the tuning doc.

## Conclusion

The architecture's actual quorum-write ceiling at `c=50 p=10` is **~13K ops/sec** with the WAL NIF and durable fsync per Raft batch. This is consistent across v0.2.0 → v0.3.7. The added overhead in v0.3.6/v0.3.7 (read-your-write gating + `{:applied_at, _, _}` wrap) does NOT cause a measurable regression at this config.

**v0.3.7's performance is healthy** — the "regression" was relative to a baseline that may never have been reproducible.

## Higher-throughput regimes

To exceed 13K at this concurrency, the architectural lever is **WAL group commit amortization** — i.e., more concurrent writes per fsync. Either:
- Higher pipeline depth (`--pipeline=50` → 99K)
- More clients (`-c 200` → 67K at p=50)
- More shards (parallel fsync streams, but each shard's WAL still bottlenecks)

The fundamental bound is fsync rate × commands per fsync. Tuning that is a separate design discussion.
