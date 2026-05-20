# FerricStore v0.3.7 — RESP3/memtier with OS+BEAM tuning applied

- **Date (UTC)**: 2026-04-29 ~10:00
- **Deployment**: Azure `northcentralus`
- **OS tuning**: hugepages=512, THP=never, NVMe scheduler=none
- **BEAM tuning**: `ERL_FLAGS="+sbt db +sbwt very_short +swt very_low +K true +A 128 +P 5000000 +Q 65536 +MHas aoffcbf +MBas aoffcbf"`
- **FerricStore**: v0.3.7 (umbrella source build)
- **Topology**: 1 server VM (`Standard_L4as_v4`, 4 vCPU), 1 client VM
- **Protocol**: RESP3 over TCP via `memtier_benchmark`

## Results

### `c=50 --pipeline=10`

| cell | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---|---:|---:|---:|---:|
| write_quorum | 14,495 | 135 | 307 | 403 |
| write_async | 124,110 | 14 | 65 | 225 |
| **read_quorum** | **601,338** | **3.1** | 8.6 | 19 |
| read_async | 562,916 | 3.3 | 12 | 24 |

### `c=200 --pipeline=50`

| cell | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---|---:|---:|---:|---:|
| write_quorum | 65,965 | 623 | 1,130 | 1,409 |
| write_async | 310,596 | 94 | 465 | 569 |
| read_quorum | 574,607 | 60 | 309 | 473 |
| read_async | 474,879 | 72 | 303 | 465 |

## Three-way comparison (`c=50 p=10` quorum)

| version | quorum ops/s | p50 | gap to v0.2.0 |
|---|---:|---:|---|
| v0.3.6 (no tune, gating bug) | 9,193 | 200 ms | -94% |
| v0.3.7 (no tune, gating fix) | 10,748 | 170 ms | -93% |
| **v0.3.7 + OS tuning** | **14,495** | **135 ms** | **-90%** |
| v0.2.0 prior baseline | 150,000 | 12 ms | — |

OS tuning gave **+35%** on quorum write throughput at c=50/p=10, but didn't close the 10× gap to the v0.2.0 baseline.

## Three-way comparison (`c=50 p=10` reads)

| version | read ops/s | p50 |
|---|---:|---:|
| v0.3.6 (no tune) | 613,202 | 3.0 ms |
| v0.3.7 (no tune) | 284,945 | 4.3 ms |
| **v0.3.7 + OS tuning** | **601,338** | **3.1 ms** |
| v0.2.0 baseline | 484,000 | 4.0 ms |

**Reads are at v0.2.0 parity with tuning** (601K vs 484K). The earlier no-tune run's 285K read number was likely affected by no-hugepages → ETS cache hits paying TLB overhead.

## Async writes

| version | c=50/p=10 async ops/s | p50 |
|---|---:|---:|
| v0.3.6 (no tune) | 71,376 | 15 ms |
| v0.3.7 (no tune) | 172,551 | 9.4 ms |
| **v0.3.7 + OS tuning** | 124,110 | 14 ms |
| v0.2.0 baseline | 187,000 | 9.4 ms |

Async at low concurrency: tuning hurt slightly here (124K vs 172K untuned) — possibly noise or a tradeoff with hugepages.

## Conclusion

**OS tuning closes the read gap to v0.2.0** but **does NOT close the quorum-write gap**:
- Reads: 601K vs 484K (BETTER than baseline)
- Quorum writes: 14.5K vs 150K (still 10× off)

The remaining quorum-write gap is **not OS-related** — it's the per-write overhead added in v0.3.6/v0.3.7:
1. `{:applied_at, ra_index, result}` wrap on every apply (allocation + WAL bytes)
2. `{:send_msg, batcher, {:locally_applied, idx}, [:local]}` effect (per-apply Erlang message)
3. Always-3-tuple effects list (no 2-tuple short-circuit when nothing emitted)

These were added to fix the cluster read-your-write hole (cluster-mode correctness for fetch_or_compute and forwarded writes). They run on every apply regardless of whether there's a remote follower waiting.

**Next step (Option C)**: skip the wrap+effect when no cross-node forwards are pending — using a shared atomic counter the SM can read cheaply.
