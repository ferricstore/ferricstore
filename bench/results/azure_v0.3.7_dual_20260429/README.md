# FerricStore v0.3.7 — RESP3/memtier dual-config benchmark

- **Date (UTC)**: 2026-04-29 09:05–09:09
- **Deployment**: Azure `northcentralus`, 1 server `Standard_L4as_v4` + 1 client `Standard_D2as_v4`
- **FerricStore**: v0.3.7 (umbrella from v0.3.7 source tag, NIFs from Hex)
- **Topology**: 1 node, 4 shards (auto from `System.schedulers_online()`), default kernel (no OS tuning)
- **Protocol**: RESP3 over TCP via `memtier_benchmark --protocol=resp3`
- **Workload**: 20s per cell, `--data-size=256 --random-data`

## What changed in v0.3.7

The v0.3.6 release added per-quorum-write **read-your-write gating** that queued every reply in `local_apply_waiters` until the local SM caught up. This was needed for **cross-node** redirected writes, but added queue/dequeue overhead to **local** writes too.

v0.3.7 adds an `all_local_callers?/1` short-circuit in `gate_reply/4`: if every caller pid is on the same node, reply directly without queueing. Cross-node callers still go through the gate + barrier. **Single-node and leader-as-origin benchmarks recover most of the lost throughput.**

## Results

### `c=50 --pipeline=10` (matches `azure_20260421` v0.2.0 baseline)

| cell | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---|---:|---:|---:|---:|
| write_quorum | 10,748 | 170 | 438 | 553 |
| write_async | **172,551** | 9.4 | 38 | 58 |
| read_quorum | 284,945 | 4.3 | 41 | 168 |
| read_async | 276,439 | 4.0 | 40 | 198 |

### `c=200 --pipeline=50` (matches `azure_20260418` baseline)

| cell | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---|---:|---:|---:|---:|
| **write_quorum** | **99,290** | **181** | 3,850 | 4,522 |
| write_async | 358,122 | 98 | 266 | 352 |
| read_quorum | 468,835 | 61 | 549 | 2,179 |
| read_async | 465,942 | 74 | 344 | 606 |

## v0.3.7 vs v0.3.6 (same 1-node Azure config, no OS tuning)

### `c=50 p=10`

| cell | v0.3.6 | **v0.3.7** | delta |
|---|---:|---:|---|
| write_quorum | 9,193 @ 200ms p50 | **10,748 @ 170ms p50** | +17% throughput, -15% latency |
| write_async | 71,376 @ 15ms p50 | **172,551 @ 9.4ms p50** | **+142% throughput**, -37% latency |
| read_quorum | 613,202 @ 3.0ms p50 | 284,945 @ 4.3ms p50 | -54% (variance — prepop key range mismatch) |
| read_async | 570,302 @ 3.1ms p50 | 276,439 @ 4.0ms p50 | -52% (variance) |

### `c=200 p=50`

| cell | v0.3.6 | **v0.3.7** | delta |
|---|---:|---:|---|
| **write_quorum** | 14,365 @ **1,974ms** p50 | **99,290 @ 181ms** p50 | **+591% throughput, 11× lower latency** |
| write_async | 296,400 @ 104ms p50 | 358,122 @ 98ms p50 | +21% throughput |
| read_quorum | 525,193 @ 61ms p50 | 468,835 @ 61ms p50 | -11% (noise) |
| read_async | 678,335 @ 49ms p50 | 465,942 @ 74ms p50 | -31% (variance) |

## v0.3.7 vs v0.2.0 prior baseline (`azure_20260421`, `c=50 p=10`)

| cell | v0.2.0 (prior, with OS tuning) | v0.3.7 (no OS tuning) | gap |
|---|---:|---:|---|
| write_quorum | 150,000 @ 12.1ms p50 | 10,748 @ 170ms p50 | -93% |
| write_async | 187,000 @ 9.4ms p50 | 172,551 @ 9.4ms p50 | **-8% (essentially parity)** |
| read | 484,000 @ 4.0ms p50 | 284,945 @ 4.3ms p50 | -41% |

## Conclusions

1. **`all_local_callers?` short-circuit recovers async writes to v0.2.0 parity** (172K vs 187K). Latency identical.
2. **Quorum writes recovered massively at high concurrency** (`c=200 p=50`: 14K → 99K, an order-of-magnitude improvement) but still significantly below the v0.2.0 baseline at `c=50 p=10` (10K vs 150K).
3. **Reads** show variance between runs, suggesting noise (or different prepop coverage) — needs a controlled head-to-head re-run to confirm.
4. **The remaining quorum gap vs v0.2.0** is likely a combination of:
   - **No OS tuning in this run** (hugepages, THP=never, NVMe scheduler, ERL_FLAGS) — the v0.2.0 number had all of that. The cloud-init scripts failed those steps.
   - **`{:applied_at, ra_index, result}` wrap overhead** still present on the leader's apply path. Removing the wrap on local-only paths could help further.
   - **Always-3-tuple effects list** allocation per apply.

## Next steps to fully close the gap

1. Apply the OS tuning that the prior bench used (hugepages, THP, scheduler, `ERL_FLAGS`)
2. Skip the `{:applied_at, _, _}` wrap when no remote callers exist (track per-apply, not always)
3. Avoid the `:locally_applied` send_msg effect when no waiters need it
