# FerricStore — Azure 4-cell @ c=200 p=30 on REAL local NVMe

- **Date (UTC)**: 2026-04-30 ~06:40
- **Server**: Azure `Standard_L4as_v4`, 4 vCPU, /data on `/dev/nvme1n1` (local NVMe)
- **Client**: Azure `Standard_D2as_v4`, separate VM, same VNet
- **FerricStore**: source @ `8c840c1` (codex commits applied)
- **OS tuning**: hugepages=512, THP=never, NVMe scheduler=none
- **BEAM tuning**: `+sbt db +sbwt very_short +K true +A 128`
- **Workload**: 256B values, RESP3 over TCP via `memtier_benchmark`
- **Config**: `-c 200 --pipeline=30 -t 4 --test-time=30`
- **Setup**: clean state per cell (data wiped + server restarted)

## Results

| cell | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---|---:|---:|---:|---:|
| **write_quorum** | **175,650** | 129 | 338 | 391 |
| **write_async** | **267,935** | 59 | 414 | 532 |
| **read_quorum** | **592,684** | 37 | 125 | 194 |
| **read_async** | **387,057** | 36 | 200 | 11,403 |

## Comparison vs c=200 p=50 (yesterday's bench, same server)

| cell | c=200 p=50 (10K in-flight) | c=200 p=30 (6K in-flight) | Δ throughput | Δ p50 |
|---|---:|---:|---|---|
| write_quorum | 188,950 ops/s, p50 202 ms | 175,650 ops/s, p50 129 ms | -7% | **-36%** |
| write_async | 292,438 ops/s, p50 93 ms | 267,935 ops/s, p50 59 ms | -8% | **-37%** |
| read_quorum | 780,395 ops/s, p50 45 ms | 592,684 ops/s, p50 37 ms | -24% | -18% |
| read_async | 475,428 ops/s, p50 65 ms | 387,057 ops/s, p50 36 ms | -19% | **-45%** |

## Interpretation

Reducing pipeline 50→30 (10K → 6K in-flight requests at c=200) trades a **~10-25% throughput drop for a ~20-45% latency drop**. This matches Little's Law: at saturation, observed latency ≈ in_flight / throughput.

For latency-sensitive workloads, **p=30 is a better operating point than p=50**:
- write_async drops p50 from 93ms → 59ms while still doing 268K ops/sec
- read_async p50 cuts in half (65ms → 36ms) at 387K ops/sec
- write_quorum p50 drops 36% (202ms → 129ms) at 175K ops/sec

If you need MORE latency reduction, drop further. The previous latency curve shows real per-request latency floors:

| concurrency | read_quorum p50 | server response time |
|---|---:|---|
| c=1, p=1 | 0.367 ms | true server work + RTT |
| c=10, p=1 | 0.391 ms | still uncongested |
| c=50, p=1 | 0.759 ms | starting to queue |
| c=200, p=30 | 37 ms | client-side queueing dominant |
| c=200, p=50 | 45 ms | more queueing |

## Files

- `write_quorum.{log,json}`, `write_async.{log,json}`, `read_quorum.{log,json}`, `read_async.{log,json}` — full memtier output per cell
