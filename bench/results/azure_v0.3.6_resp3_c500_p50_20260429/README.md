# FerricStore v0.3.6 — RESP3/memtier 4-cell benchmark

- **Date (UTC)**: 2026-04-29 07:52–07:54
- **Deployment**: Azure `northcentralus`
  - 1× server: `Standard_L4as_v4` (4 vCPU, 32 GiB RAM, NVMe local SSD)
  - 1× client: `Standard_D2as_v4` (2 vCPU, 8 GiB RAM)
  - Both VMs on same VNet, private IP connectivity
- **FerricStore version**: v0.3.6 (umbrella built from `v0.3.6` git tag with checksum files populated from Hex)
- **Topology**: 1 ferricstore node, 4 shards (auto-derived from `System.schedulers_online()` on 4-vCPU VM)
- **Environment**:
  - `FERRICSTORE_PROTECTED_MODE=false`
  - `FERRICSTORE_NAMESPACE_DURABILITY="async:=async"`
  - Default kernel (no sysctl tuning — cloud-init failed those steps)
- **Protocol**: RESP3 over TCP via `memtier_benchmark --protocol=resp3`
- **Workload**: 30s per cell, **`-c 500 --pipeline=50 -t 4 --data-size=256 --random-data`**
- **Pre-population for reads**: 100k keys × 10s SET-only run before each read cell

## Results

| cell | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) | KB/sec |
|---|---:|---:|---:|---:|---:|
| **write_quorum** | 65,722 | 1,597 | 2,785 | 3,604 | 19,179 |
| **write_async** | 355,513 | 270 | 565 | 885 | 105,849 |
| **read_quorum** | 541,212 | 161 | 553 | 918 | 15,269 |
| **read_async** | 461,666 | 173 | 668 | 1,180 | 72,063 |

> Note: latencies are reported in milliseconds because memtier reports msec at this concurrency. With `c=500 p=50` we have up to 25,000 in-flight requests, so per-request latency is dominated by client-side queueing.

## Headline numbers

- **Quorum writes: 65.7K ops/sec** — full Raft consensus + Bitcask fsync per batch
- **Async writes: 355.5K ops/sec** — `async:` namespace, no Raft, no fsync (5.4× quorum throughput)
- **Reads: 461K–541K ops/sec** — ETS hot-cache served, namespace-agnostic
- **Hits/misses on read_async**: 219K hits + 242K misses — the prepop only covered ~half the key range memtier read from at this throughput. Read latency numbers still valid.

## How this differs from the prior `azure_20260418` baseline

| metric | azure_20260418 (memtier, c=200 p=50) | this run (memtier, c=500 p=50) |
|---|---|---|
| Quorum writes | 50,255 ops/sec | **65,722 ops/sec** (+30%) |
| Async writes | (n/a — `c=50 p=10` config) | 355,513 ops/sec |
| Network | Same VNet, private IPs | Same |
| Server VM | `Standard_L4as_v4` | `Standard_L4as_v4` |
| Shard count | 2 | 4 (auto, from 4 schedulers) |
| FerricStore version | `235ed9f` (pre-0.3.0) | `v0.3.6` |

## Notes on pre-population for reads

The pre-population step only ran for **10s** before each read cell, with `c=500 p=50` — meaning it issued ~3-4M SETs to a `--key-maximum=100000` range (so heavy overwrites). However at the read step memtier picks keys from `--key-maximum=10000000` (default), causing many cache misses. To make read-cell numbers more representative, the prepop should match the read range, or read-cell should use `--key-maximum=100000`. The current numbers reflect the actual workload (mix of hits + misses), not pure-cache-hit performance.

## Files

- `write_quorum.json` — full memtier JSON (per-1ms latency histograms etc.)
- `write_async.json`
- `read_quorum.json`
- `read_async.json`

## Known issue uncovered during this bench

`v0.3.6` server replies with **RESP3 null (`_\r\n`)** to GETs on missing keys even when the client connected with RESP2 (no `HELLO 3`). This breaks `memtier_benchmark` in default `--protocol=redis` mode (which is RESP2). Worked around by passing `--protocol=resp3`. Should be fixed in code: track per-connection protocol version and use `$-1\r\n` for RESP2 clients.
