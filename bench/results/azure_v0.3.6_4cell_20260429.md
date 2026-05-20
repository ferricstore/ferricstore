# FerricStore v0.3.6 — 4-cell read/write × async/quorum benchmark

- Date (UTC): 2026-04-29 07:17:06Z
- Deployment: Azure `northcentralus`, 1 server VM (`Standard_L4as_v4`, 4 vCPU, 32 GiB RAM, NVMe local SSD), 1 client VM (`Standard_D2as_v4`, 2 vCPU, 8 GiB RAM)
- Topology: 1 ferricstore node, 3 shards
- Protocol: erpc from client → ferricstore on server
- Remote node: `:"ferricstore@ferricstore-0"`
- Payload: 256 bytes
- Parallel workers: 50
- Run duration: 15s (after 3s warmup)
- Pre-populated keys: 100000 per namespace
- ferricstore version: v0.3.6 (from Hex, x86_64-linux-gnu precompiled NIFs)

## Results

| cell | ops/sec | p50 (µs) | p99 (µs) | p99.9 (µs) | total ops |
|---|---:|---:|---:|---:|---:|
| write_quorum | 1246 | 32332 | 323375 | 623620 | 18688 |
| write_async | 35000 | 1299 | 4280 | 9853 | 524996 |
| read_quorum | 41789 | 1087 | 3241 | 9882 | 626829 |
| read_async | 43376 | 1079 | 2587 | 8928 | 650642 |

## Notes

- `write_quorum`: SET to default namespace — full Raft consensus + Bitcask fsync per batch.
- `write_async`: SET to `pv:` namespace — fire-and-forget, no Raft, no fsync.
- `read_quorum`: GET on default-namespace keys, served from ETS hot cache (or pread fallback).
- `read_async`: GET on `pv:` keys, same hot-path as quorum reads.

CSV: `/tmp/erpc_4cell_20260429_071706.csv`
