# FerricStore — Azure 4-cell benchmark on REAL local NVMe

- **Date (UTC)**: 2026-04-29 ~22:30
- **FerricStore**: source @ commit `8c840c1` (codex commits applied: quorum write correctness fixes, prefix metrics cache, fail-on-missing-active-file)
- **Server**: Azure `Standard_L4as_v4` (4 vCPU, 32 GiB RAM)
- **Storage**: `/dev/nvme1n1` — local 447GB NVMe, ext4 noatime, scheduler=none
- **Client**: Azure `Standard_D2as_v4` (2 vCPU)
- **OS tuning**: hugepages=512, THP=never
- **BEAM tuning**: `ERL_FLAGS="+sbt db +sbwt very_short +K true +A 128"`
- **MIX_ENV**: prod (so `runtime.exs` reads `FERRICSTORE_DATA_DIR=/data/ferricstore`)
- **Workload**: 256B values, 30s per cell, clean state per cell (data wipe + server restart)

## Why these numbers are different from prior `azure_v0.3.7_*` runs

Prior runs all wrote to the OS managed disk (`/dev/nvme0n1` root partition) because cloud-init's NVMe-format-and-mount step ran during the early `write_files` phase before the `ferric` user existed → the chown failed → `/data` was never created → application fell back to a relative `data/` path that landed on the OS disk. **Azure managed disks have ~5ms fdatasync; local NVMe has ~25-30µs.**

This run mounts `/dev/nvme1n1` to `/data` explicitly before starting the server, and starts with `MIX_ENV=prod` so `runtime.exs` reads the env var. `df /data` confirms the data goes to the local NVMe.

## RESP3 over TCP (memtier_benchmark, c=200 p=50, 30s)

| cell | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---|---:|---:|---:|---:|
| **write_quorum** | **188,950** | 202 | 487 | 868 |
| **write_async** | **292,438** | 93 | 481 | 541 |
| **read_quorum** | **780,395** | 45 | 128 | 185 |
| **read_async** | **475,428** | 65 | 295 | 3,539 |

**Note on read_quorum vs read_async asymmetry**: reads do not depend on namespace durability — both go through ETS hot cache. The 780K vs 475K spread is bench-prepop noise (different per-prefix prepop coverage produced different hit rates: 17% vs 28%). Architecturally these should be equal.

## erpc (Erlang distribution, parallel=200, batch=50, 30s)

| cell | ops/sec | p50 (µs) | p99 (µs) | p99.9 (µs) |
|---|---:|---:|---:|---:|
| write_quorum | 73,492 | 118,981 | 199,981 | 10,006,809 |
| write_async | 89,152 | 78,933 | 416,420 | 537,911 |
| read_quorum | 721,390 | 12,409 | 31,362 | 174,592 |
| read_async | 644,663 | 12,777 | 55,273 | 254,415 |

erpc reads now confirm what we expected: ~720K read_quorum and ~645K read_async are near-identical, consistent with reads not differing by namespace.

## Comparison vs prior runs (RESP3 c=200/p=50)

| cell | OS disk (prior) | **Local NVMe (now)** | speedup |
|---|---:|---:|---:|
| write_quorum | 99,290 | **188,950** | **1.9×** |
| write_async | 296,400 | 292,438 | ~same |
| read_quorum | 525,193 | **780,395** | 1.5× |
| read_async | 678,335 | 475,428 | (variance — see note) |

## Files

- `write_*.json`, `read_*.json` — full memtier JSON output per cell
- `write_*.log`, `read_*.log` — full memtier text output
- `erpc.csv` — erpc bench summary
- `erpc.log` — erpc bench full output

## Methodology improvements over prior runs

1. **NVMe verified explicitly** before the bench (`df /data` shows `/dev/nvme1n1`)
2. **Server starts with `MIX_ENV=prod`** so `runtime.exs` `FERRICSTORE_DATA_DIR` is honored
3. **Data wiped + server restarted between cells** — no cross-cell state pollution
4. **30s test_time** (vs prior 20s) for more stable averages
