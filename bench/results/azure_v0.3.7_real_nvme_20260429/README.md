# FerricStore v0.3.7 — Azure benchmark on REAL local NVMe

## TL;DR — prior benches were on managed disk, not NVMe

All earlier `azure_v0.3.*` bench results in this directory used `/data` mounted on the **OS managed disk** (`/dev/nvme0n1`, network-attached, ~5ms fdatasync). Cloud-init's "format and mount local NVMe" step had been failing silently because it ran in early cloud-init phases before the `ferric` user existed, so `/data` was never created and the application fell back to a relative path that landed on the OS disk.

Manually mounting `/dev/nvme1n1` (the actual local 447GB NVMe), setting `MIX_ENV=prod` so `runtime.exs` reads `FERRICSTORE_DATA_DIR`, and re-running yields:

| config | OS disk (prior) | **Real NVMe (now)** | speedup |
|---|---:|---:|---:|
| c=50/p=10 quorum | 13,696 ops/s, p50 137ms | **80,592 ops/s, p50 24ms** | **5.9×** |
| c=200/p=50 quorum | 99,290 ops/s, p50 181ms | **200,969 ops/s, p50 198ms** | **2.0×** |
| Single SET, no load | 13ms p50 | **8ms p50** | 1.6× |

## Environment

- **Date (UTC)**: 2026-04-29 ~17:40
- **Server**: Azure `Standard_L4as_v4`, 4 vCPU
- **Storage**: `/dev/nvme1n1`, 447GB local NVMe, ext4 noatime/nodiratime, scheduler=none
- **Client**: Azure `Standard_D2as_v4` (separate VM, same VNet)
- **OS tuning**: hugepages=512, THP=never (applied via apply_os_tuning.sh)
- **BEAM tuning**: `ERL_FLAGS="+sbt db +sbwt very_short +K true +A 128"`
- **FerricStore**: v0.3.7 source, NIFs cargo-built from source
- **Workload**: SET-only quorum (default namespace, Raft + fsync), 256B values, RESP3 over TCP
- **Server start**: `MIX_ENV=prod FERRICSTORE_DATA_DIR=/data/ferricstore mix run --no-halt`

## Detailed quorum-write results

### c=50 p=10 (low concurrency)

```
Totals  80,592.29 ops/sec  avg=24.80ms  p50=23.68ms  p99=48.90ms  p99.9=238.59ms
```

### c=200 p=50 (high concurrency)

```
Totals  200,968.99 ops/sec  avg=198.71ms  p50=197.63ms  p99=339.97ms  p99.9=423.94ms
```

### Single SET (no concurrent load)

```
n=30  min=7,724µs  avg=8,129µs  p50=8,054µs  max=9,296µs
```

## Comparison to historical baselines

The prior `azure_20260421_tuning.md` baseline claimed **150K @ c=50/p=10 quorum**. With actual local NVMe we measure **80K**. This is closer to that number but still ~50% off.

Hypotheses for the remaining gap to the 150K claim:
1. The 150K bench may have used `FERRICSTORE_SHARD_COUNT=2` or 3 (we use auto = 4 schedulers).
2. Different ra_ferricstore version or WAL NIF flags.
3. Different memtier_benchmark version.

## What we know now about the bottleneck

- **Single-write latency floor: 8ms p50** (one Raft commit + Bitcask write on a fully-warm system).
- **Per-write fdatasync: ~25-30µs** on local NVMe (measured via fio).
- The 8ms gap above raw fdatasync is the **Raft pipeline_command + ra_event reply chain + state machine apply + Bitcask write + reply queueing**.
- At higher concurrency, **per-fdatasync amortization scales** — 200K ops/s = 5µs/op = many ops per fdatasync, well within NVMe's IOPS budget.

## Files

- `bench_c50_p10.txt` — full memtier output for c=50/p=10
- `bench_c200_p50.txt` — full memtier output for c=200/p=50
- `solo_set.txt` — 30 single SETs, no concurrency
