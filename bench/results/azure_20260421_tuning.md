# Azure Tuning Benchmark — 2026-04-21

## Environment

- **Server**: Azure L4as_v4 (4 vCPU AMD EPYC 9V74, 2 cores × 2 threads, single NUMA)
- **Client**: Azure L4as_v4 (same region, ~1ms RTT)
- **Storage**: NVMe (nvme1n1), scheduler=none
- **OS**: Ubuntu, THP=never, hugepages=512 (1GB)
- **FerricStore**: v0.2.0 @ `346d99b` (batch GET fast path + decoupled WAL NIF)
- **Shards**: 3 (unless noted), async namespace enabled
- **ERL_FLAGS**: `+sbt db +sbwt very_short +swt very_low +K true +A 128 +P 5000000 +Q 65536 +MHas aoffcbf +MBas aoffcbf`

## Methodology

- memtier_benchmark over RESP3/TCP
- Default: 50 clients, 4 threads, pipeline=10, 256B values, 100K requests per client
- Clean restart (stop → wipe data → start) between each parameter change
- 100K keys prepopulated for read tests
- Quorum = SET to default namespace (Raft + fsync)
- Async = SET to `async:` namespace (ETS + nosync)
- Read = GET prepopulated keys (ETS hot cache)

## Results

### BEAM VM Tuning

All within noise of baseline — no BEAM flag makes a meaningful difference on 4 vCPUs.

| Parameter | Quorum ops/s | Quorum p50 | Quorum p99 | Async ops/s | Async p50 | Read ops/s | Read p50 | Read p99 |
|-----------|-------------|-----------|-----------|------------|----------|-----------|---------|---------|
| **Baseline** (db/very_short/A128) | 150K | 12.1ms | 25.0ms | 187K | 9.4ms | 484K | 4.0ms | 8.3ms |
| +sbt ts (thread spread) | 154K | 11.9ms | 27.4ms | 187K | 9.5ms | 469K | 4.0ms | 8.0ms |
| +sbwt short (10μs busy wait) | 154K | 12.0ms | 27.3ms | 194K | 9.2ms | 480K | 4.0ms | 8.4ms |
| +A 32 (fewer async threads) | 157K | 12.0ms | 26.0ms | 180K | 9.3ms | 491K | 4.0ms | 8.0ms |
| +SDio 4 (dirty I/O schedulers) | 154K | 12.0ms | 26.0ms | 166K | 9.5ms | 504K | 4.0ms | 8.7ms |

**Verdict**: Keep baseline. No change worth making.

### Shard Count

| Shards | Quorum ops/s | Quorum p50 | Async ops/s | Async p50 | Read ops/s | Read p50 |
|--------|-------------|-----------|------------|----------|-----------|---------|
| 1 | stalled (0) | 14.3ms | 207K | 8.5ms | 462K | 3.9ms |
| **2** | **158K** | **11.6ms** | 187K | 8.8ms | 470K | 4.0ms |
| 3 (current) | 150K | 12.1ms | 187K | 9.4ms | 484K | 4.0ms |
| 4 | 141K | 13.1ms | 185K | 9.5ms | 447K | 3.8ms |

**Verdict**: 2 shards is slightly better for quorum writes. 3 shards is slightly better for reads. Tradeoff is marginal — keep 3 for balanced workloads.

### OS: Transparent Huge Pages

| THP | Quorum ops/s | Quorum p50 | Quorum p99 | Async ops/s | Read ops/s | Read p50 |
|-----|-------------|-----------|-----------|------------|-----------|---------|
| **never** (current) | **150K** | 12.1ms | **25.0ms** | 187K | **484K** | 4.0ms |
| always | 137K | 12.2ms | **53.0ms** | 191K | 466K | 3.4ms |

**Verdict**: THP=always doubles quorum p99. Keep `never`.

### OS: Huge Pages

| Hugepages | Quorum ops/s | Quorum p50 | Quorum p99 | Async ops/s | Read ops/s | Read p50 |
|-----------|-------------|-----------|-----------|------------|-----------|---------|
| 0 (none) | 133K | 11.9ms | **46.3ms** | — | 471K | 4.0ms |
| **512 (1GB)** (current) | **150K** | 12.1ms | **25.0ms** | 187K | **484K** | 4.0ms |
| 2048 (4GB) | 132K | 13.0ms | 36.6ms | 186K | 480K | 4.0ms |

**Verdict**: 512 hugepages (1GB) is the sweet spot. 0 and 2048 both degrade quorum p99 significantly.

### Socket Active Mode

| Mode | Quorum ops/s | Quorum p50 | Quorum p99 | Read ops/s | Read p50 | Read p99 |
|------|-------------|-----------|-----------|-----------|---------|---------|
| once | 151K | 12.7ms | 24.7ms | — | 4.1ms | 10.2ms |
| 10 | 153K | 12.3ms | 24.8ms | 478K | 4.0ms | 9.6ms |
| 50 | 152K | 12.0ms | 26.0ms | 492K | 4.0ms | 10.2ms |
| 100 | 114K | 13.2ms | 26.6ms | 488K | 4.0ms | 7.6ms |
| 200 | 156K | 11.9ms | 26.6ms | 498K | 4.0ms | 8.3ms |
| **true** (default) | 151K | 12.1ms | 27.1ms | 477K | 4.0ms | 8.3ms |

**Verdict**: No meaningful difference. `active=true` is fine — bottleneck is Raft fsync and ETS lookups, not socket I/O.

### Hot Cache Max Value Size

| Max Size | Quorum ops/s | Quorum p50 | Read ops/s | Read p50 | Read p99 |
|----------|-------------|-----------|-----------|---------|---------|
| 0 (disabled) | 154K | 12.0ms | 460K | 4.1ms | 8.9ms |
| 65536 (default) | 150K | 12.1ms | 484K | 4.0ms | 8.3ms |

**Verdict**: ETS value cache gives ~5% read improvement at 256B values. Effect will be larger with bigger values and cold reads.

### Pipeline Depth (client-side)

| Pipeline | Quorum ops/s | Quorum p50 | Async ops/s | Async p50 | Read ops/s | Read p50 |
|----------|-------------|-----------|------------|----------|-----------|---------|
| 1 | 41K | **4.7ms** | — | — | 89K | **1.9ms** |
| **10** (baseline) | **150K** | 12.1ms | 187K | 9.4ms | 484K | 4.0ms |
| 50 | **256K** | 32.5ms | 367K | 27.6ms | 676K | 13.6ms |
| 100 | 195K | 92.2ms | stalled | 58.6ms | 705K | 24.9ms |

**Verdict**: Pipeline=10 is the best throughput/latency balance. Pipeline=50 peaks quorum at 256K but p50 triples. Pipeline=100 saturates and drops quorum throughput.

### Client Concurrency (pipeline=10)

| Clients | Quorum ops/s | Quorum p50 | Read ops/s | Read p50 | Read p99 |
|---------|-------------|-----------|-----------|---------|---------|
| **10** | 149K | **5.9ms** | 403K | **0.9ms** | **1.8ms** |
| 50 (baseline) | 150K | 12.1ms | 484K | 4.0ms | 8.3ms |
| 100 | 177K | 22.3ms | 434K | 8.5ms | 17.7ms |
| 200 | 167K | 44.5ms | 390K | 18.2ms | 39.7ms |

**Verdict**: 10 clients gives sub-1ms read p50 with only ~15% throughput reduction. Latency scales linearly with concurrency. For latency-sensitive workloads, keep clients low.

## Summary: Optimal Config for 4 vCPU Azure

The current configuration is near-optimal. No single tuning parameter yields a significant improvement.

**Keep as-is:**
- `+sbt db +sbwt very_short +swt very_low +K true +A 128` (BEAM flags don't matter on 4 vCPUs)
- `FERRICSTORE_SHARD_COUNT=3` (balanced)
- `FERRICSTORE_SOCKET_ACTIVE_MODE=true` (no difference)
- `vm.nr_hugepages=512`, THP=never, NVMe scheduler=none
- `FERRICSTORE_HOT_CACHE_MAX_VALUE_SIZE=65536`

**Biggest levers are client-side:**
- Pipeline depth: throughput vs latency tradeoff
- Client count: latency scales linearly

**Peak numbers (pipeline=10, 50 clients):**
- Quorum write: 150K ops/s, 12ms p50, 25ms p99
- Async write: 187K ops/s, 9.4ms p50
- Read: 484K ops/s, 4.0ms p50, 8.3ms p99
