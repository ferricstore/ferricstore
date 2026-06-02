# Flow WARaft Flag Probe - 2026-05-31

Workload for all runs:

- `bench/flow_state_lmdb_soak.exs`
- `DURATION_SECONDS=90` for flag probes, `300` for final verify
- Later code probes use `DURATION_SECONDS=75` to keep each TDD/benchmark
  iteration bounded while preserving the same workload shape.
- `TARGET_OPS_PER_SEC=50000`
- `PAYLOAD_BYTES=1000`
- `NORMAL_STEPS=50`
- `LONG_FLOWS=1`
- `LONG_STEPS=10000`
- `SHARDS=16`
- `WORKERS=32`
- `PRODUCERS=8`
- `PARTITIONS=4096`
- `CREATE_MODE=many`
- `CLAIM_BATCH_SIZE=1000`
- `CLAIM_PARTITION_BATCH_SIZE=32`
- `WORKER_MODE=blocking`

## Results

| Run | Override | Flow ops/s | Write ops/s | WARaft queue avg | WARaft flush avg | WARaft total avg | Notes |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| Old 5m reference | previous code/config | 48,473 | 95,999 | 32.5 ms | 35.6 ms | 68.1 ms | `bench/results/flow_state_lmdb_soak_current_5m_20260527T171303Z.log` |
| Bad 5m baseline | current code before flag change | 40,918 | 80,968 | 70.6 ms | 73.8 ms | 144.4 ms | Correctness counters clean, but flush queue pressure high. |
| Probe | `WARAFT_COMMIT_BATCH_INTERVAL_MS=3` | 40,358 | 79,974 | 57.5 ms | 61.6 ms | 119.0 ms | Not enough throughput gain; not kept. |
| Probe | `WARAFT_SEGMENT_PREALLOCATE_BYTES=67108864` | 40,576 | 80,426 | 51.2 ms | 55.7 ms | 106.9 ms | Slight latency improvement, no throughput recovery; not defaulted. |
| Probe | `WARAFT_HOT_BATCH_WINDOW_MS=0` | 39,113 | 77,554 | 62.6 ms | 66.9 ms | 129.5 ms | Worse; keep hot window at 1 ms. |
| Probe | `FERRICSTORE_FLOW_HISTORY_PROJECTOR_BATCH_SIZE=100000`, `FERRICSTORE_FLOW_HISTORY_PROJECTOR_FLUSH_INTERVAL_MS=5000` | 37,681 | 74,692 | 59.9 ms | 64.3 ms | 124.1 ms | Worse; keep projector at 25k/1000ms. |
| Probe | `WARAFT_APPLY_LOG_BATCH_SIZE=4096` | 42,039 | 83,323 | 49.6 ms | 53.5 ms | 103.0 ms | Best clear flag probe. |
| Probe | `WARAFT_APPLY_LOG_BATCH_SIZE=8192` | 39,865 | 79,012 | 52.2 ms | 56.7 ms | 108.9 ms | Worse than 4096. |
| Control | `FERRICSTORE_WARAFT_APPLY_LOG_BATCH_SIZE=1024` | 38,655 | 76,655 | 59.7 ms | 64.1 ms | 123.8 ms | Proved old default is worse on this workload. |
| Probe | `WARAFT_COMMIT_BATCH_MAX=20000` | 41,035 | 81,338 | 53.2 ms | 57.3 ms | 110.5 ms | Throughput similar, latency worse than default 10k. |
| Probe | `WARAFT_GENERIC_BATCH_WINDOW_MS=1` | 38,684 | 76,691 | 57.8 ms | 61.4 ms | 119.2 ms | Worse; keep generic window at 0. |
| Final 5m verify | no WARaft overrides | 42,089 | 83,314 | 65.3 ms | 68.6 ms | 134.0 ms | Correctness counters clean; remaining bottleneck is still WARaft flush queue pressure. |
| Code probe | command-level shard stamp + claim fast-index loop | 41,506 | 82,237 | 45.2 ms | 49.5 ms | 94.7 ms | Explicitly no WARaft override flags. Correctness counters clean. Better latency than final 5m verify, but still below old 48K reference. |
| Rejected code probe | plus independent transition_many bulk apply | 40,281 | 79,833 | 52.9 ms | 57.2 ms | 110.0 ms | Regressed; dropped this change. |
| Rejected code probe | enable due-any index by default | 41,059 | 81,364 | 48.7 ms | 52.8 ms | 101.5 ms | Slight throughput drop and worse total latency; dropped this change. |
| Rejected code probe | one-pass transition index planner | 41,127 | 81,520 | 48.6 ms | 52.8 ms | 101.4 ms | No throughput win and worse total latency; dropped this change. |
| Rejected code probe | carry claimed count in native multi-key claim loop | 36,963 | 73,279 | 55.6 ms | 60.0 ms | 115.6 ms | Regressed hard in the workload; dropped this change. |
| Rejected code probe | binary due-key matching + fixed auto-partition parse | 40,377 | 80,044 | 48.2 ms | 52.2 ms | 100.5 ms | Focused tests passed, but the clean soak was slightly worse than control; dropped this change. |
| Rejected probe | `FERRICSTORE_FLOW_HISTORY_PROJECTOR_BATCH_SIZE=4096`, `FERRICSTORE_FLOW_HISTORY_PROJECTOR_FLUSH_INTERVAL_MS=250` | 34,073 | 67,577 | 75.0 ms | 80.2 ms | 155.1 ms | Old projector sizing lowered memory pressure but hurt throughput badly; not defaulted. |
| Rejected code probe | async-backed history log fsync before LMDB publish | 32,880 | 65,214 | 61.8 ms | 67.0 ms | 128.7 ms | Correctness tests passed, but clean soak regressed; reverted. |
| Kept default fix | retention sweeper initial delay/interval defaulted to 600s | 40,747 | 80,769 | 51.1 ms | 56.4 ms | 107.6 ms | Process profile showed `FLOW.RETENTION_CLEANUP` running inside the 90s hot soak. The cleanup is durable and correct, but should not run every minute by default. |
| Rejected code probe | ordered async WARaft submit behind in-flight commit | 32,389 | 64,260 | 75.4 ms | 83.0 ms | 158.4 ms | Correctness tests passed, but hot batch avg fell to 1 item and queue pressure worsened; reverted. |
| Rejected code probe | ordered async submit only for batches with at least 2 items | 32,753 | 64,976 | 78.7 ms | 86.7 ms | 165.5 ms | Still worse. Overlapping submit is not the right fix without preserving group batching. |
| Safe-batcher verify | no overrides, retention defaults visible | 40,433 | 80,151 | 50.6 ms | 55.0 ms | 105.6 ms | Correctness counters clean. This is the safe baseline after dropping the async-submit experiment. |
| Kept code fix | history projector registry fast path | 39,067 | 77,423 | 46.6 ms | 51.1 ms | 97.6 ms | Short 60s verify. Removes a profiled apply-side `TableOwner.ensure_tables/0` GenServer dependency; not counted as a clear throughput win. |

## DBOS-Style Verification

After reverting the async fsync experiment, the default DBOS-style benchmark
remained healthy with no special performance flags:

- `bench/flow_python_backend_profile.exs`
- 1,000,000 flows, 16 workers, 8 producers, 16 shards, `many` transport
- Result: 57,489 end-to-end flows/sec
- Log: `bench/results/dbos_style_default_verify_20260531T155053Z.log`
- After removing the dead `FERRICSTORE_RAFT_BATCHER_*` config surface and
  fixing `WAKE_COALESCE_MS` env passthrough, the 100K default verify was
  67,645 end-to-end flows/sec.
- Log: `bench/results/dbos_style_default_100k_after_flag_cleanup_20260531T160226Z.log`

## Changes Kept

- Default `:waraft_apply_log_batch_size` is now `4096` in normal, bench, runtime, and backend fallback config.
- `bench/flow_state_lmdb_soak.exs` now prints hidden WARaft batching/segment flags in the effective config line.
- `bench/flow_state_lmdb_soak.exs` now prints Flow retention sweeper defaults in the effective config line.
- The soak script accepts both short benchmark flag names and production `FERRICSTORE_...` names for WARaft batching flags.
- Flow many apply now consumes the command-level shard stamp before falling back to per-record rehash.
- Flow claim_due fast-index planning now dispatches the hot plan tuple shapes directly instead of calling the generic plan-pair helper per item.
- Flow retention sweeper defaults now use a 600s maintenance cadence instead of running every minute. The sweeper still exists; it just does not compete with short hot-path soaks by default.
- Flow history projector pending/replay registry lookups now use existing ETS tables directly and only call the table owner when the table is missing. This keeps async history publication from waiting on a GenServer during apply.

## Conclusion

The regression is not primarily a bad environment flag. `4096` helps and should stay, and the retention cadence removes one source of unrelated storage work, but the old `48K` run had much lower WARaft queue/flush time. The rejected async-submit experiment proved that simply overlapping submits can destroy batching and make the workload worse. The remaining target is reducing WARaft flush queue pressure while preserving group batching, and reducing Flow claim/transition apply work per committed batch.
