# Performance Optimization Iterations

Local benchmark notes for the WARaft spike branch. These numbers are local
developer-machine numbers, so treat small swings as noise. Correctness tests are
listed with each code change.

## SET

Benchmark shape unless noted:

- `BENCH_MODE=set`
- `TOTAL=1_000_000`
- `CONCURRENCY=200`
- `PIPELINE=50`
- `DATA_SIZE=256`
- `SHARDS=4`
- command: `mix run --no-start bench/waraft_resp_router_bench.exs`

| Iteration | Change | Result | Notes |
| --- | --- | --- | --- |
| Baseline | WARaft segment-keydir + async append before current loop | `847,662 ops/sec`, `206.95 MiB/sec`, batch p99 `13.734 ms` | Stable 1M run; confirms the earlier short low run was noise. |
| Profile | `PROFILE_PROCESSES=1` bench-only process reduction snapshot | `798,158 ops/sec`, batch p99 `22.290 ms` | Profiling overhead included. Top reductions were `raft_storage_ferricstore_waraft_backend_*` at about `12M` each; batchers were about `1M`; Ra servers about `0.2M`. |
| Profile after SET-8 | `TOTAL=500000`, `PROFILE_PROCESSES=1` | `767,068 ops/sec`, batch p99 `14.652 ms` | Storage apply still dominates reductions: each `raft_storage_ferricstore_waraft_backend_*` process consumed about `5.85M` reductions; batchers were about `0.52M`; Ra servers about `0.105M`. Eprof was unavailable in this local Erlang build. |
| SET-1 | Hoist WARaft segment `put_batch` shard ETS state and hot-cache threshold out of the per-entry loop | `822,027 ops/sec`, `200.69 MiB/sec`, batch p99 `14.360 ms` | Correctness preserved by still using `ShardETS.ets_insert_with_location/9` for binary tracking, expiry accounting, and hot-cache threshold semantics. Local result is within noise, not a proven throughput win. |
| SET-2 | Router WARaft SET/DEL fixed shard tuple buckets and tuple result reassembly | `827,294 ops/sec`, batch p99 `13.642 ms`; repeat `819,155 ops/sec`, p99 `18.363 ms` | Removes map allocation in multi-shard grouping/reassembly. Correctness guard ensures the hot path stays on fixed buckets. |
| SET-3 | Router WARaft SET/DEL multi-shard batches submit with alias waiters instead of `Task.async` per shard | `860,660 ops/sec`, `210.12 MiB/sec`, batch p99 `13.866 ms` | Keeps one-shard path synchronous, but multi-shard pipeline submit now uses WARaft batcher casts and waits on `ReplyAwaiter`; unresolved replies stay `write_timeout_unknown`. |
| SET-4 | SET/DEL hot result merge writes ordered tuple results directly while counting success/error | `856,984 ops/sec`, `209.22 MiB/sec`, batch p99 `13.233 ms`, p999 `14.346 ms` | Similar throughput to SET-3, slightly better tail on this run. Keeps one result per input command and preserves partial-error placement. |
| SET-5 | Namespace-window write batches preserve compact `put_batch`/`delete_batch` commit shape instead of generic `{:batch, commands}` | Router SET reruns: `804,142 ops/sec`, p99 `22.843 ms`; repeat `813,873 ops/sec`, p99 `17.356 ms` | This path is for configured namespace windows, not normal router SET, so the router SET rerun is mostly a noise check. Tests prove windowed homogeneous SET/DEL uses the compact backend commit path. |
| SET-6 | Unified segment projection fast-paths homogeneous generic `{:batch, [{:put, ...}]}` and `{:batch, [{:delete, ...}]}` into `put_batch`/`delete_batch` projection | Targeted tests passed; no standalone throughput benchmark yet | Keeps mixed command batches on the generic reducer, but prevents homogeneous replay/window batches from paying per-command projection overhead. |
| SET-7 | Unified segment generic batch projection decodes and classifies homogeneous put/delete batches in one pass | Targeted tests passed; no standalone throughput benchmark yet | Avoids decoding the batch once and scanning it again before the put/delete fast projection. Mixed batches still fall back to generic projection. |
| SET-8 | Removed the old post-decoder homogeneous fast-path scan from generic segment batches | `814,602 ops/sec`, `198.88 MiB/sec`, batch p99 `18.775 ms`, p999 `24.426 ms` | The one-pass decoder already routes homogeneous put/delete batches. Mixed batches now go straight to the generic all-or-nothing projector instead of trying a redundant put/delete scan first. This SET benchmark is mostly a regression check because normal SET traffic already uses compact batch terms. |
| SET-9 | Fresh no-TTL segment `put_batch` preflights freshness, then uses one ETS list insert and one binary-counter update for the whole batch | Runs: `787,611 ops/sec` p99 `16.864 ms`; `833,054 ops/sec` p99 `14.847 ms`; `825,827 ops/sec` p99 `16.021 ms` | Correctness falls back to the existing per-key path for overwrites, duplicate keys, TTLs, or compound markers. Throughput is local-noise neutral; p99 is better than SET-8 in these runs. |
| Current rerun | Same SET shape after latest command optimizations | Runs: `831,996 ops/sec` p99 `19.788 ms`; `812,267 ops/sec` p99 `16.113 ms`; `848,577 ops/sec` p99 `13.905 ms` | Confirms the apparent drop from the single `860K/sec` row is not a sustained collapse. Current local range is roughly `812K-849K/sec`; `860K/sec` was the best observed run in this family. |

Tests for SET-1:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:215 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:250 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:258 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:265 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:276
```

Result: `5 tests, 0 failures`.

Tests for SET-2 through SET-4:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/store/router_test.exs:143 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:2677 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:6297 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:8871 \
  apps/ferricstore/test/ferricstore/store/batch_operations_test.exs:149
```

Result: `5 tests, 0 failures`.

Tests for SET-5:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:1276 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:1310 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:1237 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:2685 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:3033
```

Result: targeted namespace-window compact-shape tests passed.

Tests for SET-6:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:276 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:1444
```

Result: targeted segment projection tests passed.

Tests for SET-7:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:284 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:276 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:1444
```

Result: `3 tests, 0 failures`.

Tests for SET-8:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:276 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:284 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:1444 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:1461
```

Result: targeted segment projection tests passed.

Tests for SET-9:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/store/shard_ets_test.exs:13 \
  apps/ferricstore/test/ferricstore/store/shard_ets_test.exs:51 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:276 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:1444 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:1461
```

Result: `5 tests, 0 failures`.

## Flow

Important: the Flow table currently contains two benchmark families. Rows
`Baseline` through `Flow-8` used the earlier local Flow profile and RESP
pipeline microbench variants. Rows starting at `Profile after Flow-14` use the
worker-style Python backend profile with `FLOWS=100000`, `transport=many`, and a
different end-to-end workload shape. Do not compare the `~95K/sec` rows directly
to the `~50K/sec` rows; compare rows inside the same benchmark family only.

Benchmark shape used for the current worker-style WARaft flow profile:

- `BACKENDS=waraft`
- default profile workload in `bench/flow_python_backend_profile.exs`
- default `SHARDS=16`

| Iteration | Change | Result | Notes |
| --- | --- | --- | --- |
| Baseline | WARaft flow profile, async append disabled | `89,681 workflows/sec`, create `91,035/sec`, process `89,755/sec` | Flow path dominated by Bitcask/Flow apply records. |
| Flow-1 | WARaft async append path | `92,133 workflows/sec`, create `93,265/sec`, process `92,214/sec` | About `2.7%` better locally. The profile showed about `5M` Bitcask append records for `1M` workflows, so WAL async alone is not the main limiter. This is now the only WARaft mode. |
| Flow-2 | `flow_new_named_value_refs/4` skips named-value normalization when no named values/refs are present | `92,498 workflows/sec`, create `93,409/sec`, process `92,582/sec` | Correct but not a proven win. Encoding already omits empty `value_refs`, so this mainly saves a small branch/empty-map path. |
| Flow-3 | Router Flow many/create-pipeline grouping uses fixed shard tuple buckets instead of map grouping | `94,195 workflows/sec`, create `94,828/sec`, process `94,286/sec` | Targets `FLOW.CREATE_MANY`/pipeline-style independent creates and mixed-partition many commands. |
| Flow-4 | Router Flow create-pipeline result reassembly uses an ordered tuple instead of an index map | `95,405 workflows/sec`, create `96,252/sec`, process `95,491/sec` | Removes per-result map hashing while preserving response order. |
| Flow-5 | Fast create apply removes duplicate due-index insertion pass | `91,518 workflows/sec`; repeat `90,525 workflows/sec` | Not a proven throughput win in noisy local runs, but it removes duplicated lifecycle-index work. Correctness tests cover create, due claim, and command surface. |
| Flow-6 | Multi-partition `claim_due` aggregation uses reverse accumulation instead of `acc ++ records` | `94,402 workflows/sec`, create `95,035/sec`, process `94,479/sec` | Avoids quadratic list concatenation when a claim spans many partitions/shards; order is preserved by final reverse before limit. |
| Flow-7 | `pipeline_write_batch_independent/2` assembles ordered results as a list instead of an index map | Many-transport profile: `91,269 workflows/sec`; pipeline profile: `5,580 workflows/sec` | The many profile does not exercise this path. The pipeline profile exposed `FLOW.CLAIM_DUE` as the real RESP pipeline bottleneck. |
| Flow-8 | RESP `FLOW.CLAIM_DUE` pipeline batches singleton distinct-partition claims through `Router.pipeline_write_batch/2` | `FLOW.CLAIM_DUE x100`: `52 cmds/sec` -> `12,471 cmds/sec`; avg pipeline latency `1,912,080 us` -> `8,019 us` | Preserves correctness by keeping one state-machine command per claim and only batching Raft submission by shard. Tests prove distinct partitions do not overclaim. |
| Flow-9 | Adjacent `FLOW.CLAIM_DUE` pipeline fallback prepends coalesced run results without `Enum.reverse(results) ++ acc` | Targeted tests passed; no standalone throughput benchmark yet | Avoids copying every coalesced run list while preserving final response order. This is a small allocation cleanup on the fallback path when global grouping is not safe. |
| Flow-10 | Auto-partition and LMDB query aggregators accumulate chunks instead of `records ++ acc` / `ids ++ acc` | Targeted tests passed; no standalone throughput benchmark yet | Avoids quadratic list concatenation in wide Flow query paths while preserving existing reverse-partition/path behavior before final sort/take. |
| Flow-11 | Flow history projection skips `Enum.group_by/2` when all pending history entries belong to the current apply shard | Targeted tests passed; no standalone throughput benchmark yet | Common same-shard create/transition/complete paths now call the shard projection directly; cross-shard projection still groups exactly as before. |
| Flow-12 | Flow history apply batches skip the after-history trim/terminal-mirror pass when records are active and below trim limits | Targeted tests passed; no standalone throughput benchmark yet | Transition-many now builds history entries and next records in one pass, then skips per-record trim checks for the common no-trim, non-terminal case. |
| Flow-13 | Retry-many history apply builds retry history entries and next records in one traversal | Targeted tests passed; no standalone throughput benchmark yet | Same correctness as before, but avoids mapping the retry plan list twice before after-history cleanup. |
| Flow-14 | Fast create history apply builds history entries and records in one traversal | Targeted tests passed; no standalone throughput benchmark yet | Removes the second pass over fast-create plans before the after-history cleanup gate. |
| Profile after Flow-14 | Worker-style Flow Python profile, `FLOWS=100000`, `transport=many`, `SHARDS=16` | `49,420 workflows/sec` end-to-end; create `51,335/sec`; process `49,457/sec` | Telemetry still points at storage append volume, not Elixir list assembly: `500K` Bitcask records over `758` appends, avg batch `660`, plus `707` same-segment WARaft appends. |
| Flow-15 | Put-new Flow indexes record rollback originals as known-missing instead of probing each member score | `49,987 workflows/sec` end-to-end; create `52,362/sec`; process `50,042/sec` | Keeps rollback count snapshots, but skips per-member ETS score lookups where `FlowIndex.put_new_*` already has absent-member semantics. Bitcask record count is unchanged at `500K`; claim/complete telemetry improved. |
| Flow-16 | Fast-create state records record pending originals as known-missing and track keydir bytes without an ETS lookup | `49,950 workflows/sec` end-to-end; create `51,654/sec`; process `49,994/sec` | Correctness fallback remains for non-fast paths. Local result is neutral versus Flow-15, so this is kept as an apply-work reduction rather than a proven throughput win. |
| Worker 1M check | Python worker profile at `FLOWS=1000000`, `SHARDS=16`, `TRANSPORT=many` | `92,289 workflows/sec` end-to-end; create `93,004/sec`; process `92,375/sec` | Completed `1,000,000` created/claimed/completed with `0` duplicate completions. This run used the current WARaft worker path and should be compared to the same 1M worker shape, not the earlier 100K local profile rows. |
| Flow-17 | RESP Flow pipeline passes typed Rust AST write/read tuples directly into Flow batch helpers | `FLOW.CREATE x100`: `13,774 cmds/sec`; `FLOW.CLAIM_DUE x100`: `12,508 cmds/sec`; `FLOW.TRANSITION x100`: `16,616 cmds/sec` | Removes the TCP pipeline remap from Rust AST tuples into second Flow-only op shapes. Core still accepts the older op tuples for internal callers. Read AST pass-through is covered by targeted tests; the benchmark shown is the existing write-focused RESP pipeline shape. |
| Flow-17 Python 1M check | Python worker profile at `FLOWS=1000000`, `SHARDS=16`, `TRANSPORT=many` | `94,837 workflows/sec` end-to-end; create `95,651/sec`; process `94,918/sec` | Completed `1,000,000` created/claimed/completed with `0` duplicate completions. Telemetry: `5,000,000` Bitcask records over `3,072` appends, avg append batch `1,627`, `3,088` WARaft same-segment writes. |
| Flow-18 | Flow history projector computes retention cap requirements in one pass instead of scanning the whole projection batch once per history key | Pre-fix 100K repeats: `61,595`, `58,411`, `56,631/sec`; post-fix repeats: `72,740`, `66,061`, `70,543/sec`; post-fix 1M: `70,939/sec` | Fixes the DBOS-style workload where every flow has a distinct history key. The old projector path became O(history_keys x batch_entries) even when every entry was far below `history_max_events`. New tests cover no cap loads for unique under-cap entries and one cap load for missing-cap keys. The 1M run completed `1,000,000` created/claimed/completed with `0` duplicate completions. |
| Long-history soak 1 | `bench/flow_state_lmdb_soak.exs`, `5KB` payloads, normal flows `50` states, one long failure flow target `10K` states, `SHARDS=16`, `WORKERS=32`, `PRODUCERS=8`, `TARGET_OPS_PER_SEC=50000` | Stopped early at `901.5s`; `12,311,930` Flow ops, `24,312,877` write ops, `13,657 Flow ops/sec`, `26,969 writes/sec` | Did not hit disk/memory guards. Max LMDB pending `65,306`, oldest lag `949ms`, replay lag `27,202`, so LMDB projection was behind but not the main limiter. Max BEAM memory `70.6GB`, binary memory `38.9GB`, ETS `30.3GB`, keydir entries `36.9M`, disk `27.9GB` (`15GB` data, `11GB` WARaft, `1.4GB` blob). Failure mode was write timeout/unknown outcome plus one LMDB env-open warning; added WARAFT commit-timeout telemetry afterward. |
| Long-history smoke 2 | SDK workers use separate blocking-claim and command clients; soak wrapper defaults to `WORKER_MODE=blocking` and no longer passes the old wake-coalesce flag | 3-minute clean smoke: `1,856,440` Flow ops, `9,986 Flow ops/sec`, `19,502 writes/sec`, max LMDB pending `11,657`, oldest lag `1.14s` | This fixed the previous stuck shape (`~7 ops/sec`) but exposed that the pipeline create driver was still underfeeding the server. Log: `bench/results/flow_state_lmdb_soak_blocking_split_smoke_20260524T213547Z.log`. |
| Long-history smoke 3 | Python pipeline create mode now honors `--create-inflight`; unit test proves concurrent pipeline flushes | 2-minute clean smoke: `1,563,762` Flow ops, `12,519 Flow ops/sec`, `24,184 writes/sec`, max LMDB pending `9,871`, oldest lag `1.11s` | Feed rate improved, but this workload is still WARAFT/storage bound on `5KB` payload history: hot batch avg only `3` commands, WARAFT total flush avg `197ms`, p99 pipeline write `<=1s`. Log: `bench/results/flow_state_lmdb_soak_blocking_split_pipeline_inflight_smoke_20260524T214145Z.log`. |

Long-history soak command:

```sh
rm -rf /tmp/ferricstore_flow_long_soak /tmp/ferricstore_flow_long_soak.log
MIX_ENV=bench \
  DATA_DIR=/tmp/ferricstore_flow_long_soak KEEP_DATA_DIR=true DURATION_SECONDS=3600 \
  TARGET_OPS_PER_SEC=50000 PAYLOAD_BYTES=5000 BLOB_SIDE_CHANNEL_THRESHOLD_BYTES=4096 \
  NORMAL_STEPS=50 LONG_FLOWS=1 LONG_STEPS=10000 SAMPLE_INTERVAL_SECONDS=60 \
  MIN_FREE_DISK_MB=300000 MAX_TOTAL_MEM_MB=96000 SHARDS=16 WORKERS=32 PRODUCERS=8 \
  PARTITIONS=4096 CREATE_MODE=pipeline CREATE_BATCH_SIZE=500 CREATE_INFLIGHT=4 \
  CLAIM_BATCH_SIZE=1000 CLAIM_PARTITION_BATCH_SIZE=64 APPLY_INFLIGHT=16 \
  mix run --no-start bench/flow_state_lmdb_soak.exs 2>&1 | tee /tmp/ferricstore_flow_long_soak.log
```

Long-history soak interpretation:

- The current design does not sustain the requested `50K/sec` state-changing
  workload with `5KB` payloads and deep history on this local machine.
- LMDB projection lag stayed below one second on the observed run, so the main
  pressure is hot memory/keydir/history volume plus WARAFT commit stalls, not
  LMDB catch-up alone.
- If `50K/sec` sustained deep-history workloads are required, the next design
  target is making `history_hot_max_events` a real hot-memory bound: old history
  should stay durable in segments/LMDB cold projection without retaining every
  history state in hot ETS/keydir.
- WARAFT commit timeout telemetry was added after this run; rerun the soak with
  the new `waraft_commit_timeouts` and `waraft_commit_timeout_max_us` columns
  before optimizing the commit path further.

Tests for Flow-3/Flow-4:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:23 \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:31 \
  apps/ferricstore/test/ferricstore/commands/flow_test.exs:51 \
  apps/ferricstore/test/ferricstore/commands/flow_test.exs:833 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1022
```

Result: `5 tests, 0 failures`.

Tests for Flow-5:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:37 \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:327 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1439 \
  apps/ferricstore/test/ferricstore/flow_test.exs:3935 \
  apps/ferricstore/test/ferricstore/commands/flow_test.exs:51
```

Result: `5 tests, 0 failures`.

Tests for Flow-6:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:44 \
  apps/ferricstore/test/ferricstore/flow_test.exs:3935 \
  apps/ferricstore/test/ferricstore/flow_test.exs:3985 \
  apps/ferricstore/test/ferricstore/flow_test.exs:4011
```

Result: targeted claim aggregation tests passed.

Tests for Flow-7:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:51 \
  apps/ferricstore/test/ferricstore/flow_test.exs:774 \
  apps/ferricstore/test/ferricstore/flow_test.exs:804 \
  apps/ferricstore/test/ferricstore/flow_test.exs:864
```

Result: `4 tests, 0 failures`.

Tests for Flow-10:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:72 \
  apps/ferricstore/test/ferricstore/flow_test.exs:3330 \
  apps/ferricstore/test/ferricstore/flow_test.exs:6772
```

Result: `3 tests, 0 failures`.

Tests for Flow-11:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:82
```

Result: targeted Flow history projection contract passed.

Tests for Flow-12:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:91 \
  apps/ferricstore/test/ferricstore/flow_test.exs:5595 \
  apps/ferricstore/test/ferricstore/flow_test.exs:5649 \
  apps/ferricstore/test/ferricstore/flow_test.exs:5700
```

Result: `4 tests, 0 failures`.

Tests for Flow-13:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:99 \
  apps/ferricstore/test/ferricstore/flow_test.exs:3935 \
  apps/ferricstore/test/ferricstore/flow_test.exs:3985 \
  apps/ferricstore/test/ferricstore/flow_test.exs:4011
```

Result: `4 tests, 0 failures`.

Tests for Flow-14:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:106 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1439 \
  apps/ferricstore/test/ferricstore/commands/flow_test.exs:51
```

Result: `3 tests, 0 failures`.

Tests for Flow-15:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:113 \
  apps/ferricstore/test/ferricstore/flow_test.exs:5650 \
  apps/ferricstore/test/ferricstore/flow_test.exs:5832 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1439
```

Result: `4 tests, 0 failures`.

Tests for Flow-16:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:121 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1439 \
  apps/ferricstore/test/ferricstore/flow_test.exs:5650 \
  apps/ferricstore/test/ferricstore/flow_test.exs:5832
```

Result: `4 tests, 0 failures`.

Tests for Flow-8:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_test.exs:903 \
  apps/ferricstore/test/ferricstore/flow_test.exs:941 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1022 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1071 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1092
```

Result: targeted claim pipeline/coalescing tests passed.

Tests for Flow-9:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:60 \
  apps/ferricstore/test/ferricstore/flow_test.exs:926 \
  apps/ferricstore/test/ferricstore/flow_test.exs:965 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1051
```

Result: `4 tests, 0 failures`.

Dedicated RESP Flow pipeline bench:

```sh
MIX_ENV=bench \
  FLOW_RESP_BACKLOG=10000 FLOW_RESP_ITER=30 FLOW_RESP_BATCH=100 \
  FLOW_RESP_SHARDS=16 FLOW_RESP_PARTITIONS=1024 \
  mix run --no-start bench/flow_resp_pipeline_bench.exs
```

| Iteration | Command | pipelines/s | commands/s | avg us | p50 us | p95 us | p99 us |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Before Flow-8 | `FLOW.CREATE x100` | 124 | 12,443 | 8,036 | 7,295 | 12,769 | 20,170 |
| Before Flow-8 | `FLOW.CLAIM_DUE x100 limit=1` | 1 | 52 | 1,912,080 | 1,878,793 | 2,174,745 | 2,555,332 |
| Before Flow-8 | `FLOW.TRANSITION x100` | 146 | 14,647 | 6,827 | 6,996 | 7,392 | 7,421 |
| After Flow-8 | `FLOW.CREATE x100` | 128 | 12,795 | 7,816 | 7,490 | 8,563 | 17,845 |
| After Flow-8 | `FLOW.CLAIM_DUE x100 limit=1` | 125 | 12,471 | 8,019 | 7,302 | 9,290 | 18,263 |
| After Flow-8 | `FLOW.TRANSITION x100` | 163 | 16,339 | 6,120 | 6,096 | 7,154 | 7,322 |
| After Flow-14 | `FLOW.CREATE x100` | 124 | 12,387 | 8,073 | 7,399 | 12,166 | 23,822 |
| After Flow-14 | `FLOW.CLAIM_DUE x100 limit=1` | 120 | 12,011 | 8,326 | 7,923 | 11,003 | 19,081 |
| After Flow-14 | `FLOW.TRANSITION x100` | 156 | 15,611 | 6,406 | 6,164 | 7,424 | 9,797 |
| After Pipeline-1 | `FLOW.CREATE x100` | 123 | 12,284 | 8,141 | 7,677 | 13,448 | 17,742 |
| After Pipeline-1 | `FLOW.CLAIM_DUE x100 limit=1` | 109 | 10,850 | 9,216 | 8,318 | 14,757 | 17,472 |
| After Pipeline-1 | `FLOW.TRANSITION x100` | 160 | 15,959 | 6,266 | 6,121 | 7,318 | 7,362 |
| After Flow-17 | `FLOW.CREATE x100` | 138 | 13,774 | 7,260 | 6,831 | 10,926 | 20,046 |
| After Flow-17 | `FLOW.CLAIM_DUE x100 limit=1` | 125 | 12,508 | 7,995 | 7,731 | 9,062 | 15,717 |
| After Flow-17 | `FLOW.TRANSITION x100` | 166 | 16,616 | 6,018 | 5,946 | 6,314 | 7,091 |

## Streams

| Iteration | Change | Result | Notes |
| --- | --- | --- | --- |
| Streams-1 | Route `XREADGROUP ... BLOCK` through the same TCP waiter path as `XREAD`, re-dispatching the original AST on wake | Correctness fix; no throughput benchmark | TDD reproduced the bug as a connection crash while encoding internal `{:block, ...}`. The fix preserves consumer-group pending semantics by retrying the original `XREADGROUP` AST after an `XADD` wake. |
| Streams-2 | `XDEL` and `XTRIM` delete stream entries with compound batch delete | Correctness tests passed; no standalone throughput benchmark yet | Removes one Raft/write round trip per deleted entry. Metadata and stream indexes are updated only after the batch delete succeeds. |
| Streams-3 | `XREADGROUP` pending replay reads entries with one compound batch get | Correctness tests passed; no standalone throughput benchmark yet | Keeps Redis pending order and missing-entry filtering, but removes one read call per pending entry replayed to a consumer. |
| Streams-4 | `XREAD` builds ordered results with one reducer instead of map + error scan + nil reject | Targeted tests passed; no standalone throughput benchmark yet | Preserves stream order and first-error behavior while avoiding two extra passes over multi-stream reads. |
| Streams-5 | `XREADGROUP` builds ordered results with one reducer and stops on the first stream error | Targeted tests passed; no standalone throughput benchmark yet | Avoids the map/find/reject result passes and reduces side effects after an early `NOGROUP`/parse/persist error. |
| Streams-6 | `XREADGROUP` pending replay filters and parses pending IDs in one pass before sorting | Targeted tests passed; no standalone throughput benchmark yet | Removes the double filter and repeated `parse_id!` calls from pending replay while preserving consumer ownership, lower-bound filtering, order, and `COUNT`. |
| Streams-7 | `XDEL`/`XTRIM` delete stream index rows from known IDs after the batch delete succeeds | Targeted tests passed; no standalone throughput benchmark yet | Keeps the storage delete atomic boundary the same, but avoids reparsing compound keys and prefix-stripping during index cleanup. |
| Streams-8 | Indexed stream reads decode batch-get results with recursive ordered helpers instead of `Enum.zip` + `Enum.flat_map` | Targeted tests passed; no standalone throughput benchmark yet | Applies to indexed `XRANGE`/`XREVRANGE` and the `XREAD`/`XREADGROUP` paths that use them. Missing/corrupt rows are still skipped in order. |
| Streams-9 | `XDEL` filters batch-get hits with an ordered helper instead of zipping values to IDs | Targeted tests passed; no standalone throughput benchmark yet | Preserves duplicate-ID collapse and only deletes existing entries, but removes one zip allocation from multi-ID deletes. |

Tests for Streams-1:

```sh
MIX_ENV=test mix test \
  apps/ferricstore_server/test/ferricstore_server/integration/stream_tcp_test.exs:506 \
  apps/ferricstore_server/test/ferricstore_server/commands/blocking_tracking_test.exs:86 \
  apps/ferricstore_server/test/ferricstore_server/commands/blocking_tracking_test.exs:124 \
  apps/ferricstore/test/ferricstore/commands/stream_xreadgroup_block_test.exs:73
```

Result: targeted stream blocking tests passed.

Tests for Streams-2:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:768 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:784 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:805 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:818 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:729 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:739 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:747
```

Result: targeted stream delete/trim tests passed.

Tests for Streams-3:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:1144 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:1098 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:1125 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:1165
```

Result: targeted XREADGROUP tests passed.

Tests for Streams-4:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:580 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:620 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:632
```

Result: `3 tests, 0 failures`.

Tests for Streams-5:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:1041 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:1032 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:1084 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:1146
```

Result: `4 tests, 0 failures`.

Tests for Streams-6:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:1057 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:1146 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:1084
```

Result: `3 tests, 0 failures`.

Tests for Streams-7:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:781 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:833 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:685 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:737
```

Result: `4 tests, 0 failures`.

Tests for Streams-8:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:574 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:525 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:544 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:602
```

Result: `4 tests, 0 failures`.

Tests for Streams-9:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:781 \
  apps/ferricstore/test/ferricstore/commands/stream_test.exs:833
```

Result: `2 tests, 0 failures`.

Short local Stream bench after Streams-9:

```sh
MIX_ENV=bench BENCH_WARMUP=0 BENCH_TIME=1 BENCH_PARALLEL=1 \
  STREAM_BENCH_SIZES=1000 STREAM_BENCH_BATCH_DEPTH=100 \
  mix run --no-start bench/stream_bench.exs
```

Result: `xread first10` `169,701 ips`, `xrange first10` `167,504 ips`,
`xrevrange last10` `164,702 ips`, `xadd explicit` `154.78 ips`
(`6.46 ms` average, single-shard Raft fsync path). Batched read pipelines were
about `155-158 pipelines/sec` for 100 commands.

## RESP Pipeline

| Iteration | Change | Result | Notes |
| --- | --- | --- | --- |
| Pipeline-1 | Pure pipeline segment dispatcher prepends encoded entries with one reducer instead of `Enum.reverse(entries) ++ acc` | Flow pipeline rerun was mixed: transition p99 improved, create/claim stayed in local noise | Applies to Flow write/read/claim segments and phase-1 write segments. It preserves response order because the dispatcher accumulator is reversed until the final send. |

Tests for Pipeline-1:

```sh
MIX_ENV=test mix test \
  apps/ferricstore_server/test/ferricstore_server/connection_test.exs:188 \
  apps/ferricstore_server/test/ferricstore_server/connection_test.exs:217 \
  apps/ferricstore_server/test/ferricstore_server/connection_test.exs:534 \
  apps/ferricstore_server/test/ferricstore_server/connection_test.exs:285
```

Result: `4 tests, 0 failures`.

## Pub/Sub

| Iteration | Change | Result | Notes |
| --- | --- | --- | --- |
| PubSub-1 | Pattern `PUBLISH` uses `:ets.foldl/3` instead of `:ets.tab2list/1` | Correctness tests passed; no standalone throughput benchmark yet | Keeps Redis glob semantics and exact subscriber counts, but avoids copying the entire pattern table before filtering. |
| PubSub-2 | `PSUBSCRIBE` relies on ETS `:bag` idempotence and `PUBSUB CHANNELS` walks unique ETS keys with `:ets.first/:ets.next` | `22 tests, 0 failures` | Removes the pre-insert pattern scan and avoids copying every subscriber row for channel introspection. |
| PubSub-3 | Exact-channel `PUBLISH` skips the pattern-table fold when there are zero pattern subscribers | Targeted tests passed; no standalone throughput benchmark yet | Common exact-only publish now pays one ETS size check instead of a full pattern-table scan. Pattern subscriptions still use the same fold and glob matching semantics. |
| PubSub-4 | Bulk subscribe/unsubscribe APIs let TCP `SUBSCRIBE`/`PSUBSCRIBE` update ETS in batches and monitor once per command | Targeted tests passed; no standalone throughput benchmark yet | Preserves one push reply per command argument, but removes repeated PubSub GenServer monitor/demonitor calls during multi-channel subscribe/unsubscribe and cleanup. |
| PubSub-5 | `PUBSUB NUMSUB` builds the alternating reply with a reverse accumulator | Targeted tests passed; no standalone throughput benchmark yet | Avoids `Enum.flat_map/2` list churn for large channel introspection calls while preserving Redis reply order. |
| PubSub-6 | Exact-channel `PUBLISH` reads a derived `{channel, [pid]}` cache | Targeted tests passed; local bench modest exact fanout improvement | Keeps the ETS bag as source of truth for `PUBSUB CHANNELS`/`NUMSUB`, but publish avoids copying `{channel, pid}` tuples and reducing over them on every exact publish. Subscribe/unsubscribe/dead-pid cleanup rebuild only affected channel cache entries. |
| PubSub-7 | `PSUBSCRIBE` preclassifies simple patterns at subscribe time | Targeted tests passed; local pattern publish bench improved strongly | Common `prefix*`, `*suffix`, `*`, and exact literal patterns avoid the full glob matcher at publish time. Complex Redis glob patterns still fall back to `Ferricstore.GlobMatcher`. |

Tests for PubSub-1:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/pubsub_commands_test.exs:29 \
  apps/ferricstore/test/ferricstore/commands/pubsub_commands_test.exs:63 \
  apps/ferricstore/test/ferricstore/commands/pubsub_commands_test.exs:154
```

Result: `3 tests, 0 failures`.

Tests for PubSub-2:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/pubsub_commands_test.exs
```

Result: `22 tests, 0 failures`.

Tests for PubSub-3:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/pubsub_commands_test.exs:71 \
  apps/ferricstore/test/ferricstore/commands/pubsub_commands_test.exs:63 \
  apps/ferricstore/test/ferricstore/commands/pubsub_commands_test.exs:181
```

Result: `3 tests, 0 failures`.

Tests for PubSub-4:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/pubsub_commands_test.exs:95 \
  apps/ferricstore_server/test/ferricstore_server/pubsub_test.exs:107 \
  apps/ferricstore_server/test/ferricstore_server/pubsub_test.exs:124 \
  apps/ferricstore_server/test/ferricstore_server/pubsub_test.exs:166
```

Result: targeted Pub/Sub bulk API tests passed.

Tests for PubSub-5:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/pubsub_commands_test.exs:192 \
  apps/ferricstore/test/ferricstore/commands/pubsub_commands_test.exs:181
```

Result: `2 tests, 0 failures`.

Tests for PubSub-6/PubSub-7:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/pubsub_commands_test.exs \
  apps/ferricstore_server/test/ferricstore_server/pubsub_test.exs \
  apps/ferricstore_server/test/ferricstore_server/pubsub_bug_hunt_test.exs \
  apps/ferricstore_server/test/ferricstore_server/spec/edge_cases_test.exs
```

Result: `118 tests, 0 failures`.

Local Pub/Sub bench:

```sh
MIX_ENV=bench mix run --no-start bench/pubsub_bench.exs
```

Result:

| Kind | Scenario | Publishes | Subscribers/Patterns | Publishes/sec | Deliveries/sec |
| --- | --- | --- | --- | --- | --- |
| core | exact | `100,000` | `0` subscribers | `20,824,656` | `20,824,656` |
| core | exact | `100,000` | `1` subscriber | `3,834,356` | `3,834,356` |
| core | exact | `100,000` | `10` subscribers | `258,208` | `2,582,084` |
| core | exact | `100,000` | `100` subscribers | `12,947` | `1,294,700` |
| core | pattern match | `100,000` | `1` pattern | `1,002,918` | `1,002,918` |
| core | pattern match | `100,000` | `10` patterns | `86,320` | `863,203` |
| core | pattern match | `100,000` | `100` patterns | `8,791` | `879,073` |
| tcp | exact RESP publish | `50,000` | `0` subscribers | `515,645` | `515,645` |
| tcp | exact RESP publish | `50,000` | `1` subscriber | `501,349` | `501,349` |
| tcp | exact RESP publish | `50,000` | `10` subscribers | `49,091` | `490,908` |

Notes: core rows measure PubSub ETS lookup/matching and BEAM message send only.
TCP rows include RESP parsing, command dispatch, publish replies, subscriber
connection push encoding, and socket drainers. Pattern publish scales with the
number of pattern subscriptions because Redis glob matching must inspect each
pattern.

After PubSub-6/PubSub-7 rerun:

| Kind | Scenario | Publishes | Subscribers/Patterns | Publishes/sec | Deliveries/sec | Change vs prior |
| --- | --- | --- | --- | --- | --- | --- |
| core | exact | `100,000` | `10` subscribers | `274,742` | `2,747,419` | `+6.4%` publishes/sec |
| core | exact | `100,000` | `100` subscribers | `13,856` | `1,385,594` | `+7.0%` publishes/sec |
| core | pattern match | `100,000` | `1` pattern | `1,627,207` | `1,627,207` | `+62.2%` publishes/sec |
| core | pattern match | `100,000` | `10` patterns | `164,496` | `1,644,961` | `+90.6%` publishes/sec |
| core | pattern match | `100,000` | `100` patterns | `12,116` | `1,211,554` | `+37.8%` publishes/sec |
| tcp | exact RESP publish | `50,000` | `1` subscriber | `516,897` | `516,897` | `+3.1%` publishes/sec |
| tcp | exact RESP publish | `50,000` | `10` subscribers | `49,866` | `498,658` | `+1.6%` publishes/sec |

## Sets

| Iteration | Change | Result | Notes |
| --- | --- | --- | --- |
| Sets-1 | `SINTER` and `SINTERCARD` batch membership probes per remaining set | `170 tests, 0 failures` | Still scans only the smallest set, but replaces `candidate_count * remaining_set_count` single `compound_get` calls with one `compound_batch_get` per remaining set. `SINTERCARD LIMIT` processes candidates in bounded chunks so it can still stop early. |
| Sets-2 | `SADD`/`SREM` build write/delete entries directly from batch read results | Targeted tests passed; no standalone throughput benchmark yet | Preserves duplicate-member collapse, type marker rollback, and empty-set cleanup. Removes `Enum.zip`/`Enum.flat_map` plus the extra key-to-entry pass on multi-member add/remove. |

Tests for Sets-1:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/set_test.exs \
  apps/ferricstore/test/ferricstore/commands/set_store_commands_test.exs
```

Result: `170 tests, 0 failures`.

Tests for Sets-2:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/set_test.exs:73 \
  apps/ferricstore/test/ferricstore/commands/set_test.exs:217 \
  apps/ferricstore/test/ferricstore/commands/set_test.exs:25 \
  apps/ferricstore/test/ferricstore/commands/set_test.exs:164 \
  apps/ferricstore/test/ferricstore/commands/set_test.exs:204
```

Result: `4 tests, 0 failures`.

## Hashes

| Iteration | Change | Result | Notes |
| --- | --- | --- | --- |
| Hashes-1 | `HSET`/TTL variants build write entries and added-field count in one traversal after the batch existence read | Targeted tests passed; no standalone throughput benchmark yet | Preserves duplicate-field collapse, one `compound_batch_get`, and one `compound_batch_put`, but removes the separate nil-count pass plus zip/map entry construction. |
| Hashes-2 | Multi-field hash metadata helper builds the field-to-meta map in one traversal | Targeted tests passed; no standalone throughput benchmark yet | Applies to `HEXPIRE`/`HPEXPIRE`, `HTTL`/`HPTTL`, `HEXPIRETIME`/`HPEXPIRETIME`, `HPERSIST`, and `HGETEX` shared metadata reads. Missing/truncated batch results still fail at the existing `Map.fetch!` use sites instead of inventing defaults. |
| Hashes-3 | `HDEL` derives deleted field entries directly from batch metadata | Targeted tests passed; no standalone throughput benchmark yet | Keeps duplicate-field collapse, one metadata read batch, one delete batch, and empty-hash cleanup. Removes the temporary key-to-meta map and second scan over delete keys. |
| Hashes-4 | `HGETDEL` carries response values and rollback/delete entries in one reducer | Targeted tests passed; no standalone throughput benchmark yet | Duplicate fields still return the value once and nil afterward. The delete batch still carries original value+TTL for cleanup rollback, but avoids building a deleted-entry map and mapping it back to a list. |
| Hashes-5 | Shared field-value response flattener for `HGETALL`, `HSCAN`, and `HRANDFIELD WITHVALUES` | Targeted tests passed; no standalone throughput benchmark yet | Removes repeated `Enum.flat_map` closures while preserving scan filtering, cursor pagination, random selection, and response order. |
| Hashes-6 | `HPERSIST` and `HGETEX` metadata rewrite entries use one-pass builders | Targeted tests passed; no standalone throughput benchmark yet | Keeps the same batch metadata read and batch put, skips missing/persistent fields exactly as before, and avoids `Enum.zip`/`Enum.flat_map` in TTL rewrite assembly. |

Tests for Hashes-1/Hashes-2:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:122 \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:112 \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:132 \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:521 \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:286 \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:238 \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:335 \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:348 \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:438 \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:1310 \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:1526 \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:247 \
  apps/ferricstore/test/ferricstore/commands/hash_test.exs:395
```

Latest targeted subsets passed; last metadata-write run: `4 tests, 0 failures`.

## Sorted Sets

| Iteration | Change | Result | Notes |
| --- | --- | --- | --- |
| ZSets-1 | `ZADD` builds batch write entries with a recursive ordered helper instead of `Enum.zip` + `Enum.flat_map` | Targeted tests passed; no standalone throughput benchmark yet | Applies to both string-parser and AST ZADD paths. Batched reads, Redis option handling, duplicate-member final-write semantics, and rollback behavior stay unchanged. |
| ZSets-2 | `ZADD` builds the current-score map directly from batch-get results instead of `Enum.zip` + `Map.new` | Targeted tests passed; no standalone throughput benchmark yet | Keeps one `compound_batch_get`; missing/truncated batch results still behave as absent members. |
| ZSets-3 | Shared WITHSCORES flatteners for rank/range/scan/pop/random responses | Targeted tests passed; no standalone throughput benchmark yet | Removes repeated `Enum.flat_map` closures in response assembly while preserving score formatting differences between numeric and stored string scores. |

Tests for ZSets-1 through ZSets-3:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/sorted_set_test.exs:168 \
  apps/ferricstore/test/ferricstore/commands/sorted_set_test.exs:159 \
  apps/ferricstore/test/ferricstore/commands/sorted_set_test.exs:87 \
  apps/ferricstore/test/ferricstore/commands/sorted_set_test.exs:297 \
  apps/ferricstore/test/ferricstore/commands/sorted_set_test.exs:682 \
  apps/ferricstore/test/ferricstore/commands/sorted_set_test.exs:756 \
  apps/ferricstore/test/ferricstore/commands/sorted_set_test.exs:934 \
  apps/ferricstore/test/ferricstore/commands/sorted_set_test.exs:1087
```

Result: latest ZSets-3 targeted run: `6 tests, 0 failures`.

## Probabilistic Commands

| Iteration | Change | Result | Notes |
| --- | --- | --- | --- |
| TopK-1 | `TOPK.LIST WITHCOUNT` flattens item/count pairs with a recursive helper | Targeted tests passed; no standalone throughput benchmark yet | Keeps the same two async NIF reads and preserves `Enum.zip` truncation semantics on mismatched item/count result lengths, but removes the zip plus flat-map allocation in response assembly. |

Tests for TopK-1:

```sh
MIX_ENV=test mix test \
  apps/ferricstore/test/ferricstore/commands/topk_test.exs:394 \
  apps/ferricstore/test/ferricstore/commands/topk_test.exs:384 \
  apps/ferricstore/test/ferricstore/commands/topk_test.exs:503
```

Result: `3 tests, 0 failures`.

## TCP Server Pipeline

Goal: reduce server-side TCP/pipeline overhead one change at a time, measuring
throughput and scheduler utilization before keeping runtime changes. Adaptive
socket pressure is intentionally not part of this round.

Benchmark command:

```sh
MIX_ENV=test mix test \
  apps/ferricstore_server/test/ferricstore_server/bench/tcp_pipeline_profile_test.exs \
  --include bench
```

Baseline before the parser-plan experiment:

| Scenario | Throughput | Scheduler utilization |
| --- | ---: | ---: |
| 5 connections x 10 pipeline | `790,602 reads/sec` | `9.4%` |
| 5 connections x 50 pipeline | `1,987,712 reads/sec` | `11.9%` |
| 10 connections x 50 pipeline | `202,662 reads/sec` | `26.1%` |
| 25 connections x 50 pipeline | `126,937 reads/sec` | `49.4%` |

Kept checks:

- Generic pure fallback stays in the connection process; no per-command `Task`
  spawn in the pipeline fallback path.
- Generic pure fallback coalesces a pipeline into one socket send.
- Pipeline segment response prepend avoids `Enum.reverse(entries) ++ acc`.
- Connection module docs now describe the current segmented pipeline behavior
  instead of the old task-based model.

Targeted guard tests:

```sh
MIX_ENV=test mix test \
  apps/ferricstore_server/test/ferricstore_server/connection_test.exs:189 \
  apps/ferricstore_server/test/ferricstore_server/connection_test.exs:198 \
  apps/ferricstore_server/test/ferricstore_server/connection_test.exs:209
```

Result: `3 tests, 0 failures`.

Rejected experiment: RESP parser returned a six-tuple "pipeline plan" shape with
command class, primary key, and group. Correctness tests passed, but the extra
NIF term allocation did not pay for itself because the connection layer still
needed most of the Elixir dispatch logic.

Parser-plan trial result:

| Scenario | Throughput | Scheduler utilization | Decision |
| --- | ---: | ---: | --- |
| 5 connections x 10 pipeline | `781,042 reads/sec` | `9.3%` | Flat/slightly worse |
| 5 connections x 50 pipeline | `1,825,325 reads/sec` | `11.9%` | Worse |
| 10 connections x 50 pipeline | `208,175 reads/sec` | `25.3%` | Slightly better |
| 25 connections x 50 pipeline | `110,250 reads/sec` | `49.7%` | Worse |

Decision: dropped. Do not reintroduce a wider parser return tuple unless it lets
the TCP loop skip a meaningful amount of Elixir classification, grouping, or
response assembly.

Post-revert control run on the dirty WARaft branch:

| Scenario | Throughput | Scheduler utilization |
| --- | ---: | ---: |
| 5 connections x 10 pipeline | `717,220 reads/sec` | `9.6%` |
| 5 connections x 50 pipeline | `1,623,912 reads/sec` | `12.2%` |
| 10 connections x 50 pipeline | `170,700 reads/sec` | `26.4%` |
| 25 connections x 50 pipeline | `108,162 reads/sec` | `49.7%` |

This control is lower than the earlier baseline even after the parser-plan
runtime code was reverted, so treat it as branch/noise-sensitive rather than a
parser-plan win/loss signal by itself. The decision to drop parser-plan is based
on the direct before/after experiment.

## Flow Claim Due Polling

Goal: recover the state-machine soak latency/throughput after removing the old
owner-wakeup path, without reintroducing local wake queues or moving LMDB back
into the hot path.

Profile finding:

- `FLOW.CLAIM_DUE` polling spent visible reductions in
  `Ferricstore.Flow.validate_claim_due_keys/4`.
- The hot stack built due-index keys for every selected partition/state just to
  check max key size.
- Fixed by replacing generated key allocation with length math and one
  `Router.max_key_size/0` read per claim.

Targeted tests:

```sh
PYTHONPATH=src pytest -q \
  tests/test_flow_worker_scheduler.py \
  tests/test_workflow.py \
  tests/test_async_sdk.py \
  tests/test_async_state_machine_benchmark.py

mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:238

mix test \
  apps/ferricstore/test/ferricstore/flow_test.exs:3223
```

Result:

- Python SDK targeted tests: `100 passed`.
- Flow write contract and malformed-input tests: passed.

Apples-to-apples polling benchmark, `payload_bytes=1000`, `SHARDS=16`,
`WORKERS=32`, `PRODUCERS=8`, `CREATE_MODE=many`, `CLAIM_BATCH_SIZE=1000`,
`CLAIM_PARTITION_BATCH_SIZE=32`, `WORKER_MODE=polling`,
`NORMAL_CLAIM_STATES_MODE=cursor`.

| Run | At ~64s | At ~185s | Notes |
| --- | ---: | ---: | --- |
| Before | `17.8K flow_ops/s` | `26.8K flow_ops/s` | `flow_state_lmdb_soak_apples_polling_fixed_20260525T042350Z.log` |
| After claim validation fast path | `19.6K flow_ops/s` | `29.3K flow_ops/s` | `flow_state_lmdb_soak_claim_validate_fast_20260525T044458Z.log` |

Latency at ~185s improved from:

| Command | Before avg | After avg |
| --- | ---: | ---: |
| create | `88.0 ms` | `76.8 ms` |
| claim_due | `51.6 ms` | `44.9 ms` |
| transition | `75.3 ms` | `65.2 ms` |
| complete | `62.9 ms` | `53.6 ms` |

Rejected probes:

- `WORKER_MODE=blocking`: current server long-poll path is not production-ready
  for this soak. It dropped to about `943 flow_ops/s`, produced WARaft commit
  timeouts, and inflated ETS memory to `3.3GB`.
- `NORMAL_CLAIM_STATES_MODE=all`: lowered per-command latency but did not make
  workflow progress fast enough; throughput fell to about `6.9K flow_ops/s`.

Remaining gap:

- The old `flow_state_lmdb_soak_final_20260524T152410Z.log` run claimed much
  larger batches (`~560` jobs/claim at ~64s) than the current polling path
  (`~51` jobs/claim). That came from the removed wake/credit behavior, not from
  the key-size validation hotspot.

## Flow Claim Due Long Polling, Compact Waiters, And Ready Credits

Design decision:

- Keep worker scheduling on `FLOW.CLAIM_DUE ... BLOCK`, not Pub/Sub.
- Store wake credits inside the server-side waiter registry as ready hints.
- Do not use cursor-state blocking for multi-state workers; a worker can block
  on the wrong state and stop making progress.
- Use `RETURN JOBS_COMPACT_STATE` for broad blocking claims so the SDK can claim
  any ready state and still dispatch each job to the correct handler.

Correctness fixes:

- Router precheck now uses the requested state filter for `STATE ANY` and
  multi-state claims. The route key can still use a cheap stable state for shard
  selection, but an empty `"queued"` index cannot suppress ready work in another
  state.
- `ClaimWaiters.notify_ready_many/2` coalesces repeated ready hints by bucket,
  records each waiter's requested `LIMIT`, and wakes only enough waiters to cover
  the ready count. This keeps the old credit idea inside the server waiter
  registry without Pub/Sub in the worker hot path.
- Python SDK `claim_jobs(..., include_state=True)` requests
  `JOBS_COMPACT_STATE`, and workflow workers use it only when all selected states
  are compact-claim safe.

Targeted tests:

```sh
mix test \
  apps/ferricstore/test/ferricstore/flow/claim_waiters_test.exs \
  apps/ferricstore/test/ferricstore/flow_test.exs:1154 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1181 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1326 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1430 \
  apps/ferricstore/test/ferricstore/commands/flow_test.exs:1390 \
  apps/ferricstore_server/test/ferricstore_server/connection_test.exs:657

PYTHONPATH=src pytest -q \
  tests/test_client.py \
  tests/test_async_client.py \
  tests/test_workflow.py \
  tests/test_async_sdk.py \
  tests/test_async_state_machine_benchmark.py
```

Results:

- Elixir targeted Flow/RESP/waiter tests: `11` tests passed.
- Python SDK/client/workflow targeted suites: `175 passed`.

Soak benchmark, `payload_bytes=1000`, `SHARDS=16`, `WORKERS=32`,
`PRODUCERS=8`, `CREATE_MODE=many`, `CLAIM_BATCH_SIZE=1000`,
`CLAIM_PARTITION_BATCH_SIZE=32`, `WORKER_MODE=blocking`,
`NORMAL_CLAIM_STATES_MODE=any`, `LONG_CLAIM_STATES_MODE=any`:

| Run | Flow ops/sec | Write ops/sec | Claim avg | Transition avg | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| Bad blocking cursor | `943/s` | n/a | very high | n/a | `flow_state_lmdb_soak_blocking_after_sdk_pool_20260525T044958Z.log`; wrong blocking shape, waiter ETS blow-up |
| Compact waiters, cursor | `30.1K/s` | n/a | p99 `<=100ms` at first sample | p99 `<=250ms` | `flow_state_lmdb_soak_blocking_compact_waiters_20260525T0900Z.log`; row blow-up fixed but cursor-state still weaker |
| Exact wait credits, cursor | `918/s` | n/a | n/a | n/a | `flow_state_lmdb_soak_blocking_waiter_credits_20260525T0906Z.log`; workers blocked on wrong state |
| Explicit all states | `5.7K/s` | n/a | n/a | n/a | `flow_state_lmdb_soak_blocking_all_states_credits_20260525T0910Z.log`; correct but too much state scan/return overhead |
| Any-state compact return | `45.6K/s` | `90.4K/s` | `53.2ms` | `96.5ms` | `flow_state_lmdb_soak_blocking_any_state_20260525T063330Z.log`; no WAraft timeouts, max memory `1.4GB` |
| Limit-aware waiter credits | `45.0K/s` | `89.1K/s` | `53.3ms` | `97.8ms` | `flow_state_lmdb_soak_blocking_any_state_limit_credits_20260525T064721Z.log`; no WAraft timeouts, same latency class, fewer unnecessary worker wakes |
| Real dashboard soak, 5 min | `47.6K/s` | `94.3K/s` | `67.2ms` | `127.3ms` | `1KB` payloads, `50` normal states, one `10K`-step failure flow, `SHARDS=16`, `WORKERS=32`, `PRODUCERS=8`, `CREATE_MODE=many`, `NORMAL_CLAIM_STATES_MODE=any`, `LONG_CLAIM_STATES_MODE=any`. Completed duration with no WARaft timeouts/errors. Max LMDB pending `199,804`, oldest lag `1.15s`, disk `50.9GB`, LMDB `20.0GB`, WARaft `23.1GB`, BEAM memory `2.1GB`. Live dashboard loaded but `FLOW.HISTORY COUNT 100` timed out for one completed flow under load. Restarting against the 50GB data dir did not reach the dashboard within `3.5min`; process sampled in the Bitcask NIF during recovery. Log: `/tmp/ferricstore_flow_real_soak.log`. |

Conclusion:

- Long polling is worth keeping. It is better than Pub/Sub wake credits for
  worker scheduling because the blocking claim itself is the durable/atomic
  claim point.
- Pub/Sub should not be in the worker hot path. It can still exist later for
  UI/event notifications, but not for Flow work delivery.
- The next bottleneck is still WARaft/storage apply batching and transition
  latency, not empty polling.

## Flow Soak Hot Path Cleanup: Path Helpers, Small Values, Projection Writers

Workload:

- `DURATION_SECONDS=60`
- `TARGET_OPS_PER_SEC=50000`
- `PAYLOAD_BYTES=1000`
- `NORMAL_STEPS=50`
- `LONG_FLOWS=1`
- `LONG_STEPS=10000`
- `SHARDS=16`
- `WORKERS=32`
- `PRODUCERS=8`
- `CREATE_MODE=many`
- `FLOW_LATENCY_SAMPLE_RATE=10`
- process profile enabled

Changes tested:

- `DataDir.shard_data_path/2` and `blob_shard_path/2` now avoid generic
  `Path.join/1` on hot shard paths while preserving canonical slash behavior.
- Flow apply skips `BlobValue.maybe_externalize/4` for normal small binaries,
  but still routes blob-ref-shaped byte sizes through the blob path so user data
  cannot be confused with internal refs.
- Apply-projection `write_projection_batches/2` now reuses the direct nosync
  segment writer instead of closing/reopening the segment file for every spill.

Targeted tests:

```sh
mix compile --warnings-as-errors --no-validate-compile-env

mix test \
  apps/ferricstore/test/ferricstore/data_dir_test.exs \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs \
  apps/ferricstore/test/ferricstore/raft/waraft_segment_log_test.exs \
  apps/ferricstore_server/test/ferricstore_server/connection_test.exs:710 \
  apps/ferricstore_server/test/ferricstore_server/connection_test.exs:760

mix test \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:2274 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:2292 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:7422 \
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:7577
```

Results:

- Compile: passed.
- DataDir/Flow contract/WARaft segment log/RESP blocking locations: passed.
- WARaft apply-projection backend subset: `4` tests passed.

Benchmark comparison:

| Run | Flow ops/sec | Write ops/sec | Claim avg | Transition avg | Max memory | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Before these cleanups | `18.94K/s` | `37.00K/s` | `32.6ms` | `41.2ms` | `598.9MB` | `flow_state_lmdb_soak_profile_after_block_once_20260526T110108Z.log` |
| Path + small-value skip | `18.70K/s` | `36.52K/s` | `32.5ms` | `41.2ms` | `613.3MB` | `flow_state_lmdb_soak_profile_after_path_blob_skip_20260526T111110Z.log` |
| Projection nosync writer reuse | `18.68K/s` | `36.48K/s` | `32.9ms` | `42.3ms` | `591.9MB` | `flow_state_lmdb_soak_profile_after_nosync_writer_reuse_20260526T111904Z.log` |

Conclusion:

- The changes are correctness-safe and remove avoidable work, but the 60s soak
  does not prove a throughput or latency win. Keep them only because they are
  low-risk and make the profile cleaner.
- The old bad stack
  `filename.absname -> close_writer_for_path -> open_record_group_once_file_direct_nosync`
  disappeared from the main profile after writer reuse.
- Remaining sampled hotspots are:
  `flow_plan_claim_candidates_native/10`,
  `flow_read_claim_candidate_hot_values_loop/5`,
  `flow_many_same_state_machine_shard?/2`,
  `Ferricstore.Flow.encode_record/1`, and
  `WARaftSegmentReader.apply_projection_cache_entries/2`.
- Next real latency work should focus on claim planning and transition batch
  apply, not path construction or segment writer open/close.

## Flow Shard Marker And Zero-Hot History Fast Path

Workload: same 60s Flow state/LMDB soak as above:

- `PAYLOAD_BYTES=1000`
- `NORMAL_STEPS=50`
- `LONG_FLOWS=1`
- `LONG_STEPS=10000`
- `SHARDS=16`
- `WORKERS=32`
- `PRODUCERS=8`
- `CREATE_MODE=many`
- process stack profiling enabled

Changes tested:

- Router now stamps internally-built per-shard Flow batches with
  `:__flow_shard_index__`. State-machine apply validates that stamp and skips
  per-item `Router.shard_for/2` hashing when the batch already came from a
  trusted per-shard router grouping. Missing or mismatched stamps fall back to
  the old hash check.
- Flow history projector now treats the default `history_hot_max_events=0` as a
  direct eviction of the just-projected event after LMDB has the locator. This
  avoids native rank/count scans for the common "keep no hot history" policy.
  Terminal history still uses the rank path because it may need to evict older
  retained hot events.

Targeted tests:

```sh
mix test \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs \
  apps/ferricstore/test/ferricstore/flow/history_projector_test.exs --max-failures 3

mix test \
  apps/ferricstore/test/ferricstore/flow_test.exs:847 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1006 \
  apps/ferricstore/test/ferricstore/flow_test.exs:1849 \
  apps/ferricstore/test/ferricstore/flow_test.exs:3303 \
  apps/ferricstore/test/ferricstore/flow_test.exs:6171 \
  apps/ferricstore/test/ferricstore/flow_test.exs:6441 \
  apps/ferricstore/test/ferricstore/flow_test.exs:6497 \
  apps/ferricstore/test/ferricstore/flow_test.exs:6551 \
  apps/ferricstore/test/ferricstore/flow_test.exs:6645 \
  apps/ferricstore/test/ferricstore/flow_test.exs:6924 --max-failures 3
```

Results:

- Flow write contract: `38` tests passed.
- History projector: `22` tests passed.
- Selected functional Flow transition/create/retention tests: `10` tests
  passed.

Benchmark comparison:

| Run | Flow ops/sec | Write ops/sec | Claim avg | Transition avg | Max memory | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Projection writer reuse | `18.68K/s` | `36.48K/s` | `32.9ms` | `42.3ms` | `591.9MB` | `flow_state_lmdb_soak_profile_after_nosync_writer_reuse_20260526T111904Z.log` |
| Shard marker only | `18.80K/s` | `36.71K/s` | `32.2ms` | `40.6ms` | `625.5MB` | `flow_state_lmdb_soak_profile_after_flow_shard_marker_20260526T112854Z.log`; removed the `flow_many_same_state_machine_shard?/2 -> Router.shard_for/2` profile frame |
| Shard marker + zero-hot direct eviction | `20.96K/s` | `41.05K/s` | `28.7ms` | `36.2ms` | `619.6MB` | `flow_state_lmdb_soak_profile_after_zero_hot_direct_20260526T113414Z.log`; removed the `HistoryProjector.trim_history_hot_cache_by_rank/4` rank-trim profile frame for default hot-history policy |

Conclusion:

- The shard marker is a small but correct apply-path CPU reduction.
- The zero-hot history fast path is the real win in this round: about `+11.5%`
  Flow throughput over the projection-writer baseline and materially lower
  claim/transition latency.
- Remaining sampled hotspots are now claim planning/decode work:
  `flow_plan_claim_candidates_native/10`,
  `flow_decode_native_claim_plans/1`,
  `flow_native_claim_keys/4`, and `flow_claim_due_scan/13`.

## Async Apply Projection Cache Spill

Workload: 120s Flow state/LMDB soak, clean temp data dir:

- `TARGET_OPS_PER_SEC=50000`
- `PAYLOAD_BYTES=1000`
- `NORMAL_STEPS=50`
- `LONG_FLOWS=1`
- `LONG_STEPS=10000`
- `SHARDS=16`
- `FLOW_LATENCY_SAMPLE_RATE=10`

Change:

- `maybe_compact_apply_projection_cache/1` no longer spills the apply
  projection cache synchronously inside storage apply.
- Apply now starts one bounded background spill task per shard. New applies
  skip starting another spill while one is pending.
- This is safe because the apply projection cache is a read cache, not the
  durability boundary. The WAraft segment remains the durable source before
  trim, and trim/checkpoint writes durable projection data before old segment
  removal.

Targeted tests:

```sh
mix test --no-start \
  test/ferricstore/raft/waraft_backend_test.exs:8487 \
  test/ferricstore/raft/waraft_backend_test.exs:8520 \
  test/ferricstore/raft/waraft_backend_test.exs:8600 \
  test/ferricstore/raft/waraft_backend_test.exs:8623 \
  --max-failures 3
```

Results:

- Focused cache tests passed.
- Nearby projection/restart coverage passed.

Benchmark comparison:

| Run | Flow ops/sec | Write ops/sec | Storage cache avg/max | Storage apply avg/max | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| Sync spill | `27.36K/s` | `53.79K/s` | `1616us / 214708us` | `4550us / 215220us` | `bench/results/flow_state_lmdb_soak_spill_projection_20260527T050841Z.log` |
| Async spill | `27.45K/s` | `53.98K/s` | `3us / 4986us` | `2947us / 26217us` | `bench/results/flow_state_lmdb_soak_async_apply_projection_cache_20260527T052012Z.log` |

Conclusion:

- The target hotspot moved out of the apply critical path. Cache phase max fell
  from about `215ms` to `5ms`; storage apply max fell from about `215ms` to
  `26ms`.
- Overall Flow latency changed only modestly because the remaining tail is
  batcher queue/flush pressure, not projection-cache spill.

## DBOS-Style Long Poll Terminal Batch Fan-Out

Workload: 100K live queue Flow DBOS-style benchmark:

- `FLOWS=100000`
- `WORKER_MODE=blocking`
- `CLAIM_PARTITION_BATCH_SIZE=16`
- `workers=16`
- `producers=8`
- `partitions=1024`
- `shards=16`

Change:

- `FLOW.*_MANY independent` terminal writes now use an independent terminal
  batch path that skips cross-shard parent/child pre-read planning.
- Terminal shard groups are submitted through the existing batch path instead
  of synchronously writing one shard group at a time.

Targeted tests:

```sh
mix test \
  apps/ferricstore/test/ferricstore/flow_test.exs:6989 \
  apps/ferricstore/test/ferricstore/flow_test.exs:7146 \
  --max-failures 3
```

Results:

- Focused terminal-many tests passed.
- New guard test proves independent terminal batches do not pre-read records for
  cross-shard planning.

Benchmark comparison:

| Run | E2E flows/sec | Create/sec | Process/sec | Claim avg | Complete avg | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Long poll no-tail baseline | `16.14K/s` | `139.86K/s` | `16.14K/s` | `61.6ms` | `357.1ms` | `bench/results/dbos_style_longpoll_partitions16_notail_20260527T054330Z.log` |
| Skip terminal pre-read only | `16.06K/s` | `139.86K/s` | `16.06K/s` | `61.6ms` | `357.1ms` | `bench/results/dbos_style_independent_terminal_fast_20260527T055745Z.log` |
| Terminal shard fan-out | `16.47K/s` | `145.81K/s` | `16.48K/s` | `57.1ms` | `108.0ms` | `bench/results/dbos_style_terminal_parallel_shards_20260527T060132Z.log` |
| Terminal fan-out + drain block 50ms | `16.42K/s` | `137.60K/s` | `16.43K/s` | `73.3ms` | `109.8ms` | `bench/results/dbos_style_drain_block50_100k_20260527T061300Z.log` |
| No server BLOCK control | `58.02K/s` | `62.33K/s` | `58.07K/s` | `15.2ms` | `46.1ms` | `CLAIM_BLOCK_MS=-1 CLAIM_DRAIN_BLOCK_MS=-1`; `bench/results/dbos_style_noblock_control_100k_20260527T061545Z.log` |
| Active scan, drain-only block | `58.58K/s` | `63.83K/s` | `58.65K/s` | `15.6ms` | `46.3ms` | `BLOCK 5000` configured, but benchmark workers only block after producers finish; `bench/results/dbos_style_active_scan_100k_20260527T071134Z.log` |

Conclusion:

- Removing the independent terminal pre-read is correctness/performance-clean,
  but not enough by itself.
- Fan-out dropped `complete_many` average latency by roughly `3.3x`.
- Disabling server-side `BLOCK` recovers the old throughput class on this
  100K profile, proving the regression was wait scheduling and not WARaft or
  completion batching.
- The fixed benchmark algorithm scans owned partition pages without `BLOCK`
  while producers are active, then uses short `CLAIM_DRAIN_BLOCK_MS=50` only
  after producers finish. This keeps long polling for idle drain while preserving
  active-throughput behavior.

## DBOS-Style And Flow Soak Check, 2026-05-27

Purpose: verify the current WARaft/Flow branch is still in the expected DBOS
worker throughput range, then run a clean 5-minute Flow soak to check latency,
LMDB lag, memory, and storage behavior.

DBOS-style 100K worker profile:

```sh
MIX_ENV=bench FLOWS=100000 SHARDS=16 WORKERS=16 \
  PRODUCERS=8 TRANSPORT=many CLAIM_BLOCK_MS=5000 CLAIM_DRAIN_BLOCK_MS=50 \
  mix run --no-start bench/flow_python_backend_profile.exs
```

Result:

- `100,000` created, claimed, and completed.
- `0` duplicate completions.
- End-to-end throughput: `55,653 flows/sec`.
- Create throughput: `60,223/sec`.
- Process throughput: `55,710/sec`.
- Log: `bench/results/dbos_style_current_100k_20260527T171219Z.log`.

5-minute clean Flow soak:

```sh
MIX_ENV=bench \
  DATA_DIR=/tmp/ferricstore_flow_soak_current KEEP_DATA_DIR=false \
  DURATION_SECONDS=300 TARGET_OPS_PER_SEC=50000 PAYLOAD_BYTES=1000 \
  NORMAL_STEPS=50 LONG_FLOWS=1 LONG_STEPS=10000 SAMPLE_INTERVAL_SECONDS=30 \
  MIN_FREE_DISK_MB=5000 MAX_TOTAL_MEM_MB=64000 SHARDS=16 WORKERS=32 PRODUCERS=8 \
  PARTITIONS=4096 CREATE_MODE=many CREATE_INFLIGHT=64 \
  CLAIM_BATCH_SIZE=1000 CLAIM_PARTITION_BATCH_SIZE=32 APPLY_INFLIGHT=8 \
  FLOW_LATENCY_SAMPLE_RATE=10 \
  mix run --no-start bench/flow_state_lmdb_soak.exs
```

Result:

- Flow ops: `14,788,597`.
- Flow throughput: `48,473/sec`.
- Write throughput: `95,999/sec`.
- WARaft flush errors/apply-full/commit timeouts: `0`.
- LMDB max pending: `4,356`.
- LMDB max oldest lag: `1.01s`.
- Max disk during run: `50.1GB`.
- Max LMDB size: `20.6GB`.
- Max WARaft size: `21.7GB`.
- Max BEAM memory: `1.07GB`.
- Max binary memory: `348MB`.
- Max keydir binary memory: `25.8MB`.
- Log: `bench/results/flow_state_lmdb_soak_current_5m_20260527T171303Z.log`.

Sampled latency summary:

| Command | Avg | Avg/item | p50 | p95 | p99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| create | `51.4ms` | `2.73ms` | `<=50ms` | `<=250ms` | `<=250ms` | `262ms` |
| claim_due | `35.9ms` | `0.20ms` | `<=30ms` | `<=100ms` | `<=250ms` | `242ms` |
| transition | `69.0ms` | `1.07ms` | `<=75ms` | `<=250ms` | `<=250ms` | `288ms` |
| complete | `69.0ms` | `0.55ms` | `<=75ms` | `<=250ms` | `<=250ms` | `211ms` |

Conclusion:

- DBOS-style worker path is still in the target `50K-60K/sec` band on the
  100K profile.
- The 5-minute soak is stable and close to the previous dashboard-soak class:
  `48.5K flow_ops/sec`, `96.0K writes/sec`, no WARaft commit timeouts, and LMDB
  lag around one second worst case.
- The soak is slightly below the requested `50K/sec` target, but operationally
  clean. Remaining work is latency/throughput tuning rather than correctness or
  projection lag repair.

## DBOS-Style Audit-Fix Performance Repair, 2026-05-31

Context: follow-up audit fixes introduced two performance risks on the Flow
DBOS-style path:

- WARaft trim became synchronous for correctness; the first implementation made
  the segment provider physically rewrite trimmed files on the commit path.
- A ClaimWaiters wake optimization scanned the waiter table for large ready
  fanout.

Fixes:

- Keep synchronous WARaft trim correctness, but make trim persist a logical
  trim floor and remove old entries from in-memory/offset views only. Physical
  segment cleanup is no longer in the commit path; recovery honors the durable
  floor and will not replay trimmed entries.
- Drop the waiter-table scan optimization. It benchmarked worse than direct
  ready-bucket lookups.

Targeted tests:

```sh
mix test \
  apps/ferricstore/test/ferricstore/raft/waraft_segment_log_test.exs \
  apps/ferricstore/test/ferricstore/flow/claim_waiters_test.exs
```

Results:

- Segment log: `38 tests, 0 failures`.
- Claim waiters: `22 tests, 0 failures`.

100K DBOS-style repair run:

```sh
MIX_ENV=bench FLOWS=100000 SHARDS=16 WORKERS=16 \
  PRODUCERS=8 TRANSPORT=many CLAIM_BLOCK_MS=5000 CLAIM_DRAIN_BLOCK_MS=50 \
  mix run --no-start bench/flow_python_backend_profile.exs
```

Result:

- End-to-end throughput: `51,359 flows/sec`.
- Create throughput: `56,408/sec`.
- Process throughput: `51,409/sec`.
- Claim calls: `864`, empty claims: `628`, average claim batch: `115.7`.
- Log: `bench/results/dbos_style_after_audit_perf_repair_100k_20260531T092201Z.log`.

Rejected experiment:

- One non-blocking partition probe followed by one `BLOCK` claim across all
  owned partitions reduced claim calls to `238`, but throughput collapsed to
  `15,716 flows/sec`.
- Reason: multi-partition blocking over all owned partitions is too expensive
  for this hot worker benchmark, even though it reduces empty polling.
- Log: `bench/results/dbos_style_one_probe_then_block_100k_20260531T091944Z.log`.

Conclusion:

- The physical trim rewrite was a real bug/performance conflict and is fixed
  without weakening restart correctness.
- The waiter-table scan was a real regression and should stay out.
- The first repaired run returned to the `50K-60K/sec` target band, though
  below the best May 27 run (`55,653/sec`). Follow-up repeats below showed
  local variance; the remaining difference is mostly WARaft hot-flush time, not
  segment append bytes.

Follow-up repair:

- A clean pre-audit worktree rerun with the current Python SDK shape produced
  `49,301 flows/sec`, so the full drop from the older `55K/sec` run was not
  reproduced before the audit fixes either.
- A failed current benchmark exposed build contamination from the pre-audit
  worktree: WARaft BEAM debug paths referenced `/tmp/ferricstore-preaudit`, and
  a stale segment-log module missed `reset_disk_to_position/2`. Cleaning
  `_build/prod` and `_build/bench` restored the correct export.
- The remaining reproducible short-run cost was background LMDB projection
  flush contention. The writer now defers timer and replay-safe flushes while
  writes are still arriving, bounded by a `5s` max projection lag; the default
  quiet window is `1s`.

Tests:

```sh
mix test apps/ferricstore/test/ferricstore/flow_lmdb_test.exs
mix test apps/ferricstore/test/ferricstore/raft/waraft_segment_log_test.exs
mix test apps/ferricstore/test/ferricstore/flow/claim_waiters_test.exs
```

Results:

- Flow LMDB: `130 tests, 0 failures`.
- Segment log: `38 tests, 0 failures`.
- Claim waiters: `22 tests, 0 failures`.

100K DBOS-style results after adaptive LMDB flush and opt-in internal profiling:

- Best repeat: `52,305 flows/sec`, hot_flush avg `46.2ms`.
- Median of three local repeats: ~`48.3K flows/sec`.
- Worst repeat: `43,359 flows/sec`, with one small LMDB flush and hot_flush
  avg `59.5ms`; this looks like local scheduler/disk noise rather than a
  correctness-fix regression.
- Two of the three repeats had `0` LMDB flush events during the hot profile.
- Logs:
  - `bench/results/dbos_style_no_internal_profile_quiet1s_100k_20260531T095122Z.log`
  - `bench/results/dbos_style_no_internal_profile_quiet1s_repeat_100k_20260531T095146Z.log`
  - `bench/results/dbos_style_no_internal_profile_quiet1s_repeat2_100k_20260531T095212Z.log`

Clean default validation after LMDB shutdown fix:

- Problem observed: raising `flow_lmdb_max_concurrent_flushes` to an adaptive
  default bounded shutdown but cut the 1M DBOS-style run to ~`29K flows/sec`.
- Final fix: keep steady-state LMDB projection flushes serial by default,
  request `flush_all` concurrently when explicitly called, and skip the
  rebuildable lagged LMDB projection drain during node shutdown.
- Clean no-arg command:

```sh
MIX_ENV=bench mix run --no-start bench/flow_python_backend_profile.exs
```

Result:

- Created/completed: `1,000,000 / 1,000,000`, duplicates: `0`.
- Create throughput: `50,825 flows/sec`.
- Process throughput: `49,368 flows/sec`.
- End-to-end throughput: `49,346 flows/sec`.
- Shutdown exited cleanly with no Flow LMDB flush timeout.
- LMDB flush profile during workload: `6` flushes, `80,000` projected ops,
  average batch `13,333`.

DBOS-style benchmark profiler/arg audit:

- The benchmark telemetry profiler is now opt-in through `PROFILE_TELEMETRY=1`
  (or `PROFILE_WARAFT_INTERNAL=1`). Clean runs no longer attach the ETS
  telemetry collector by default.
- No-profiler controls showed the remaining gap was not mostly logs or
  profiling: 100K default runs landed in the `47K-52K/sec` band depending on
  local scheduler/disk noise; a no-BLOCK control was only ~`51K/sec`.
- A rejected short-run arg tweak, `PRODUCERS=16`, reached `56.2K/sec` on 100K
  but collapsed the 1M run to ~`21.4K/sec`, so it is not a production/default
  fix.
- Real planner fix: the low-level DBOS-style benchmark now uses shard-aware
  auto-partition ownership, matching the SDK worker planner. TDD added a unit
  test proving a single auto-partition claim page stays on one server shard.
- Default-profile fix: `bench/flow_python_backend_profile.exs` now has one
  guarded DBOS production profile for the no-arg benchmark path. The profile is
  `1M` flows, `16` workers, `8` producers, `1024` auto partitions, claim page
  size `16`, one drain batch, async complete depth `4`, and `16` server shards.
  This avoids hidden run-to-run drift from scattered literal defaults.
- Validation after the planner fix:
  - 100K default: `50,079 flows/sec`.
  - 1M default: `51,735 flows/sec`.
  - 1M clean no-arg profile after centralized defaults: `50,023 flows/sec`,
    created/completed `1,000,000 / 1,000,000`, duplicates `0`.
  - 1M with `CLAIM_PARTITION_BATCH_SIZE=32`: `51,400 flows/sec`, so the default
    `16` remains better for this profile.

## Flow Claim-Due Ordered Native Runs, 2026-06-01

Problem:

- Multi-state `FLOW.CLAIM_DUE` used native due candidates, then regrouped them
  in Elixir. That extra grouping could reintroduce state/key bias and added
  avoidable hot-path allocation after the native scan had already produced
  ordered candidates.

Fix:

- The native ordered-index claim scan now merges due candidates by score across
  due keys and returns adjacent ordered runs directly:
  `{due_key, [{member, score}, ...]}`.
- The state-machine apply path consumes those ordered runs as-is. This preserves
  global due-time order while avoiding the Elixir regrouping pass.

TDD / verification:

```sh
mix test apps/ferricstore/test/ferricstore/flow/ordered_index_test.exs --max-failures 3
mix test apps/ferricstore/test/ferricstore/flow_test.exs:1268 --max-failures 1
mix test apps/ferricstore/test/ferricstore/flow_test.exs --max-failures 3
(cd apps/ferricstore/native/ferricstore_bitcask && cargo test)
```

Results:

- Native ordered-index wrapper: `20 tests, 0 failures`.
- Focused multi-state fairness regression: `1 test, 0 failures`.
- Flow suite: `212 tests, 0 failures`.
- Native bitcask crate: `564 tests, 0 failures`.

Clean DBOS-style default benchmark:

```sh
MIX_ENV=bench mix run --no-start bench/flow_python_backend_profile.exs
```

Result:

- Created/completed: `1,000,000 / 1,000,000`, duplicates: `0`.
- Create throughput: `56,892 flows/sec`.
- Process throughput: `56,355 flows/sec`.
- End-to-end throughput: `56,329 flows/sec`.
