# Benchmark Setup Issues

## Follow-up Tasks

### erpc/Benchee benchmark on client VM
After the TCP tuning suite completes, run `bench/erpc_throughput.exs` from the client VM (`20.25.227.12`) against the server. This tests the Elixir-native erpc protocol (no RESP3 overhead). Run with:
```bash
ssh ferric@20.25.227.12
cd ~/ferricstore
elixir --sname bench --cookie ferricstore_bench -S mix run bench/erpc_throughput.exs --remote ferricstore@ferricstore-0
```
Client VM needs cloud-init setup to complete first (OTP, Elixir, Rust, FerricStore compiled).

---

Issues found while setting up the tuning benchmark suite on Azure VMs.

## Fixed During Session

### 2. NIF .so not compiled for prod
**Symptom:** `UndefinedFunctionError: function Ferricstore.Bitcask.NIF.v2_scan_file/1 is undefined`
**Cause:** Older config skipped native compilation unless a build env flag was set, so prod builds could load stale or missing `.so` files.
**Fix:** Repo checkouts now source-build NIFs by default through Rustler config. `MIX_ENV=prod mix compile` is enough.
**Action needed:** None for repo checkouts. Packaged dependency users continue to use RustlerPrecompiled artifacts.

## Azure / Cloud-Init Issues

### 4. cloud-init.yaml user ordering bug
**Symptom:** `write_files` fails with `Unknown user or group: ferric` because it tries to `chown` before the user is created.
**Cause:** cloud-init's `write_files` module runs before `users` module creates the admin user.
**Fix:** Remove `owner:` from `write_files` entries, or use `runcmd` to chown after user creation.
**File:** `deploy/azure/cloud-init.yaml`

### 5. `libncurses5-dev` unavailable on Ubuntu 24.04
**Symptom:** Package install fails.
**Cause:** Ubuntu 24.04 replaced `libncurses5-dev` with `libncurses-dev`.
**Fix:** Change to `libncurses-dev` in cloud-init.yaml.
**File:** `deploy/azure/cloud-init.yaml`

### 6. NVMe device path wrong
**Symptom:** `cannot create /sys/block/nvme0/queue/scheduler: Directory nonexistent`
**Cause:** L4as_v4 has NVMe at `nvme1n1` and `nvme2n1` (not `nvme0n1` which is the OS disk). cloud-init.yaml hardcodes `nvme0`.
**Fix:** cloud-init should detect the NVMe device dynamically (e.g., first non-OS NVMe device).
**File:** `deploy/azure/cloud-init.yaml`

### 7. NVMe already formatted by Azure
**Symptom:** `mke2fs: /dev/nvme0n1 is apparently in use by the system`
**Cause:** Azure uses `nvme0n1` as the OS disk. The local NVMe temp storage is on `nvme1n1`/`nvme2n1`.
**Fix:** Format `nvme1n1` instead of `nvme0n1`.
**File:** `deploy/azure/cloud-init.yaml`

### 8. Missing `zlib1g-dev` for memtier_benchmark
**Symptom:** memtier `configure` fails: `zlib is required; try installing zlib1g-dev`
**Cause:** `zlib1g-dev` not in the package list.
**Fix:** Add `zlib1g-dev` to packages in cloud-init-client.yaml (and server if building memtier there).
**File:** `deploy/azure/cloud-init-client.yaml`

### 9. VM SKU quota limits
**Symptom:** Various `OperationNotAllowed` errors about exceeding core quotas.
**Cause:** Pay-as-you-go subscriptions have per-family quotas (e.g., `DASv5Family = 0`), not just total regional quota.
**Fix:** Check per-family quota with `az vm list-usage --location <region>`, use families with quota > 0.

### 10. `Standard_L4s_v3` doesn't exist
**Symptom:** Terraform error: `The value Standard_L4s_v3 provided for the VM size is not valid`
**Cause:** L-series v3 starts at L8. There is no L4s_v3.
**Fix:** Use `Standard_L4as_v4` (4 vCPU, dedicated NVMe, AMD).
**File:** `deploy/azure/variables.tf`

## Application Issues

### 13. Pre-population with quorum writes is very slow
**Symptom:** Pre-populating 100K keys via memtier takes several minutes with quorum durability (Raft + fsync per batch).
**Cause:** Each write goes through Raft consensus and Bitcask fsync. On 4 cores with 4 shards, throughput is limited by fsync latency.
**Impact:** Benchmark cycle time is dominated by pre-population, not the actual benchmark workloads.
**Action needed:** Pre-populate through the fastest quorum path available: batched writes, larger pipelines, fixture import, or a reusable warmed data directory. Do not use a separate non-quorum durability path for benchmark setup.

### 17. release_cursor_interval too small for high throughput
**Symptom:** Ra snapshots fire every ~1.3 seconds per shard at 7.6K ops/sec. Each snapshot + ETS delete takes 25-50ms.
**Cause:** `FERRICSTORE_RELEASE_CURSOR_INTERVAL=10000` (default in runtime.exs). At 100K ops/sec target, that's a snapshot every 0.1 seconds — constant overhead.
**Impact:** Not the bottleneck at 7.6K ops/sec (server is idle), but will become a problem at higher throughput.
**Fix:** Increase to 100,000 or make it time-based (~60 seconds) instead of count-based.
**File:** `config/runtime.exs:103`, `apps/ferricstore/lib/ferricstore/raft/state_machine.ex`

### 14. memtier `-n` is per-client, not total
**Symptom:** Write workloads take 10+ minutes because 50 clients × 4 threads × 100K requests = 20M total quorum writes.
**Cause:** memtier's `-n` flag specifies requests **per client**. With 200 connections (50 clients × 4 threads), `-n 100000` = 20M total.
**Fix:** Reduced default `MT_REQUESTS` from 100000 to 10000 (= 2M total requests, still statistically significant).

## Bench Script Issues

### 15. memtier JSON key is "ALL STATS" not "ALL_STATS"
**Symptom:** All parsed results show 0 ops/sec.
**Cause:** memtier_benchmark uses `"ALL STATS"` (with space) as the JSON key, not `"ALL_STATS"` (underscore). The jq parser couldn't find the data.
**Fix:** Updated `parse_and_emit` to use `."ALL STATS"` in jq queries.
**File:** `bench/tuning_suite.sh`

### 16. memtier JSON doesn't have percentile latencies in Totals
**Symptom:** p50/p99/p999 all return 0.
**Cause:** memtier Totals only has `"Latency"` (median), `"Average Latency"`, `"Min Latency"`, `"Max Latency"`. Per-second percentiles are in Time-Serie entries, not aggregated in Totals.
**Workaround:** Use Average Latency for p50 and Max Latency for p99/p999. For real percentiles, use `--print-percentiles=50,99,99.9` in the text output and parse that, or compute from Time-Serie data.
**File:** `bench/tuning_suite.sh`

### 11. `pipefail` breaks metadata on macOS
**Symptom:** Script exits early during `write_metadata` on macOS.
**Cause:** `grep MemTotal /proc/meminfo` fails on macOS and `set -o pipefail` propagates the error through the pipe.
**Fix:** Changed to `cat /proc/meminfo 2>/dev/null | grep MemTotal | awk ... || true`.
**File:** `bench/tuning_suite.sh`
