#!/bin/bash
# Benchmark: quorum write, read — varying shard count (2, 3, 4)
#
# Run from the CLIENT VM:
#   bash bench/shard_bench.sh <server_ip>
#
# Prerequisites:
#   - Server VM has FerricStore built and systemd service configured
#   - Client has memtier_benchmark installed
#   - SSH key-based access to server as 'ferric'

set -euo pipefail

SERVER=${1:?Usage: bash bench/shard_bench.sh <server_ip>}
PORT=${2:-6379}
SSH_USER=${3:-ferric}

CLIENTS=50
THREADS=4
REQUESTS=100000
PIPELINE=50
DATA_SIZE=256
KEY_MAX=1000000
PREPOP_KEYS=100000

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="bench/results/shards_${TIMESTAMP}"
mkdir -p "$RESULTS_DIR/json"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------

restart_ferricstore() {
  local shards=$1
  log "Restarting FerricStore with SHARD_COUNT=$shards"

  ssh ${SSH_USER}@${SERVER} "sudo systemctl stop ferricstore || true; sleep 1; sudo rm -rf /data/ferricstore/*"

  # Create systemd override for shard count.
  ssh ${SSH_USER}@${SERVER} "sudo mkdir -p /etc/systemd/system/ferricstore.service.d && \
    echo '[Service]
Environment=FERRICSTORE_SHARD_COUNT=${shards}
Environment=FERRICSTORE_PROTECTED_MODE=false' | sudo tee /etc/systemd/system/ferricstore.service.d/bench.conf > /dev/null && \
    sudo systemctl daemon-reload && \
    sudo systemctl start ferricstore"

  # Wait for port
  local attempts=0
  while ! nc -z "$SERVER" "$PORT" 2>/dev/null; do
    sleep 1
    attempts=$((attempts + 1))
    if [ $attempts -gt 30 ]; then
      log "ERROR: FerricStore did not start within 30s"
      exit 1
    fi
  done
  sleep 2
  log "FerricStore ready (shards=$shards)"
}

prepopulate() {
  log "Pre-populating $PREPOP_KEYS keys for read bench"
  memtier_benchmark -s "$SERVER" -p "$PORT" \
    --protocol=resp3 \
    --clients=50 --threads=4 \
    --requests=$((PREPOP_KEYS / 50 / 4)) \
    --pipeline=$PIPELINE \
    --command="SET bench:read:__key__ __data__" \
    --command-key-pattern=P \
    --key-minimum=1 --key-maximum=$PREPOP_KEYS \
    --data-size=$DATA_SIZE \
    --hide-histogram > /dev/null 2>&1
  log "Pre-population done"
}

run_bench() {
  local name=$1
  local shards=$2
  local workload=$3
  shift 3
  local label="${shards}shards_${workload}"
  local json_file="$RESULTS_DIR/json/${label}.json"

  log "  Running: $workload (shards=$shards)"
  memtier_benchmark -s "$SERVER" -p "$PORT" \
    --protocol=resp3 \
    --clients=$CLIENTS --threads=$THREADS \
    --requests=$REQUESTS \
    --pipeline=$PIPELINE \
    --data-size=$DATA_SIZE \
    --key-minimum=1 --key-maximum=$KEY_MAX \
    --json-out-file="$json_file" \
    --hide-histogram \
    "$@" 2>&1 | tee "$RESULTS_DIR/${label}.txt"
}

parse_json() {
  local json=$1
  if [ -f "$json" ] && command -v jq &>/dev/null; then
    local ops=$(jq -r '."ALL STATS".Totals.Ops' "$json" 2>/dev/null || echo "N/A")
    local p50=$(jq -r '."ALL STATS".Totals."Latency"' "$json" 2>/dev/null || echo "N/A")
    echo "$ops"
  else
    echo "N/A"
  fi
}

# ---------------------------------------------------------------
# CSV header
# ---------------------------------------------------------------

CSV="$RESULTS_DIR/summary.csv"
echo "shards,workload,ops_sec,avg_latency_ms,p50_ms,p99_ms,p999_ms,kb_sec" > "$CSV"

extract_to_csv() {
  local shards=$1
  local workload=$2
  local json="$RESULTS_DIR/json/${shards}shards_${workload}.json"

  if [ -f "$json" ] && command -v jq &>/dev/null; then
    local ops=$(jq -r '."ALL STATS".Totals."Ops/sec"' "$json" 2>/dev/null || echo "0")
    local avg_lat=$(jq -r '."ALL STATS".Totals."Latency"' "$json" 2>/dev/null || echo "0")
    local p50=$(jq -r '."ALL STATS".Totals."Percentile Latencies"."p50.00"' "$json" 2>/dev/null || echo "0")
    local p99=$(jq -r '."ALL STATS".Totals."Percentile Latencies"."p99.00"' "$json" 2>/dev/null || echo "0")
    local p999=$(jq -r '."ALL STATS".Totals."Percentile Latencies"."p99.90"' "$json" 2>/dev/null || echo "0")
    local kbsec=$(jq -r '."ALL STATS".Totals."KB/sec"' "$json" 2>/dev/null || echo "0")
    echo "$shards,$workload,$ops,$avg_lat,$p50,$p99,$p999,$kbsec" >> "$CSV"
  fi
}

# ---------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------

cat > "$RESULTS_DIR/metadata.txt" << EOF
=== Shard Benchmark ===
Date: $(date)
Server: $SERVER:$PORT
Clients: $CLIENTS, Threads: $THREADS, Pipeline: $PIPELINE
Requests: $REQUESTS per run
Data size: ${DATA_SIZE}B
Key space: $KEY_MAX
Pre-populated keys: $PREPOP_KEYS (for reads)
Shard counts tested: 2, 3, 4

=== Workloads ===
write         : SET bench:q:__key__ (Raft + fsync, durable)
read          : GET bench:read:__key__ (ETS hot cache)
mixed_80_20   : --ratio 1:4 = 20% SET + 80% GET (quorum writes)
EOF

# ---------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------

log "Starting shard benchmark suite"
log "Results: $RESULTS_DIR"

for SHARDS in 2 3 4; do
  log "=== SHARD COUNT: $SHARDS ==="
  restart_ferricstore $SHARDS
  prepopulate

  # Write
  run_bench "write" $SHARDS "write" \
    --command="SET bench:q:__key__ __data__"
  extract_to_csv $SHARDS "write"

  # Read (pre-populated keys)
  run_bench "read" $SHARDS "read" \
    --command="GET bench:read:__key__" \
    --key-maximum=$PREPOP_KEYS
  extract_to_csv $SHARDS "read"

  # Mixed 80/20
  run_bench "mixed" $SHARDS "mixed_80_20" \
    --key-pattern=R:R \
    --ratio=1:4 \
    --key-maximum=$PREPOP_KEYS
  extract_to_csv $SHARDS "mixed_80_20"

  log "=== Done: $SHARDS shards ==="
  echo ""
done

log "All done. Results in $RESULTS_DIR/summary.csv"
echo ""
echo "=== SUMMARY ==="
column -t -s, "$CSV"
