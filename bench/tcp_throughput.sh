#!/bin/bash
# FerricStore TCP Benchmark (via memtier_benchmark)
#
# Run the SAME workload as bench/erpc_throughput.exs but over Redis protocol.
# FerricStore is quorum-only: every write goes through Raft.
#
# Usage:
#   mix run --no-halt   # start FerricStore
#   bash bench/tcp_throughput.sh

HOST=${1:-127.0.0.1}
PORT=${2:-6379}
CLIENTS=50
THREADS=4
REQUESTS=100000
DATA_SIZE=100
KEY_MAX=1000000

echo "============================================"
echo "  FerricStore TCP Benchmark"
echo "  Host: $HOST:$PORT"
echo "  Clients: $CLIENTS, Threads: $THREADS"
echo "  Requests: $REQUESTS, Payload: ${DATA_SIZE}B"
echo "============================================"

# ===================================================================
# QUORUM WRITES
# ===================================================================

echo ""
echo "=== QUORUM WRITES ==="

echo ""
echo "--- SET ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="SET bench:__key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- GET ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="GET bench:__key__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --hide-histogram

echo ""
echo "--- HSET ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="HSET bench:hash:__key__ field __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- LPUSH ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="LPUSH bench:list:__key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- SADD ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="SADD bench:set:__key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- INCR ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="INCR bench:counter:__key__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --hide-histogram

echo ""
echo "=== Done ==="
