#!/bin/bash
# Benchmark: TCP_NODELAY on vs off, pipeline=1 vs pipeline=50
#
# Tests the hypothesis that disabling Nagle (nodelay=false) improves
# throughput by coalescing small responses into fewer send() syscalls.
#
# Run twice: once with FERRICSTORE_TCP_NODELAY=true, once with =false.
#
# Usage:
#   FERRICSTORE_TCP_NODELAY=true  mix run --no-halt &
#   bash bench/nodelay_bench.sh         # nodelay=true baseline
#   # restart with:
#   FERRICSTORE_TCP_NODELAY=false mix run --no-halt &
#   bash bench/nodelay_bench.sh         # nodelay=false comparison

HOST=${1:-127.0.0.1}
PORT=${2:-6379}
CLIENTS=50
THREADS=4
REQUESTS=100000
DATA_SIZE=256
KEY_MAX=1000000

echo "============================================"
echo "  Nodelay Benchmark — $HOST:$PORT"
echo "  Clients=$CLIENTS Threads=$THREADS Req=$REQUESTS"
echo "============================================"

for PIPELINE in 1 50; do
  echo ""
  echo "####  PIPELINE=$PIPELINE  ####"

  echo ""
  echo "--- SET (pipeline=$PIPELINE) ---"
  memtier_benchmark -s $HOST -p $PORT \
    --protocol=resp3 \
    --clients=$CLIENTS --threads=$THREADS \
    --requests=$REQUESTS \
    --pipeline=$PIPELINE \
    --command="SET bench:nd:__key__ __data__" \
    --command-key-pattern=R --key-minimum=1 --key-maximum=$KEY_MAX \
    --data-size=$DATA_SIZE \
    --hide-histogram

  echo ""
  echo "--- GET (pipeline=$PIPELINE) ---"
  memtier_benchmark -s $HOST -p $PORT \
    --protocol=resp3 \
    --clients=$CLIENTS --threads=$THREADS \
    --requests=$REQUESTS \
    --pipeline=$PIPELINE \
    --command="GET bench:nd:__key__" \
    --command-key-pattern=R --key-minimum=1 --key-maximum=$KEY_MAX \
    --hide-histogram

done

echo ""
echo "=== Done ==="
