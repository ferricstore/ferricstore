# Flow Lineage Bench

- started_at: 2026-05-06T08:55:37.966290Z
- backlog: 100000
- terminal_count: 10000
- iterations: 200
- roots: 1000
- terminal_roots: 100
- shards: 4
- partitions: 4
- query_count: 100
- seed_concurrency: 32
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- beam_memory_before: 84089791
- beam_memory_after_seed: 480829281
- beam_memory_delta: 396739490

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create lineage under 100000 | 103 | 9736 | 9101 | 17100 | 21008 | 29390 | 200 |
| flow.by_parent hot count=100 | 2499 | 400 | 319 | 482 | 571 | 6989 | 200 |
| flow.by_root hot count=100 | 3499 | 286 | 283 | 324 | 345 | 413 | 200 |
| flow.by_correlation hot count=100 | 3964 | 252 | 250 | 285 | 296 | 338 | 200 |
| flow.by_root terminal lmdb count=100 | 1844 | 542 | 498 | 748 | 1257 | 1541 | 200 |
| flow.by_correlation terminal lmdb count=100 | 2469 | 405 | 402 | 450 | 464 | 561 | 200 |
