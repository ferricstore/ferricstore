# Flow Lineage Bench

- started_at: 2026-05-05T16:21:54.893646Z
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
- beam_memory_before: 81075779
- beam_memory_after_seed: 797449854
- beam_memory_delta: 716374075

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create lineage under 100000 | 94 | 10600 | 9499 | 20297 | 25575 | 32078 | 200 |
| flow.by_parent hot count=100 | 225 | 4454 | 4339 | 4646 | 5286 | 27124 | 200 |
| flow.by_root hot count=100 | 229 | 4364 | 4384 | 4571 | 4663 | 4709 | 200 |
| flow.by_correlation hot count=100 | 233 | 4287 | 4286 | 4596 | 4807 | 5376 | 200 |
| flow.by_root terminal lmdb count=100 | 717 | 1395 | 1070 | 2246 | 6174 | 12140 | 200 |
| flow.by_correlation terminal lmdb count=100 | 784 | 1276 | 1449 | 1611 | 1669 | 1759 | 200 |
