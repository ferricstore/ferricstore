# Flow Lineage Bench

- started_at: 2026-05-05T16:29:31.417979Z
- backlog: 20
- terminal_count: 8
- iterations: 3
- roots: 4
- terminal_roots: 2
- shards: 2
- partitions: 2
- query_count: 2
- seed_concurrency: 32
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- beam_memory_before: 79249232
- beam_memory_after_seed: 80854911
- beam_memory_delta: 1605679

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create lineage under 20 | 129 | 7732 | 7704 | 9182 | 9182 | 9182 | 3 |
| flow.by_parent hot count=2 | 365 | 2737 | 300 | 7671 | 7671 | 7671 | 3 |
| flow.by_root hot count=2 | 4769 | 210 | 153 | 339 | 339 | 339 | 3 |
| flow.by_correlation hot count=2 | 7109 | 141 | 145 | 160 | 160 | 160 | 3 |
| flow.by_root terminal lmdb count=2 | 3937 | 254 | 212 | 351 | 351 | 351 | 3 |
| flow.by_correlation terminal lmdb count=2 | 6977 | 143 | 153 | 157 | 157 | 157 | 3 |
