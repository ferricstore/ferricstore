# Flow Lineage Bench

- started_at: 2026-05-05T16:21:43.430339Z
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
- beam_memory_before: 79242219
- beam_memory_after_seed: 81020170
- beam_memory_delta: 1777951

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create lineage under 20 | 110 | 9121 | 5540 | 16312 | 16312 | 16312 | 3 |
| flow.by_parent hot count=2 | 113 | 8854 | 311 | 26052 | 26052 | 26052 | 3 |
| flow.by_root hot count=2 | 5607 | 178 | 159 | 228 | 228 | 228 | 3 |
| flow.by_correlation hot count=2 | 9202 | 109 | 111 | 117 | 117 | 117 | 3 |
| flow.by_root terminal lmdb count=2 | 7673 | 130 | 125 | 150 | 150 | 150 | 3 |
| flow.by_correlation terminal lmdb count=2 | 6186 | 162 | 158 | 179 | 179 | 179 | 3 |
