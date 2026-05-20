# Flow Lineage Bench

- started_at: 2026-05-05T16:43:51.762335Z
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
- beam_memory_before: 84395978
- beam_memory_after_seed: 729631374
- beam_memory_delta: 645235396

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create lineage under 100000 | 82 | 12124 | 8829 | 18864 | 113060 | 140343 | 200 |
| flow.by_parent hot count=100 | 217 | 4614 | 4389 | 5122 | 5897 | 33642 | 200 |
| flow.by_root hot count=100 | 224 | 4457 | 4441 | 4783 | 4956 | 5457 | 200 |
| flow.by_correlation hot count=100 | 230 | 4349 | 4325 | 4596 | 4722 | 4847 | 200 |
| flow.by_root terminal lmdb count=100 | 808 | 1238 | 962 | 2012 | 6859 | 9350 | 200 |
| flow.by_correlation terminal lmdb count=100 | 1120 | 893 | 865 | 1200 | 1413 | 1426 | 200 |
