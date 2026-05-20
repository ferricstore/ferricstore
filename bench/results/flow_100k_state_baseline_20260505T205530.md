# Flow 100k State Baseline

- started_at: 2026-05-05T20:55:30.279557Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 100
- beam_memory_before: 87339854
- beam_memory_after_seed: 439693402
- beam_memory_delta: 352353548

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 126 | 7918 | 8202 | 15474 | 16892 | 16983 | 200 |
| flow.create_many batch=100 under 100000 | 101 | 9918 | 9082 | 13470 | 14750 | 19489 | 200 |
| flow.get from 100000 | 15187 | 66 | 58 | 88 | 166 | 1098 | 200 |
| flow.list count=100 from 100000 | 2537 | 394 | 269 | 334 | 5219 | 5629 | 200 |
| flow.info over 100000 | 258 | 3881 | 267 | 376 | 151787 | 229602 | 200 |
| flow.history count=10 under 100000 | 13728 | 73 | 64 | 134 | 157 | 176 | 200 |
| flow.stuck count=100 under 100000 | 6391 | 156 | 114 | 140 | 1839 | 2815 | 200 |
| flow.claim_due limit=100 from 100000 | 78 | 12872 | 12365 | 19525 | 22626 | 23835 | 200 |
| flow.transition under 100000 | 89 | 11216 | 10269 | 19238 | 24104 | 37008 | 200 |
| flow.transition_many batch=100 under 100000 | 77 | 13022 | 12202 | 19486 | 22251 | 48608 | 200 |
| flow.complete under 100000 | 69 | 14471 | 10703 | 18509 | 155825 | 171961 | 200 |
| flow.retry under 100000 | 113 | 8857 | 8539 | 15418 | 16907 | 19272 | 200 |
| flow.fail under 100000 | 115 | 8699 | 8494 | 15814 | 21050 | 22913 | 200 |
| flow.cancel under 100000 | 88 | 11313 | 8254 | 14667 | 154107 | 229452 | 200 |
| flow.rewind under 100000 | 124 | 8043 | 8332 | 13189 | 16164 | 22243 | 200 |
