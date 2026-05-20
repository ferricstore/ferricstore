# Flow 100k State Baseline

- started_at: 2026-05-05T05:42:38.360971Z
- backlog: 20000
- iterations: 100
- shards: 4
- partitions: 4
- seed_concurrency: 64
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 100
- beam_memory_before: 84204132
- beam_memory_after_seed: 176686916
- beam_memory_delta: 92482784

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 20000 | 117 | 8513 | 8352 | 13892 | 16263 | 18741 | 100 |
| flow.create_many batch=100 under 20000 | 62 | 16177 | 11879 | 20525 | 86792 | 316446 | 100 |
| flow.get from 20000 | 8369 | 119 | 63 | 181 | 348 | 3747 | 100 |
| flow.list count=100 from 20000 | 2109 | 474 | 273 | 380 | 5168 | 5233 | 100 |
| flow.info over 20000 | 951 | 1051 | 22 | 39 | 34097 | 38358 | 100 |
| flow.history count=10 under 20000 | 20387 | 49 | 41 | 71 | 221 | 233 | 100 |
| flow.stuck count=100 under 20000 | 5059 | 198 | 130 | 157 | 1912 | 2300 | 100 |
| flow.claim_due limit=100 from 20000 | 69 | 14403 | 14150 | 17895 | 21943 | 41880 | 100 |
| flow.transition under 20000 | 83 | 11999 | 10840 | 20004 | 26619 | 28462 | 100 |
| flow.transition_many batch=100 under 20000 | 68 | 14691 | 15374 | 21331 | 22662 | 26008 | 100 |
| flow.complete under 20000 | 84 | 11841 | 10179 | 23129 | 28459 | 35996 | 100 |
| flow.retry under 20000 | 104 | 9584 | 8912 | 15888 | 20501 | 21628 | 100 |
| flow.fail under 20000 | 89 | 11215 | 10440 | 19780 | 23058 | 23783 | 100 |
| flow.cancel under 20000 | 108 | 9235 | 8756 | 16966 | 20068 | 21079 | 100 |
| flow.rewind under 20000 | 86 | 11578 | 10421 | 18084 | 22084 | 23202 | 100 |
