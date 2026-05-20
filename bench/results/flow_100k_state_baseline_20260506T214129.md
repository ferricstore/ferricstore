# Flow 100k State Baseline

- started_at: 2026-05-06T21:41:29.963032Z
- backlog: 20000
- iterations: 100
- shards: 4
- partitions: 4
- seed_concurrency: 32
- create_many_batch: 100
- transition_many_batch: 100
- terminal_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 100
- beam_memory_before: 81390830
- beam_memory_after_seed: 156079569
- beam_memory_delta: 74688739

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 20000 | 203 | 4931 | 4875 | 5795 | 6432 | 8259 | 100 |
| flow.create_many batch=100 under 20000 | 106 | 9401 | 9318 | 10384 | 12853 | 13194 | 100 |
| flow.get from 20000 | 276243 | 4 | 3 | 6 | 15 | 30 | 100 |
| flow.list count=100 from 20000 | 3864 | 259 | 255 | 301 | 313 | 339 | 100 |
| flow.info over 20000 | 157233 | 6 | 5 | 12 | 22 | 25 | 100 |
| flow.history count=10 under 20000 | 5943 | 168 | 117 | 181 | 294 | 4588 | 100 |
| flow.history include_cold count=10 under 20000 | 21678 | 46 | 45 | 59 | 64 | 67 | 100 |
| flow.history cold_consistent count=10 under 20000 | 22795 | 44 | 43 | 52 | 63 | 66 | 100 |
| flow.stuck count=100 under 20000 | 13454 | 74 | 69 | 96 | 116 | 278 | 100 |
| flow.claim_due limit=100 from 20000 | 109 | 9133 | 9103 | 10118 | 10285 | 11007 | 100 |
| flow.transition under 20000 | 211 | 4748 | 4813 | 5542 | 5620 | 5630 | 100 |
| flow.transition_many batch=100 under 20000 | 106 | 9439 | 9721 | 10250 | 10994 | 11627 | 100 |
| flow.complete under 20000 | 184 | 5428 | 4992 | 9965 | 10987 | 14918 | 100 |
| flow.complete_many batch=100 under 20000 | 106 | 9410 | 9135 | 11911 | 15479 | 18384 | 100 |
| flow.retry under 20000 | 214 | 4678 | 4705 | 5389 | 5606 | 5630 | 100 |
| flow.retry_many batch=100 under 20000 | 107 | 9387 | 8948 | 13174 | 15474 | 15552 | 100 |
| flow.fail under 20000 | 197 | 5080 | 4479 | 9338 | 12889 | 18129 | 100 |
| flow.fail_many batch=100 under 20000 | 112 | 8895 | 8328 | 12448 | 14327 | 14333 | 100 |
| flow.cancel under 20000 | 208 | 4802 | 4212 | 9003 | 12072 | 16519 | 100 |
| flow.cancel_many batch=100 under 20000 | 109 | 9141 | 8673 | 12742 | 14207 | 14569 | 100 |
| flow.rewind under 20000 | 189 | 5289 | 4831 | 8824 | 9377 | 10389 | 100 |
