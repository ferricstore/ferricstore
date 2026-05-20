# Flow 100k State Baseline

- started_at: 2026-05-05T17:23:36.714845Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: false
- flow_lmdb_mode: off
- claim_limits: 100
- beam_memory_before: 84459621
- beam_memory_after_seed: 556921257
- beam_memory_delta: 472461636

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 105 | 9534 | 8916 | 15463 | 16620 | 20797 | 200 |
| flow.create_many batch=100 under 100000 | 93 | 10777 | 11399 | 14494 | 17472 | 25002 | 200 |
| flow.get from 100000 | 20257 | 49 | 34 | 56 | 122 | 2251 | 200 |
| flow.list count=100 from 100000 | 239 | 4176 | 4181 | 4428 | 4613 | 4712 | 200 |
| flow.info over 100000 | 177 | 5647 | 127 | 274 | 265279 | 301244 | 200 |
| flow.history count=10 under 100000 | 49068 | 20 | 19 | 25 | 55 | 91 | 200 |
| flow.stuck count=100 under 100000 | 467 | 2143 | 2112 | 2482 | 2728 | 3037 | 200 |
| flow.claim_due limit=100 from 100000 | 76 | 13106 | 9079 | 17279 | 146890 | 153789 | 200 |
| flow.transition under 100000 | 99 | 10080 | 9760 | 15898 | 16929 | 21112 | 200 |
| flow.transition_many batch=100 under 100000 | 115 | 8659 | 8403 | 11086 | 12530 | 13101 | 200 |
| flow.complete under 100000 | 109 | 9191 | 8585 | 14994 | 15839 | 16599 | 200 |
| flow.retry under 100000 | 99 | 10123 | 9561 | 16669 | 21929 | 28678 | 200 |
| flow.fail under 100000 | 109 | 9182 | 8583 | 14921 | 18211 | 43682 | 200 |
| flow.cancel under 100000 | 106 | 9454 | 8711 | 14990 | 16195 | 16756 | 200 |
| flow.rewind under 100000 | 104 | 9614 | 9276 | 15980 | 20009 | 39729 | 200 |
