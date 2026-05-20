# Flow 100k State Baseline

- started_at: 2026-05-06T17:52:05.767518Z
- backlog: 100000
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
- beam_memory_before: 85535111
- beam_memory_after_seed: 451519249
- beam_memory_delta: 365984138

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 125 | 8006 | 8334 | 14912 | 15918 | 16198 | 100 |
| flow.create_many batch=100 under 100000 | 94 | 10635 | 9635 | 14167 | 17352 | 17697 | 100 |
| flow.get from 100000 | 193798 | 5 | 4 | 8 | 19 | 37 | 100 |
| flow.list count=100 from 100000 | 3357 | 298 | 288 | 371 | 414 | 468 | 100 |
| flow.info over 100000 | 98619 | 10 | 8 | 15 | 22 | 35 | 100 |
| flow.history count=10 under 100000 | 7371 | 136 | 93 | 187 | 294 | 3013 | 100 |
| flow.stuck count=100 under 100000 | 8411 | 119 | 114 | 147 | 180 | 209 | 100 |
| flow.claim_due limit=100 from 100000 | 83 | 12097 | 12233 | 16583 | 17463 | 17694 | 100 |
| flow.transition under 100000 | 109 | 9197 | 9542 | 16991 | 17582 | 21755 | 100 |
| flow.transition_many batch=100 under 100000 | 81 | 12352 | 11772 | 17954 | 23235 | 24248 | 100 |
| flow.complete under 100000 | 128 | 7796 | 8025 | 13355 | 15795 | 16306 | 100 |
| flow.complete_many batch=100 under 100000 | 93 | 10772 | 9968 | 15027 | 15866 | 16982 | 100 |
| flow.retry under 100000 | 135 | 7397 | 8083 | 12797 | 13242 | 16149 | 100 |
| flow.retry_many batch=100 under 100000 | 85 | 11778 | 11148 | 17385 | 19942 | 21353 | 100 |
| flow.fail under 100000 | 111 | 9027 | 9150 | 15834 | 17433 | 19981 | 100 |
| flow.fail_many batch=100 under 100000 | 102 | 9813 | 8629 | 13277 | 16904 | 17827 | 100 |
| flow.cancel under 100000 | 125 | 8007 | 8300 | 13709 | 17848 | 19992 | 100 |
| flow.cancel_many batch=100 under 100000 | 98 | 10216 | 9168 | 14148 | 15705 | 18620 | 100 |
| flow.rewind under 100000 | 64 | 15721 | 8675 | 61233 | 155250 | 227123 | 100 |
