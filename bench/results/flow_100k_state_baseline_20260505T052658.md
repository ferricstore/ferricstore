# Flow 100k State Baseline

- started_at: 2026-05-05T05:26:58.046324Z
- backlog: 20000
- iterations: 100
- shards: 4
- partitions: 4
- seed_concurrency: 64
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: true
- claim_limits: 100
- beam_memory_before: 81381915
- beam_memory_after_seed: 169759577
- beam_memory_delta: 88377662

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 20000 | 78 | 12840 | 12797 | 13950 | 14637 | 15018 | 100 |
| flow.create_many batch=100 under 20000 | 70 | 14368 | 13898 | 19357 | 20016 | 22159 | 100 |
| flow.get from 20000 | 96339 | 10 | 9 | 20 | 29 | 37 | 100 |
| flow.list count=100 from 20000 | 1453 | 688 | 591 | 689 | 2274 | 7525 | 100 |
| flow.info over 20000 | 1170 | 854 | 23 | 41 | 24567 | 29831 | 100 |
| flow.history count=10 under 20000 | 48852 | 20 | 19 | 27 | 41 | 108 | 100 |
| flow.stuck count=100 under 20000 | 4790 | 209 | 202 | 262 | 314 | 324 | 100 |
| flow.claim_due limit=100 from 20000 | 75 | 13385 | 13204 | 14676 | 18539 | 21437 | 100 |
| flow.transition under 20000 | 74 | 13546 | 13536 | 15925 | 17747 | 17982 | 100 |
| flow.transition_many batch=100 under 20000 | 75 | 13299 | 13051 | 14851 | 16950 | 17940 | 100 |
| flow.complete under 20000 | 70 | 14240 | 13887 | 18982 | 21110 | 21367 | 100 |
| flow.retry under 20000 | 73 | 13702 | 13784 | 15119 | 15783 | 17429 | 100 |
| flow.fail under 20000 | 72 | 13950 | 13724 | 17335 | 21507 | 22953 | 100 |
| flow.cancel under 20000 | 50 | 19984 | 12921 | 15180 | 226837 | 230765 | 100 |
| flow.rewind under 20000 | 73 | 13751 | 13794 | 15425 | 15725 | 15949 | 100 |
