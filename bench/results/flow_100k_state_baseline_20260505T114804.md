# Flow 100k State Baseline

- started_at: 2026-05-05T11:48:04.896777Z
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
- beam_memory_before: 93486038
- beam_memory_after_seed: 673013154
- beam_memory_delta: 579527116

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 83 | 12029 | 10962 | 20281 | 28148 | 31104 | 200 |
| flow.create_many batch=100 under 100000 | 88 | 11336 | 10800 | 17047 | 22810 | 25783 | 200 |
| flow.get from 100000 | 13194 | 76 | 56 | 95 | 141 | 3009 | 200 |
| flow.list count=100 from 100000 | 208 | 4800 | 4739 | 5526 | 6955 | 7489 | 200 |
| flow.info over 100000 | 168 | 5941 | 232 | 357 | 218456 | 363931 | 200 |
| flow.history count=10 under 100000 | 13116 | 76 | 72 | 136 | 245 | 569 | 200 |
| flow.stuck count=100 under 100000 | 426 | 2347 | 2145 | 2857 | 6035 | 14822 | 200 |
| flow.claim_due limit=100 from 100000 | 93 | 10696 | 9966 | 16514 | 19042 | 22290 | 200 |
| flow.transition under 100000 | 100 | 9957 | 8925 | 17087 | 22253 | 39525 | 200 |
| flow.transition_many batch=100 under 100000 | 95 | 10569 | 9709 | 15876 | 17061 | 18577 | 200 |
| flow.complete under 100000 | 102 | 9773 | 9137 | 16296 | 20457 | 43192 | 200 |
| flow.retry under 100000 | 101 | 9916 | 9339 | 17103 | 21534 | 29930 | 200 |
| flow.fail under 100000 | 103 | 9673 | 9441 | 15973 | 16951 | 17893 | 200 |
| flow.cancel under 100000 | 102 | 9826 | 9558 | 15866 | 18265 | 23063 | 200 |
| flow.rewind under 100000 | 104 | 9571 | 9527 | 16921 | 20090 | 20453 | 200 |
