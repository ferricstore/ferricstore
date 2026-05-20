# Flow 100k State Baseline

- started_at: 2026-05-05T06:23:46.174461Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 64
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 100
- beam_memory_before: 84647068
- beam_memory_after_seed: 526248872
- beam_memory_delta: 441601804

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 113 | 8820 | 8460 | 15805 | 16974 | 18946 | 200 |
| flow.create_many batch=100 under 100000 | 72 | 13863 | 9598 | 17163 | 157070 | 228120 | 200 |
| flow.get from 100000 | 14224 | 70 | 52 | 93 | 143 | 2570 | 200 |
| flow.list count=100 from 100000 | 2354 | 425 | 324 | 388 | 5149 | 5875 | 200 |
| flow.info over 100000 | 447 | 2239 | 72 | 110 | 103851 | 115083 | 200 |
| flow.history count=10 under 100000 | 47226 | 21 | 23 | 30 | 34 | 49 | 200 |
| flow.stuck count=100 under 100000 | 4114 | 243 | 172 | 234 | 3195 | 4021 | 200 |
| flow.claim_due limit=100 from 100000 | 96 | 10371 | 9021 | 16897 | 18650 | 22079 | 200 |
| flow.transition under 100000 | 106 | 9449 | 8838 | 18060 | 20055 | 20884 | 200 |
| flow.transition_many batch=100 under 100000 | 100 | 9968 | 8795 | 16637 | 18184 | 22263 | 200 |
| flow.complete under 100000 | 106 | 9421 | 8532 | 19974 | 23014 | 24270 | 200 |
| flow.retry under 100000 | 81 | 12358 | 8566 | 17809 | 149407 | 151050 | 200 |
| flow.fail under 100000 | 107 | 9379 | 8570 | 17043 | 21081 | 24069 | 200 |
| flow.cancel under 100000 | 108 | 9233 | 8488 | 16531 | 19393 | 20799 | 200 |
| flow.rewind under 100000 | 103 | 9705 | 9078 | 17059 | 21309 | 22074 | 200 |
