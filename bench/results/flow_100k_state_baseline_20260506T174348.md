# Flow 100k State Baseline

- started_at: 2026-05-06T17:43:48.009518Z
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
- beam_memory_before: 89867683
- beam_memory_after_seed: 456081461
- beam_memory_delta: 366213778

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 98 | 10163 | 9732 | 17074 | 19296 | 19445 | 100 |
| flow.create_many batch=100 under 100000 | 85 | 11747 | 10104 | 19909 | 21914 | 22806 | 100 |
| flow.get from 100000 | 89286 | 11 | 10 | 15 | 21 | 115 | 100 |
| flow.list count=100 from 100000 | 2360 | 424 | 403 | 568 | 759 | 1118 | 100 |
| flow.info over 100000 | 78616 | 13 | 11 | 22 | 26 | 69 | 100 |
| flow.history count=10 under 100000 | 5210 | 192 | 135 | 233 | 295 | 4723 | 100 |
| flow.stuck count=100 under 100000 | 8170 | 122 | 126 | 170 | 189 | 232 | 100 |
| flow.claim_due limit=100 from 100000 | 93 | 10700 | 9136 | 17894 | 23967 | 24510 | 100 |
| flow.transition under 100000 | 100 | 10044 | 9695 | 16014 | 18281 | 19466 | 100 |
| flow.transition_many batch=100 under 100000 | 75 | 13245 | 11781 | 20486 | 24734 | 25674 | 100 |
| flow.complete under 100000 | 90 | 11107 | 10426 | 18347 | 24382 | 24866 | 100 |
| flow.complete_many batch=100 under 100000 | 87 | 11551 | 10243 | 18117 | 19291 | 24028 | 100 |
| flow.retry under 100000 | 89 | 11192 | 10776 | 18121 | 21055 | 22973 | 100 |
| flow.retry_many batch=100 under 100000 | 85 | 11828 | 11022 | 17101 | 20919 | 21478 | 100 |
| flow.fail under 100000 | 82 | 12171 | 11191 | 20957 | 32080 | 59851 | 100 |
| flow.fail_many batch=100 under 100000 | 85 | 11831 | 10240 | 18971 | 21385 | 23201 | 100 |
| flow.cancel under 100000 | 91 | 11004 | 10748 | 20066 | 22880 | 29193 | 100 |
| flow.cancel_many batch=100 under 100000 | 101 | 9940 | 8721 | 13338 | 15757 | 15959 | 100 |
| flow.rewind under 100000 | 101 | 9950 | 9258 | 15802 | 20093 | 24488 | 100 |
