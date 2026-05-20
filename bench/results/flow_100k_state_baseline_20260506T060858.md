# Flow 100k State Baseline

- started_at: 2026-05-06T06:08:58.009967Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 10,100
- beam_memory_before: 85943151
- beam_memory_after_seed: 455841727
- beam_memory_delta: 369898576

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 106 | 9476 | 9178 | 14867 | 16583 | 17203 | 200 |
| flow.create_many batch=100 under 100000 | 89 | 11258 | 11599 | 17038 | 19022 | 21071 | 200 |
| flow.get from 100000 | 336134 | 3 | 2 | 5 | 17 | 61 | 200 |
| flow.list count=100 from 100000 | 3794 | 264 | 255 | 334 | 388 | 408 | 200 |
| flow.info over 100000 | 346 | 2891 | 53 | 114 | 140559 | 143879 | 200 |
| flow.history count=10 under 100000 | 10063 | 99 | 76 | 144 | 254 | 2865 | 200 |
| flow.stuck count=100 under 100000 | 9788 | 102 | 99 | 121 | 161 | 317 | 200 |
| flow.claim_due limit=10 from 100000 | 108 | 9293 | 8689 | 15716 | 19870 | 20081 | 200 |
| flow.claim_due limit=100 from 100000 | 105 | 9564 | 8572 | 13092 | 16736 | 16895 | 200 |
| flow.transition under 100000 | 108 | 9274 | 8617 | 13915 | 15676 | 18793 | 200 |
| flow.transition_many batch=100 under 100000 | 89 | 11222 | 9990 | 17023 | 17740 | 18507 | 200 |
| flow.complete under 100000 | 111 | 9035 | 8468 | 13877 | 16224 | 29481 | 200 |
| flow.retry under 100000 | 105 | 9534 | 8837 | 14721 | 16764 | 29387 | 200 |
| flow.fail under 100000 | 100 | 9974 | 9724 | 15866 | 18803 | 19659 | 200 |
| flow.cancel under 100000 | 103 | 9677 | 9697 | 15099 | 20002 | 28970 | 200 |
| flow.rewind under 100000 | 105 | 9525 | 9148 | 15047 | 16504 | 18095 | 200 |
