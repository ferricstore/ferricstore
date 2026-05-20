# Flow 100k State Baseline

- started_at: 2026-05-06T21:34:10.414475Z
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
- beam_memory_before: 88271354
- beam_memory_after_seed: 162008613
- beam_memory_delta: 73737259

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 20000 | 225 | 4448 | 4365 | 5258 | 5676 | 5890 | 100 |
| flow.create_many batch=100 under 20000 | 109 | 9209 | 9025 | 10370 | 11356 | 15964 | 100 |
| flow.get from 20000 | 291545 | 3 | 3 | 5 | 11 | 32 | 100 |
| flow.list count=100 from 20000 | 4244 | 236 | 232 | 278 | 324 | 367 | 100 |
| flow.info over 20000 | 190476 | 5 | 5 | 8 | 10 | 21 | 100 |
| flow.history count=10 under 20000 | 6467 | 155 | 140 | 213 | 551 | 584 | 100 |
| flow.history include_cold count=10 under 20000 | 18362 | 54 | 52 | 71 | 93 | 99 | 100 |
| flow.history cold_consistent count=10 under 20000 | 20012 | 50 | 49 | 60 | 65 | 66 | 100 |
| flow.stuck count=100 under 20000 | 12739 | 79 | 75 | 109 | 127 | 159 | 100 |
| flow.claim_due limit=100 from 20000 | 112 | 8916 | 8874 | 10054 | 10172 | 10293 | 100 |
| flow.transition under 20000 | 215 | 4648 | 4678 | 5130 | 5599 | 5642 | 100 |
| flow.transition_many batch=100 under 20000 | 115 | 8689 | 8305 | 9959 | 10326 | 11688 | 100 |
| flow.complete under 20000 | 118 | 8441 | 8823 | 15499 | 16938 | 16990 | 100 |
| flow.complete_many batch=100 under 20000 | 93 | 10733 | 9885 | 14468 | 14668 | 14861 | 100 |
| flow.retry under 20000 | 213 | 4698 | 4723 | 5429 | 5511 | 5525 | 100 |
| flow.retry_many batch=100 under 20000 | 109 | 9146 | 9026 | 11060 | 11819 | 12299 | 100 |
| flow.fail under 20000 | 116 | 8642 | 8499 | 16674 | 17274 | 20301 | 100 |
| flow.fail_many batch=100 under 20000 | 94 | 10643 | 9857 | 15054 | 17336 | 19736 | 100 |
| flow.cancel under 20000 | 132 | 7573 | 7828 | 14852 | 17065 | 17140 | 100 |
| flow.cancel_many batch=100 under 20000 | 93 | 10809 | 9885 | 15582 | 16296 | 18192 | 100 |
| flow.rewind under 20000 | 111 | 9033 | 9069 | 16245 | 18031 | 20430 | 100 |
