# Flow 100k State Baseline

- started_at: 2026-05-05T11:55:46.089221Z
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
- beam_memory_before: 84348800
- beam_memory_after_seed: 554442700
- beam_memory_delta: 470093900

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 149 | 6727 | 7849 | 9602 | 11773 | 12596 | 200 |
| flow.create_many batch=100 under 100000 | 105 | 9520 | 8701 | 12781 | 13485 | 14226 | 200 |
| flow.get from 100000 | 18352 | 54 | 39 | 59 | 137 | 2531 | 200 |
| flow.list count=100 from 100000 | 260 | 3849 | 3827 | 4118 | 4242 | 4426 | 200 |
| flow.info over 100000 | 179 | 5594 | 98 | 147 | 266269 | 297647 | 200 |
| flow.history count=10 under 100000 | 51867 | 19 | 19 | 23 | 28 | 74 | 200 |
| flow.stuck count=100 under 100000 | 460 | 2175 | 2173 | 2458 | 2733 | 2812 | 200 |
| flow.claim_due limit=100 from 100000 | 100 | 9982 | 8997 | 13325 | 15469 | 16689 | 200 |
| flow.transition under 100000 | 152 | 6594 | 6190 | 9683 | 11841 | 17855 | 200 |
| flow.transition_many batch=100 under 100000 | 105 | 9567 | 9195 | 12386 | 15033 | 19924 | 200 |
| flow.complete under 100000 | 149 | 6694 | 5426 | 9946 | 13820 | 15091 | 200 |
| flow.retry under 100000 | 142 | 7053 | 5674 | 10433 | 15805 | 40037 | 200 |
| flow.fail under 100000 | 138 | 7231 | 8280 | 10333 | 12849 | 15852 | 200 |
| flow.cancel under 100000 | 132 | 7581 | 8347 | 10920 | 14568 | 14771 | 200 |
| flow.rewind under 100000 | 142 | 7024 | 8186 | 10303 | 12623 | 15104 | 200 |
