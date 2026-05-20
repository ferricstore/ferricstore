# Flow 100k State Baseline

- started_at: 2026-05-04T22:10:29.181080Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 64
- transition_many_batch: 100
- claim_limits: 100
- beam_memory_before: 96524942
- beam_memory_after_seed: 596446706
- beam_memory_delta: 499921764

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 225 | 4446 | 4330 | 5343 | 5686 | 16829 | 200 |
| flow.get from 100000 | 199402 | 5 | 4 | 12 | 23 | 31 | 200 |
| flow.list count=100 from 100000 | 3132 | 319 | 301 | 416 | 529 | 600 | 200 |
| flow.info over 100000 | 408 | 2449 | 21 | 36 | 77359 | 166896 | 200 |
| flow.history count=10 under 100000 | 48088 | 21 | 20 | 26 | 38 | 78 | 200 |
| flow.stuck count=100 under 100000 | 5174 | 193 | 183 | 241 | 333 | 448 | 200 |
| flow.claim_due limit=100 from 100000 | 79 | 12721 | 8917 | 10678 | 171093 | 174639 | 200 |
| flow.transition under 100000 | 198 | 5051 | 4967 | 6215 | 7621 | 8587 | 200 |
| flow.transition_many batch=100 under 100000 | 107 | 9338 | 9347 | 10541 | 10883 | 11937 | 200 |
| flow.complete under 100000 | 215 | 4650 | 4484 | 5493 | 8460 | 21089 | 200 |
| flow.retry under 100000 | 225 | 4446 | 4271 | 5574 | 6298 | 7352 | 200 |
| flow.fail under 100000 | 220 | 4537 | 4321 | 5676 | 8062 | 15583 | 200 |
| flow.cancel under 100000 | 123 | 8121 | 4441 | 9334 | 85443 | 93873 | 200 |
| flow.rewind under 100000 | 206 | 4861 | 4888 | 5443 | 5687 | 10803 | 200 |
