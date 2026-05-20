# Flow 100k State Baseline

- started_at: 2026-05-04T19:50:33.923319Z
- backlog: 1000
- iterations: 10
- shards: 4
- partitions: 4
- seed_concurrency: 8
- claim_limits: 10,20
- beam_memory_before: 83339386
- beam_memory_after_seed: 90315026
- beam_memory_delta: 6975640

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 1000 | 209 | 4793 | 4859 | 5245 | 5245 | 5245 | 10 |
| flow.get from 1000 | 123457 | 8 | 3 | 48 | 48 | 48 | 10 |
| flow.list count=100 from 1000 | 2484 | 403 | 376 | 517 | 517 | 517 | 10 |
| flow.info over 1000 | 1793 | 558 | 49 | 1831 | 1831 | 1831 | 10 |
| flow.history count=10 under 1000 | 41322 | 24 | 19 | 63 | 63 | 63 | 10 |
| flow.stuck count=100 under 1000 | 44248 | 23 | 18 | 54 | 54 | 54 | 10 |
| flow.claim_due limit=10 from 1000 | 113 | 8858 | 9573 | 10403 | 10403 | 10403 | 10 |
| flow.claim_due limit=20 from 1000 | 114 | 8741 | 8701 | 10497 | 10497 | 10497 | 10 |
| flow.transition under 1000 | 238 | 4199 | 4167 | 4643 | 4643 | 4643 | 10 |
| flow.complete under 1000 | 222 | 4508 | 4245 | 5276 | 5276 | 5276 | 10 |
| flow.retry under 1000 | 237 | 4216 | 4113 | 4613 | 4613 | 4613 | 10 |
| flow.fail under 1000 | 233 | 4286 | 4191 | 4810 | 4810 | 4810 | 10 |
| flow.cancel under 1000 | 218 | 4594 | 4531 | 5336 | 5336 | 5336 | 10 |
| flow.rewind under 1000 | 117 | 8552 | 9262 | 10183 | 10183 | 10183 | 10 |
