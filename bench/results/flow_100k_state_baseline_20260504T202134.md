# Flow 100k State Baseline

- started_at: 2026-05-04T20:21:34.549366Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- claim_limits: 100
- beam_memory_before: 83717128
- beam_memory_after_seed: 582179196
- beam_memory_delta: 498462068

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 201 | 4982 | 4948 | 6106 | 6461 | 7488 | 200 |
| flow.get from 100000 | 221729 | 5 | 4 | 9 | 19 | 27 | 200 |
| flow.list count=100 from 100000 | 3121 | 320 | 303 | 404 | 503 | 578 | 200 |
| flow.info over 100000 | 230 | 4340 | 21 | 41 | 252982 | 270500 | 200 |
| flow.history count=10 under 100000 | 59630 | 17 | 16 | 22 | 30 | 50 | 200 |
| flow.stuck count=100 under 100000 | 6104 | 164 | 161 | 186 | 235 | 293 | 200 |
| flow.claim_due limit=100 from 100000 | 106 | 9452 | 9414 | 10573 | 13165 | 14272 | 200 |
| flow.transition under 100000 | 204 | 4898 | 4831 | 6213 | 8104 | 9240 | 200 |
| flow.complete under 100000 | 218 | 4587 | 4438 | 5626 | 6693 | 8563 | 200 |
| flow.retry under 100000 | 218 | 4588 | 4429 | 5541 | 9335 | 10554 | 200 |
| flow.fail under 100000 | 129 | 7732 | 4501 | 8879 | 76771 | 78760 | 200 |
| flow.cancel under 100000 | 205 | 4872 | 4777 | 5541 | 5706 | 19566 | 200 |
| flow.rewind under 100000 | 20 | 48813 | 56772 | 59440 | 61901 | 64213 | 200 |
