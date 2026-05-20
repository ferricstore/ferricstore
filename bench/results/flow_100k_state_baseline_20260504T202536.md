# Flow 100k State Baseline

- started_at: 2026-05-04T20:25:36.745240Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- claim_limits: 100
- beam_memory_before: 84559236
- beam_memory_after_seed: 569071224
- beam_memory_delta: 484511988

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 226 | 4426 | 4341 | 5309 | 5511 | 8463 | 200 |
| flow.get from 100000 | 206186 | 5 | 4 | 10 | 21 | 27 | 200 |
| flow.list count=100 from 100000 | 2855 | 350 | 331 | 465 | 568 | 631 | 200 |
| flow.info over 100000 | 240 | 4171 | 23 | 38 | 250393 | 255150 | 200 |
| flow.history count=10 under 100000 | 28588 | 35 | 32 | 56 | 87 | 267 | 200 |
| flow.stuck count=100 under 100000 | 4975 | 201 | 184 | 275 | 380 | 443 | 200 |
| flow.claim_due limit=100 from 100000 | 83 | 12053 | 8580 | 11265 | 163084 | 168927 | 200 |
| flow.transition under 100000 | 211 | 4749 | 4776 | 5450 | 5565 | 5730 | 200 |
| flow.complete under 100000 | 202 | 4939 | 4807 | 5467 | 5693 | 35917 | 200 |
| flow.retry under 100000 | 227 | 4410 | 4325 | 5397 | 5520 | 5710 | 200 |
| flow.fail under 100000 | 228 | 4394 | 4324 | 5323 | 5658 | 9913 | 200 |
| flow.cancel under 100000 | 218 | 4582 | 4444 | 5544 | 5934 | 10446 | 200 |
| flow.rewind under 100000 | 19 | 52623 | 56313 | 60159 | 232473 | 235928 | 200 |
