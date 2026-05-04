# Flow 100k State Baseline

- started_at: 2026-05-04T19:59:27.183768Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- claim_limits: 100
- beam_memory_before: 84983519
- beam_memory_after_seed: 578297611
- beam_memory_delta: 493314092

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 204 | 4896 | 4432 | 8609 | 11196 | 15958 | 200 |
| flow.get from 100000 | 68942 | 15 | 13 | 27 | 72 | 106 | 200 |
| flow.list count=100 from 100000 | 2583 | 387 | 336 | 614 | 1101 | 1201 | 200 |
| flow.info over 100000 | 407 | 2456 | 21 | 48 | 73565 | 176665 | 200 |
| flow.history count=10 under 100000 | 21265 | 47 | 41 | 85 | 193 | 231 | 200 |
| flow.stuck count=100 under 100000 | 4240 | 236 | 214 | 342 | 460 | 628 | 200 |
| flow.claim_due limit=100 from 100000 | 111 | 9019 | 8944 | 9956 | 12149 | 18257 | 200 |
| flow.transition under 100000 | 198 | 5045 | 4743 | 8152 | 9740 | 11563 | 200 |
| flow.complete under 100000 | 135 | 7429 | 7703 | 11794 | 13158 | 15407 | 200 |
| flow.retry under 100000 | 174 | 5746 | 4912 | 10698 | 14008 | 18002 | 200 |
| flow.fail under 100000 | 114 | 8756 | 9162 | 12769 | 15680 | 15803 | 200 |
| flow.cancel under 100000 | 136 | 7351 | 6524 | 11873 | 15845 | 21887 | 200 |
| flow.rewind under 100000 | 30 | 33616 | 37005 | 41626 | 154998 | 168991 | 200 |
