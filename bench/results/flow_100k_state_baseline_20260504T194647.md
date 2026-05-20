# Flow 100k State Baseline

- started_at: 2026-05-04T19:46:47.861897Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- claim_limits: 10,100
- beam_memory_before: 80963009
- beam_memory_after_seed: 580530831
- beam_memory_delta: 499567822

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 225 | 4436 | 4358 | 5509 | 5935 | 8337 | 200 |
| flow.get from 100000 | 362319 | 3 | 2 | 6 | 13 | 15 | 200 |
| flow.list count=100 from 100000 | 3407 | 294 | 262 | 303 | 332 | 6330 | 200 |
| flow.info over 100000 | 227 | 4408 | 22 | 29 | 257714 | 273504 | 200 |
| flow.history count=10 under 100000 | 42 | 23957 | 25280 | 34465 | 38442 | 56308 | 200 |
| flow.stuck count=100 under 100000 | 156740 | 6 | 5 | 11 | 15 | 36 | 200 |
| flow.claim_due limit=10 from 100000 | 145 | 6902 | 7620 | 9354 | 9832 | 10392 | 200 |
| flow.claim_due limit=100 from 100000 | 77 | 12954 | 9117 | 12610 | 191428 | 222182 | 200 |
| flow.transition under 100000 | 96 | 10423 | 10137 | 13678 | 15220 | 27971 | 200 |
| flow.complete under 100000 | 69 | 14427 | 13988 | 15705 | 23053 | 39973 | 200 |
| flow.retry under 100000 | 76 | 13163 | 12915 | 14570 | 15684 | 15947 | 200 |
| flow.fail under 100000 | 74 | 13602 | 13216 | 15420 | 21289 | 40094 | 200 |
| flow.cancel under 100000 | 110 | 9069 | 8939 | 10486 | 11130 | 14770 | 200 |
| flow.rewind under 100000 | 16 | 61900 | 63289 | 70665 | 83437 | 534194 | 200 |
