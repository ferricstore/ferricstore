# Flow 100k State Baseline

- started_at: 2026-05-04T21:51:00.744607Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 64
- claim_limits: 100
- beam_memory_before: 83419367
- beam_memory_after_seed: 584524467
- beam_memory_delta: 501105100

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 95 | 10564 | 5852 | 11460 | 86758 | 373521 | 200 |
| flow.get from 100000 | 97087 | 10 | 8 | 23 | 61 | 76 | 200 |
| flow.list count=100 from 100000 | 2597 | 385 | 355 | 540 | 999 | 1200 | 200 |
| flow.info over 100000 | 390 | 2561 | 22 | 36 | 81731 | 174417 | 200 |
| flow.history count=10 under 100000 | 47996 | 21 | 20 | 32 | 47 | 89 | 200 |
| flow.stuck count=100 under 100000 | 4853 | 206 | 195 | 301 | 382 | 414 | 200 |
| flow.claim_due limit=100 from 100000 | 113 | 8838 | 8820 | 10009 | 10161 | 10256 | 200 |
| flow.transition under 100000 | 221 | 4526 | 4528 | 5295 | 5620 | 5668 | 200 |
| flow.complete under 100000 | 225 | 4446 | 4312 | 5386 | 7223 | 8250 | 200 |
| flow.retry under 100000 | 197 | 5072 | 4899 | 5986 | 9618 | 21781 | 200 |
| flow.fail under 100000 | 195 | 5116 | 5013 | 6208 | 7321 | 8693 | 200 |
| flow.cancel under 100000 | 201 | 4975 | 4754 | 6186 | 14296 | 20440 | 200 |
| flow.rewind under 100000 | 211 | 4733 | 4644 | 5629 | 7502 | 10767 | 200 |
