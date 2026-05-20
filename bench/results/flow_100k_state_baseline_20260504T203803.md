# Flow 100k State Baseline

- started_at: 2026-05-04T20:38:03.158427Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- claim_limits: 100
- beam_memory_before: 83346174
- beam_memory_after_seed: 564594130
- beam_memory_delta: 481247956

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 217 | 4605 | 4522 | 5580 | 5783 | 5938 | 200 |
| flow.get from 100000 | 158479 | 6 | 5 | 13 | 27 | 32 | 200 |
| flow.list count=100 from 100000 | 2762 | 362 | 322 | 553 | 708 | 765 | 200 |
| flow.info over 100000 | 244 | 4093 | 21 | 29 | 242461 | 248532 | 200 |
| flow.history count=10 under 100000 | 61331 | 16 | 15 | 22 | 35 | 62 | 200 |
| flow.stuck count=100 under 100000 | 4659 | 215 | 202 | 288 | 469 | 501 | 200 |
| flow.claim_due limit=100 from 100000 | 111 | 9016 | 8712 | 10077 | 12341 | 33157 | 200 |
| flow.transition under 100000 | 218 | 4577 | 4417 | 5328 | 5604 | 9268 | 200 |
| flow.complete under 100000 | 215 | 4645 | 4322 | 5355 | 8352 | 44736 | 200 |
| flow.retry under 100000 | 231 | 4320 | 4241 | 5135 | 5402 | 8907 | 200 |
| flow.fail under 100000 | 219 | 4563 | 4388 | 5449 | 6652 | 9648 | 200 |
| flow.cancel under 100000 | 223 | 4494 | 4311 | 6198 | 7183 | 8753 | 200 |
| flow.rewind under 100000 | 21 | 47792 | 55312 | 58500 | 60839 | 61114 | 200 |
