# Flow 100k State Baseline

- started_at: 2026-05-04T22:25:53.691655Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 64
- transition_many_batch: 100
- flow_lmdb_enabled: true
- claim_limits: 100
- beam_memory_before: 86231441
- beam_memory_after_seed: 462364645
- beam_memory_delta: 376133204

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 67 | 14856 | 14839 | 18143 | 19770 | 22698 | 200 |
| flow.get from 100000 | 55279 | 18 | 15 | 36 | 49 | 65 | 200 |
| flow.list count=100 from 100000 | 1462 | 684 | 625 | 979 | 1162 | 1297 | 200 |
| flow.info over 100000 | 371 | 2697 | 22 | 37 | 133964 | 134037 | 200 |
| flow.history count=10 under 100000 | 80257 | 12 | 12 | 16 | 22 | 54 | 200 |
| flow.stuck count=100 under 100000 | 2657 | 376 | 342 | 554 | 640 | 770 | 200 |
| flow.claim_due limit=100 from 100000 | 66 | 15121 | 14820 | 19377 | 20987 | 22922 | 200 |
| flow.transition under 100000 | 72 | 13986 | 13857 | 15548 | 18246 | 25920 | 200 |
| flow.transition_many batch=100 under 100000 | 74 | 13581 | 13154 | 15394 | 18770 | 33640 | 200 |
| flow.complete under 100000 | 71 | 14013 | 13818 | 15415 | 18526 | 26902 | 200 |
| flow.retry under 100000 | 60 | 16777 | 13624 | 16558 | 203013 | 230401 | 200 |
| flow.fail under 100000 | 73 | 13763 | 13781 | 15391 | 18321 | 20529 | 200 |
| flow.cancel under 100000 | 58 | 17335 | 13762 | 16896 | 226289 | 234995 | 200 |
| flow.rewind under 100000 | 73 | 13744 | 13750 | 15302 | 15878 | 35384 | 200 |
