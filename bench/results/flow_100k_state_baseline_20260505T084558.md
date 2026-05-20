# Flow 100k State Baseline

- started_at: 2026-05-05T08:45:58.769571Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 64
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 10,100
- beam_memory_before: 81183205
- beam_memory_after_seed: 537458975
- beam_memory_delta: 456275770

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 117 | 8514 | 8605 | 15003 | 16998 | 17308 | 200 |
| flow.create_many batch=100 under 100000 | 98 | 10173 | 9154 | 13482 | 17727 | 19015 | 200 |
| flow.get from 100000 | 16427 | 61 | 46 | 81 | 110 | 2196 | 200 |
| flow.list count=100 from 100000 | 2396 | 417 | 285 | 355 | 4894 | 11466 | 200 |
| flow.info over 100000 | 320 | 3121 | 67 | 98 | 97436 | 213703 | 200 |
| flow.history count=10 under 100000 | 48146 | 21 | 19 | 34 | 45 | 65 | 200 |
| flow.stuck count=100 under 100000 | 5450 | 183 | 143 | 170 | 2166 | 2442 | 200 |
| flow.claim_due limit=10 from 100000 | 105 | 9537 | 9208 | 14498 | 17117 | 19256 | 200 |
| flow.claim_due limit=100 from 100000 | 74 | 13489 | 8763 | 17012 | 150551 | 224356 | 200 |
| flow.transition under 100000 | 117 | 8572 | 8597 | 15618 | 19890 | 20092 | 200 |
| flow.transition_many batch=100 under 100000 | 107 | 9369 | 8595 | 13192 | 16560 | 23655 | 200 |
| flow.complete under 100000 | 118 | 8472 | 8452 | 15539 | 18078 | 30206 | 200 |
| flow.retry under 100000 | 118 | 8461 | 8360 | 14761 | 16762 | 27387 | 200 |
| flow.fail under 100000 | 122 | 8196 | 8327 | 14611 | 19087 | 33187 | 200 |
| flow.cancel under 100000 | 127 | 7859 | 8178 | 13171 | 15840 | 19073 | 200 |
| flow.rewind under 100000 | 123 | 8140 | 8307 | 14385 | 18091 | 22642 | 200 |
