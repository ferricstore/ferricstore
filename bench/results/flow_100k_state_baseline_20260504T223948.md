# Flow 100k State Baseline

- started_at: 2026-05-04T22:39:48.263609Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 64
- transition_many_batch: 100
- flow_lmdb_enabled: true
- claim_limits: 100
- beam_memory_before: 84137628
- beam_memory_after_seed: 466289072
- beam_memory_delta: 382151444

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 60 | 16786 | 13591 | 15380 | 211857 | 241411 | 200 |
| flow.get from 100000 | 78709 | 13 | 10 | 26 | 39 | 43 | 200 |
| flow.list count=100 from 100000 | 1520 | 658 | 617 | 853 | 985 | 1092 | 200 |
| flow.info over 100000 | 847 | 1180 | 21 | 29 | 57230 | 60597 | 200 |
| flow.history count=10 under 100000 | 27560 | 36 | 32 | 55 | 104 | 207 | 200 |
| flow.stuck count=100 under 100000 | 2518 | 397 | 343 | 598 | 808 | 970 | 200 |
| flow.claim_due limit=100 from 100000 | 58 | 17102 | 13673 | 18175 | 36196 | 327397 | 200 |
| flow.transition under 100000 | 70 | 14191 | 14064 | 15716 | 18378 | 19847 | 200 |
| flow.transition_many batch=100 under 100000 | 74 | 13563 | 13162 | 15525 | 19252 | 21060 | 200 |
| flow.complete under 100000 | 69 | 14397 | 14750 | 15546 | 19115 | 19782 | 200 |
| flow.retry under 100000 | 74 | 13536 | 13685 | 15361 | 17462 | 17927 | 200 |
| flow.fail under 100000 | 72 | 13919 | 13847 | 15501 | 18434 | 23861 | 200 |
| flow.cancel under 100000 | 72 | 13795 | 13806 | 16127 | 17901 | 21041 | 200 |
| flow.rewind under 100000 | 71 | 14018 | 13822 | 16515 | 19227 | 20255 | 200 |
