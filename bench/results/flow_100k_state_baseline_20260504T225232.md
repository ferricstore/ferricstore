# Flow 100k State Baseline

- started_at: 2026-05-04T22:52:32.875833Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 64
- transition_many_batch: 100
- flow_lmdb_enabled: true
- claim_limits: 100
- beam_memory_before: 84165232
- beam_memory_after_seed: 463632476
- beam_memory_delta: 379467244

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 73 | 13757 | 13747 | 15380 | 19342 | 19799 | 200 |
| flow.get from 100000 | 49529 | 20 | 17 | 37 | 52 | 80 | 200 |
| flow.list count=100 from 100000 | 1431 | 699 | 632 | 1086 | 1348 | 1461 | 200 |
| flow.info over 100000 | 503 | 1986 | 22 | 39 | 60562 | 136259 | 200 |
| flow.history count=10 under 100000 | 35702 | 28 | 26 | 41 | 52 | 110 | 200 |
| flow.stuck count=100 under 100000 | 1642 | 609 | 577 | 822 | 1010 | 1286 | 200 |
| flow.claim_due limit=100 from 100000 | 69 | 14426 | 13950 | 18123 | 20845 | 23352 | 200 |
| flow.transition under 100000 | 73 | 13779 | 13711 | 16433 | 20797 | 26934 | 200 |
| flow.transition_many batch=100 under 100000 | 67 | 14833 | 14775 | 16098 | 19635 | 29088 | 200 |
| flow.complete under 100000 | 73 | 13607 | 13641 | 15495 | 18860 | 22098 | 200 |
| flow.retry under 100000 | 73 | 13769 | 13752 | 15546 | 18440 | 20377 | 200 |
| flow.fail under 100000 | 73 | 13698 | 13761 | 15276 | 17355 | 31296 | 200 |
| flow.cancel under 100000 | 69 | 14509 | 14635 | 16911 | 19462 | 20310 | 200 |
| flow.rewind under 100000 | 73 | 13766 | 13749 | 15410 | 18113 | 18511 | 200 |
