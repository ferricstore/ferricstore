# Flow 100k State Baseline

- started_at: 2026-05-05T05:59:51.114600Z
- backlog: 20000
- iterations: 100
- shards: 4
- partitions: 4
- seed_concurrency: 64
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 100
- beam_memory_before: 83993317
- beam_memory_after_seed: 175615365
- beam_memory_delta: 91622048

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 20000 | 103 | 9697 | 9448 | 15519 | 16316 | 16656 | 100 |
| flow.create_many batch=100 under 20000 | 95 | 10483 | 9546 | 13859 | 14876 | 18128 | 100 |
| flow.get from 20000 | 14368 | 70 | 44 | 97 | 141 | 2070 | 100 |
| flow.list count=100 from 20000 | 2163 | 462 | 291 | 363 | 4568 | 5021 | 100 |
| flow.info over 20000 | 568 | 1761 | 64 | 103 | 37794 | 57821 | 100 |
| flow.history count=10 under 20000 | 54318 | 18 | 17 | 24 | 41 | 103 | 100 |
| flow.stuck count=100 under 20000 | 8197 | 122 | 74 | 118 | 1201 | 1246 | 100 |
| flow.claim_due limit=100 from 20000 | 98 | 10215 | 9100 | 13684 | 16509 | 19613 | 100 |
| flow.transition under 20000 | 112 | 8919 | 8569 | 13934 | 15824 | 16491 | 100 |
| flow.transition_many batch=100 under 20000 | 66 | 15197 | 14957 | 24429 | 26537 | 26657 | 100 |
| flow.complete under 20000 | 79 | 12612 | 12545 | 20106 | 21335 | 23152 | 100 |
| flow.retry under 20000 | 82 | 12141 | 12666 | 17323 | 20868 | 21533 | 100 |
| flow.fail under 20000 | 114 | 8799 | 8435 | 13646 | 15620 | 15655 | 100 |
| flow.cancel under 20000 | 105 | 9509 | 9086 | 15798 | 17074 | 17299 | 100 |
| flow.rewind under 20000 | 105 | 9479 | 9081 | 14903 | 15786 | 17054 | 100 |
