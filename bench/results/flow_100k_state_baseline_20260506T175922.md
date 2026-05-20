# Flow 100k State Baseline

- started_at: 2026-05-06T17:59:22.326433Z
- backlog: 100000
- iterations: 100
- shards: 4
- partitions: 4
- seed_concurrency: 32
- create_many_batch: 100
- transition_many_batch: 100
- terminal_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 100
- beam_memory_before: 84835163
- beam_memory_after_seed: 452754501
- beam_memory_delta: 367919338

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 86 | 11693 | 10585 | 18492 | 23447 | 23938 | 100 |
| flow.create_many batch=100 under 100000 | 79 | 12594 | 12112 | 18751 | 20513 | 23409 | 100 |
| flow.get from 100000 | 114286 | 9 | 7 | 14 | 39 | 65 | 100 |
| flow.list count=100 from 100000 | 2489 | 402 | 389 | 566 | 667 | 709 | 100 |
| flow.info over 100000 | 77882 | 13 | 12 | 20 | 25 | 46 | 100 |
| flow.history count=10 under 100000 | 2636 | 379 | 227 | 501 | 560 | 12607 | 100 |
| flow.stuck count=100 under 100000 | 4423 | 226 | 194 | 425 | 865 | 885 | 100 |
| flow.claim_due limit=100 from 100000 | 84 | 11951 | 11395 | 17511 | 21901 | 21980 | 100 |
| flow.transition under 100000 | 98 | 10163 | 10029 | 16582 | 21736 | 22141 | 100 |
| flow.transition_many batch=100 under 100000 | 94 | 10586 | 9598 | 16565 | 17503 | 17845 | 100 |
| flow.complete under 100000 | 63 | 15817 | 9471 | 17553 | 156212 | 241050 | 100 |
| flow.complete_many batch=100 under 100000 | 92 | 10833 | 9857 | 14359 | 14928 | 23345 | 100 |
| flow.retry under 100000 | 107 | 9323 | 9327 | 15696 | 17046 | 20932 | 100 |
| flow.retry_many batch=100 under 100000 | 103 | 9746 | 8809 | 14302 | 17731 | 22700 | 100 |
| flow.fail under 100000 | 93 | 10768 | 9850 | 18437 | 21043 | 24428 | 100 |
| flow.fail_many batch=100 under 100000 | 99 | 10145 | 9013 | 13421 | 14088 | 15701 | 100 |
| flow.cancel under 100000 | 94 | 10610 | 10241 | 17576 | 22234 | 25699 | 100 |
| flow.cancel_many batch=100 under 100000 | 89 | 11189 | 10018 | 14722 | 15247 | 15667 | 100 |
| flow.rewind under 100000 | 110 | 9083 | 8682 | 16700 | 17479 | 20709 | 100 |
