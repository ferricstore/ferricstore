# Flow 100k State Baseline

- started_at: 2026-05-06T18:40:42.515745Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- create_many_batch: 100
- transition_many_batch: 100
- terminal_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 10,100
- beam_memory_before: 82386033
- beam_memory_after_seed: 443248855
- beam_memory_delta: 360862822

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 213 | 4699 | 4740 | 5370 | 5467 | 5508 | 200 |
| flow.create_many batch=100 under 100000 | 106 | 9412 | 9391 | 10462 | 10602 | 11018 | 200 |
| flow.get from 100000 | 226501 | 4 | 4 | 7 | 16 | 34 | 200 |
| flow.list count=100 from 100000 | 3824 | 262 | 250 | 317 | 371 | 405 | 200 |
| flow.info over 100000 | 96479 | 10 | 9 | 18 | 29 | 70 | 200 |
| flow.history count=10 under 100000 | 6868 | 146 | 97 | 171 | 255 | 7786 | 200 |
| flow.stuck count=100 under 100000 | 5887 | 170 | 165 | 214 | 278 | 328 | 200 |
| flow.claim_due limit=10 from 100000 | 137 | 7322 | 7657 | 10050 | 11857 | 25322 | 200 |
| flow.claim_due limit=100 from 100000 | 110 | 9061 | 8905 | 10170 | 10373 | 15261 | 200 |
| flow.transition under 100000 | 124 | 8088 | 4432 | 9181 | 78347 | 146045 | 200 |
| flow.transition_many batch=100 under 100000 | 107 | 9328 | 9331 | 10469 | 10715 | 11681 | 200 |
| flow.complete under 100000 | 118 | 8463 | 8566 | 15861 | 16576 | 20044 | 200 |
| flow.complete_many batch=100 under 100000 | 95 | 10490 | 9547 | 14534 | 15061 | 22087 | 200 |
| flow.retry under 100000 | 224 | 4456 | 4364 | 5341 | 5651 | 5721 | 200 |
| flow.retry_many batch=100 under 100000 | 111 | 9013 | 8624 | 10460 | 14161 | 15650 | 200 |
| flow.fail under 100000 | 115 | 8728 | 8635 | 14806 | 20077 | 22111 | 200 |
| flow.fail_many batch=100 under 100000 | 73 | 13693 | 9523 | 14179 | 147663 | 302273 | 200 |
| flow.cancel under 100000 | 114 | 8747 | 9199 | 15197 | 16851 | 17775 | 200 |
| flow.cancel_many batch=100 under 100000 | 94 | 10675 | 9655 | 15387 | 17789 | 21493 | 200 |
| flow.rewind under 100000 | 121 | 8298 | 8478 | 15343 | 17062 | 31505 | 200 |
