# Flow 100k State Baseline

- started_at: 2026-05-05T06:11:41.164053Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 64
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 100
- beam_memory_before: 84113317
- beam_memory_after_seed: 520044969
- beam_memory_delta: 435931652

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 119 | 8372 | 8617 | 14736 | 16534 | 16799 | 200 |
| flow.create_many batch=100 under 100000 | 97 | 10350 | 9334 | 14742 | 17390 | 19147 | 200 |
| flow.get from 100000 | 15966 | 63 | 46 | 83 | 161 | 2259 | 200 |
| flow.list count=100 from 100000 | 2591 | 386 | 304 | 359 | 4226 | 4728 | 200 |
| flow.info over 100000 | 323 | 3100 | 69 | 119 | 96540 | 212257 | 200 |
| flow.history count=10 under 100000 | 52576 | 19 | 18 | 25 | 35 | 81 | 200 |
| flow.stuck count=100 under 100000 | 4092 | 244 | 181 | 226 | 2739 | 4707 | 200 |
| flow.claim_due limit=100 from 100000 | 103 | 9695 | 8711 | 14898 | 16776 | 24848 | 200 |
| flow.transition under 100000 | 124 | 8049 | 8378 | 13486 | 16934 | 17550 | 200 |
| flow.transition_many batch=100 under 100000 | 70 | 14215 | 8931 | 16077 | 212199 | 213933 | 200 |
| flow.complete under 100000 | 123 | 8129 | 8364 | 15491 | 17063 | 19133 | 200 |
| flow.retry under 100000 | 125 | 7976 | 8289 | 14975 | 16398 | 17974 | 200 |
| flow.fail under 100000 | 85 | 11769 | 8364 | 14136 | 122000 | 289224 | 200 |
| flow.cancel under 100000 | 110 | 9094 | 9634 | 15944 | 18439 | 20202 | 200 |
| flow.rewind under 100000 | 124 | 8071 | 8250 | 13960 | 15699 | 16939 | 200 |
