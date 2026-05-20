# Flow 100k State Baseline

- started_at: 2026-05-05T08:16:29.919251Z
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
- beam_memory_before: 84770258
- beam_memory_after_seed: 535315694
- beam_memory_delta: 450545436

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 133 | 7527 | 8145 | 12660 | 16625 | 18795 | 200 |
| flow.create_many batch=100 under 100000 | 95 | 10497 | 9357 | 13303 | 15492 | 18249 | 200 |
| flow.get from 100000 | 15418 | 65 | 49 | 91 | 143 | 2323 | 200 |
| flow.list count=100 from 100000 | 2350 | 426 | 319 | 374 | 5122 | 6169 | 200 |
| flow.info over 100000 | 182 | 5491 | 113 | 151 | 318876 | 336321 | 200 |
| flow.history count=10 under 100000 | 76570 | 13 | 12 | 17 | 19 | 55 | 200 |
| flow.stuck count=100 under 100000 | 4821 | 207 | 149 | 168 | 3143 | 3244 | 200 |
| flow.claim_due limit=10 from 100000 | 86 | 11614 | 8444 | 15501 | 153949 | 221904 | 200 |
| flow.claim_due limit=100 from 100000 | 76 | 13162 | 8953 | 16697 | 147514 | 229951 | 200 |
| flow.transition under 100000 | 119 | 8436 | 8478 | 14575 | 16523 | 48097 | 200 |
| flow.transition_many batch=100 under 100000 | 107 | 9359 | 8558 | 12757 | 17910 | 43500 | 200 |
| flow.complete under 100000 | 143 | 6970 | 7658 | 13018 | 14212 | 17554 | 200 |
| flow.retry under 100000 | 95 | 10519 | 8152 | 13625 | 153617 | 226716 | 200 |
| flow.fail under 100000 | 126 | 7949 | 8424 | 13471 | 16885 | 20417 | 200 |
| flow.cancel under 100000 | 138 | 7240 | 7862 | 12909 | 13813 | 16124 | 200 |
| flow.rewind under 100000 | 133 | 7502 | 8071 | 12292 | 14256 | 17721 | 200 |
