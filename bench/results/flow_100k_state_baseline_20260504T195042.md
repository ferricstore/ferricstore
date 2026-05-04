# Flow 100k State Baseline

- started_at: 2026-05-04T19:50:42.615911Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- claim_limits: 10,100
- beam_memory_before: 81257980
- beam_memory_after_seed: 567557154
- beam_memory_delta: 486299174

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 221 | 4517 | 4454 | 5289 | 5622 | 6084 | 200 |
| flow.get from 100000 | 182149 | 5 | 5 | 10 | 25 | 35 | 200 |
| flow.list count=100 from 100000 | 2670 | 374 | 303 | 445 | 514 | 10689 | 200 |
| flow.info over 100000 | 400 | 2498 | 24 | 40 | 78843 | 169344 | 200 |
| flow.history count=10 under 100000 | 48792 | 20 | 20 | 27 | 38 | 89 | 200 |
| flow.stuck count=100 under 100000 | 6015 | 166 | 164 | 200 | 251 | 331 | 200 |
| flow.claim_due limit=10 from 100000 | 132 | 7593 | 8249 | 9873 | 10003 | 10672 | 200 |
| flow.claim_due limit=100 from 100000 | 83 | 12041 | 8723 | 10365 | 151249 | 151662 | 200 |
| flow.transition under 100000 | 214 | 4674 | 4530 | 5509 | 5610 | 5868 | 200 |
| flow.complete under 100000 | 220 | 4545 | 4385 | 5583 | 6251 | 7607 | 200 |
| flow.retry under 100000 | 229 | 4370 | 4270 | 5208 | 5667 | 9356 | 200 |
| flow.fail under 100000 | 213 | 4692 | 4466 | 5837 | 8990 | 10131 | 200 |
| flow.cancel under 100000 | 230 | 4350 | 4275 | 5275 | 5561 | 5824 | 200 |
| flow.rewind under 100000 | 29 | 34306 | 31183 | 46842 | 48052 | 50482 | 200 |
