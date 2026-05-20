# Flow 100k State Baseline

- started_at: 2026-05-04T21:45:29.766884Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 64
- claim_limits: 100
- beam_memory_before: 93682226
- beam_memory_after_seed: 584928662
- beam_memory_delta: 491246436

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 188 | 5314 | 4530 | 9669 | 15108 | 27569 | 200 |
| flow.get from 100000 | 110619 | 9 | 6 | 24 | 63 | 102 | 200 |
| flow.list count=100 from 100000 | 2566 | 390 | 351 | 624 | 867 | 906 | 200 |
| flow.info over 100000 | 267 | 3748 | 23 | 38 | 182408 | 197609 | 200 |
| flow.history count=10 under 100000 | 53390 | 19 | 17 | 28 | 35 | 65 | 200 |
| flow.stuck count=100 under 100000 | 4923 | 203 | 189 | 285 | 355 | 421 | 200 |
| flow.claim_due limit=100 from 100000 | 116 | 8598 | 8390 | 9694 | 9877 | 20520 | 200 |
| flow.transition under 100000 | 227 | 4408 | 4300 | 5274 | 5482 | 5580 | 200 |
| flow.complete under 100000 | 221 | 4515 | 4283 | 5361 | 7669 | 27296 | 200 |
| flow.retry under 100000 | 233 | 4294 | 4232 | 4656 | 8418 | 10170 | 200 |
| flow.fail under 100000 | 100 | 9997 | 8323 | 11159 | 76790 | 158813 | 200 |
| flow.cancel under 100000 | 206 | 4861 | 4759 | 5420 | 5666 | 29529 | 200 |
| flow.rewind under 100000 | 24 | 41341 | 40938 | 43280 | 48185 | 60444 | 200 |
