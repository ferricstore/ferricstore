# Flow 100k State Baseline

- started_at: 2026-05-05T11:35:09.901453Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 100
- beam_memory_before: 94782360
- beam_memory_after_seed: 548562980
- beam_memory_delta: 453780620

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 124 | 8054 | 8467 | 12210 | 14944 | 16644 | 200 |
| flow.create_many batch=100 under 100000 | 95 | 10494 | 9682 | 14348 | 16610 | 23716 | 200 |
| flow.get from 100000 | 16372 | 61 | 46 | 75 | 108 | 2445 | 200 |
| flow.list count=100 from 100000 | 2561 | 390 | 300 | 345 | 4655 | 5194 | 200 |
| flow.info over 100000 | 179 | 5573 | 101 | 134 | 262849 | 306264 | 200 |
| flow.history count=10 under 100000 | 46827 | 21 | 21 | 26 | 41 | 71 | 200 |
| flow.stuck count=100 under 100000 | 3688 | 271 | 192 | 267 | 3654 | 4300 | 200 |
| flow.claim_due limit=100 from 100000 | 103 | 9751 | 8941 | 14054 | 17072 | 22528 | 200 |
| flow.transition under 100000 | 94 | 10648 | 7364 | 12962 | 153506 | 154485 | 200 |
| flow.transition_many batch=100 under 100000 | 99 | 10057 | 9343 | 13003 | 16010 | 17479 | 200 |
| flow.complete under 100000 | 148 | 6747 | 7172 | 10321 | 11724 | 14108 | 200 |
| flow.retry under 100000 | 142 | 7048 | 7742 | 10406 | 13679 | 17195 | 200 |
| flow.fail under 100000 | 126 | 7912 | 8643 | 11561 | 14974 | 15420 | 200 |
| flow.cancel under 100000 | 118 | 8498 | 8569 | 14754 | 17049 | 46829 | 200 |
| flow.rewind under 100000 | 139 | 7178 | 8183 | 9719 | 16910 | 33478 | 200 |
