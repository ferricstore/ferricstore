# Flow 100k State Baseline

- started_at: 2026-05-06T06:22:37.728342Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 10,100
- beam_memory_before: 85326301
- beam_memory_after_seed: 453234693
- beam_memory_delta: 367908392

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 94 | 10629 | 10173 | 17218 | 17376 | 18571 | 200 |
| flow.create_many batch=100 under 100000 | 91 | 10995 | 9879 | 14794 | 16262 | 18241 | 200 |
| flow.get from 100000 | 319489 | 3 | 3 | 6 | 8 | 21 | 200 |
| flow.list count=100 from 100000 | 3903 | 256 | 251 | 292 | 358 | 417 | 200 |
| flow.info over 100000 | 346 | 2890 | 24 | 49 | 140936 | 149873 | 200 |
| flow.history count=10 under 100000 | 9173 | 109 | 85 | 158 | 180 | 2909 | 200 |
| flow.stuck count=100 under 100000 | 5936 | 168 | 163 | 209 | 262 | 373 | 200 |
| flow.claim_due limit=10 from 100000 | 76 | 13077 | 9321 | 19995 | 148640 | 232869 | 200 |
| flow.claim_due limit=100 from 100000 | 93 | 10763 | 10000 | 16327 | 18081 | 23033 | 200 |
| flow.transition under 100000 | 114 | 8753 | 8388 | 12836 | 15595 | 18674 | 200 |
| flow.transition_many batch=100 under 100000 | 114 | 8751 | 8260 | 12182 | 15867 | 16422 | 200 |
| flow.complete under 100000 | 81 | 12370 | 8851 | 16758 | 154422 | 236320 | 200 |
| flow.retry under 100000 | 114 | 8758 | 8395 | 13548 | 15705 | 18937 | 200 |
| flow.fail under 100000 | 110 | 9119 | 8752 | 14908 | 17177 | 17424 | 200 |
| flow.cancel under 100000 | 83 | 12001 | 8863 | 13868 | 165061 | 222771 | 200 |
| flow.rewind under 100000 | 109 | 9204 | 8896 | 15565 | 17208 | 20930 | 200 |
