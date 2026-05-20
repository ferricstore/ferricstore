# Flow 100k State Baseline

- started_at: 2026-05-06T21:36:08.641989Z
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
- claim_limits: 100
- beam_memory_before: 81367598
- beam_memory_after_seed: 442950329
- beam_memory_delta: 361582731

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 132 | 7575 | 4285 | 10316 | 77349 | 78734 | 200 |
| flow.create_many batch=100 under 100000 | 102 | 9760 | 9717 | 10831 | 13047 | 16025 | 200 |
| flow.get from 100000 | 224215 | 4 | 4 | 7 | 16 | 43 | 200 |
| flow.list count=100 from 100000 | 3529 | 283 | 256 | 313 | 416 | 4361 | 200 |
| flow.info over 100000 | 166667 | 6 | 5 | 11 | 14 | 19 | 200 |
| flow.history count=10 under 100000 | 5939 | 168 | 140 | 188 | 259 | 4530 | 200 |
| flow.history include_cold count=10 under 100000 | 20348 | 49 | 47 | 67 | 81 | 109 | 200 |
| flow.history cold_consistent count=10 under 100000 | 18776 | 53 | 51 | 72 | 84 | 105 | 200 |
| flow.stuck count=100 under 100000 | 6794 | 147 | 143 | 185 | 217 | 293 | 200 |
| flow.claim_due limit=100 from 100000 | 112 | 8966 | 8607 | 10429 | 13914 | 15087 | 200 |
| flow.transition under 100000 | 223 | 4477 | 4352 | 5214 | 6344 | 8898 | 200 |
| flow.transition_many batch=100 under 100000 | 107 | 9358 | 9203 | 10551 | 11331 | 16956 | 200 |
| flow.complete under 100000 | 129 | 7728 | 8236 | 11949 | 15909 | 19662 | 200 |
| flow.complete_many batch=100 under 100000 | 96 | 10403 | 9498 | 14320 | 15195 | 17608 | 200 |
| flow.retry under 100000 | 216 | 4626 | 4480 | 5419 | 5775 | 6681 | 200 |
| flow.retry_many batch=100 under 100000 | 106 | 9399 | 9586 | 10324 | 10984 | 13034 | 200 |
| flow.fail under 100000 | 119 | 8424 | 8458 | 14923 | 18447 | 24563 | 200 |
| flow.fail_many batch=100 under 100000 | 95 | 10569 | 9630 | 14390 | 16857 | 23227 | 200 |
| flow.cancel under 100000 | 84 | 11919 | 8625 | 16734 | 151876 | 229089 | 200 |
| flow.cancel_many batch=100 under 100000 | 92 | 10835 | 9957 | 14889 | 15879 | 18901 | 200 |
| flow.rewind under 100000 | 117 | 8546 | 8453 | 15426 | 17217 | 25403 | 200 |
