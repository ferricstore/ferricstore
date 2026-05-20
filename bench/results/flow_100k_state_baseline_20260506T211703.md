# Flow 100k State Baseline

- started_at: 2026-05-06T21:17:03.722700Z
- backlog: 20000
- iterations: 100
- shards: 4
- partitions: 4
- seed_concurrency: 32
- create_many_batch: 100
- transition_many_batch: 100
- terminal_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: off
- claim_limits: 100
- beam_memory_before: 86459007
- beam_memory_after_seed: 163254369
- beam_memory_delta: 76795362

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 20000 | 85 | 11765 | 10507 | 18660 | 28555 | 32173 | 100 |
| flow.create_many batch=100 under 20000 | 88 | 11347 | 9723 | 17887 | 19478 | 19848 | 100 |
| flow.get from 20000 | 113379 | 9 | 7 | 14 | 39 | 62 | 100 |
| flow.list count=100 from 20000 | 2495 | 401 | 363 | 601 | 700 | 808 | 100 |
| flow.info over 20000 | 137174 | 7 | 6 | 11 | 15 | 27 | 100 |
| flow.history count=10 under 20000 | 1936 | 517 | 260 | 1117 | 6115 | 9429 | 100 |
| flow.history include_cold count=10 under 20000 | 13470 | 74 | 70 | 104 | 115 | 141 | 100 |
| flow.history cold_consistent count=10 under 20000 | 15939 | 63 | 61 | 77 | 96 | 97 | 100 |
| flow.stuck count=100 under 20000 | 4117 | 243 | 222 | 369 | 445 | 553 | 100 |
| flow.claim_due limit=100 from 20000 | 79 | 12638 | 12401 | 17865 | 20436 | 21468 | 100 |
| flow.transition under 20000 | 114 | 8744 | 8473 | 11569 | 14517 | 16076 | 100 |
| flow.transition_many batch=100 under 20000 | 85 | 11728 | 10476 | 17451 | 18714 | 21327 | 100 |
| flow.complete under 20000 | 96 | 10424 | 8871 | 19401 | 25677 | 28313 | 100 |
| flow.complete_many batch=100 under 20000 | 86 | 11668 | 10422 | 17861 | 18899 | 18949 | 100 |
| flow.retry under 20000 | 90 | 11078 | 9731 | 20799 | 24635 | 25617 | 100 |
| flow.retry_many batch=100 under 20000 | 84 | 11938 | 11132 | 17211 | 19179 | 19396 | 100 |
| flow.fail under 20000 | 91 | 11019 | 9713 | 19920 | 23584 | 25414 | 100 |
| flow.fail_many batch=100 under 20000 | 82 | 12178 | 11855 | 18138 | 18661 | 18701 | 100 |
| flow.cancel under 20000 | 56 | 17832 | 9515 | 20791 | 347896 | 360684 | 100 |
| flow.cancel_many batch=100 under 20000 | 98 | 10229 | 9010 | 13897 | 14099 | 14146 | 100 |
| flow.rewind under 20000 | 88 | 11365 | 10426 | 20483 | 23133 | 26332 | 100 |
