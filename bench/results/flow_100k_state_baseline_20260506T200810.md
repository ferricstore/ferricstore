# Flow 100k State Baseline

- started_at: 2026-05-06T20:08:10.263138Z
- backlog: 1000
- iterations: 10
- shards: 4
- partitions: 4
- seed_concurrency: 8
- create_many_batch: 10
- transition_many_batch: 10
- terminal_many_batch: 10
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 10
- beam_memory_before: 86270783
- beam_memory_after_seed: 92418821
- beam_memory_delta: 6148038

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 1000 | 123 | 8105 | 7030 | 16519 | 16519 | 16519 | 10 |
| flow.create_many batch=10 under 1000 | 102 | 9773 | 8861 | 24777 | 24777 | 24777 | 10 |
| flow.get from 1000 | 86957 | 12 | 7 | 38 | 38 | 38 | 10 |
| flow.list count=100 from 1000 | 1770 | 565 | 570 | 614 | 614 | 614 | 10 |
| flow.info over 1000 | 60606 | 17 | 13 | 34 | 34 | 34 | 10 |
| flow.history count=10 under 1000 | 1655 | 604 | 278 | 3391 | 3391 | 3391 | 10 |
| flow.history include_cold count=10 under 1000 | 11494 | 87 | 84 | 141 | 141 | 141 | 10 |
| flow.history cold_consistent count=10 under 1000 | 13477 | 74 | 77 | 88 | 88 | 88 | 10 |
| flow.stuck count=100 under 1000 | 30395 | 33 | 25 | 89 | 89 | 89 | 10 |
| flow.claim_due limit=10 from 1000 | 80 | 12548 | 12058 | 21011 | 21011 | 21011 | 10 |
| flow.transition under 1000 | 128 | 7803 | 6482 | 13011 | 13011 | 13011 | 10 |
| flow.transition_many batch=10 under 1000 | 102 | 9785 | 9314 | 14109 | 14109 | 14109 | 10 |
| flow.complete under 1000 | 127 | 7902 | 8483 | 12705 | 12705 | 12705 | 10 |
| flow.complete_many batch=10 under 1000 | 99 | 10120 | 8653 | 15520 | 15520 | 15520 | 10 |
| flow.retry under 1000 | 104 | 9640 | 8243 | 18981 | 18981 | 18981 | 10 |
| flow.retry_many batch=10 under 1000 | 99 | 10087 | 9665 | 13243 | 13243 | 13243 | 10 |
| flow.fail under 1000 | 113 | 8832 | 9162 | 17272 | 17272 | 17272 | 10 |
| flow.fail_many batch=10 under 1000 | 95 | 10548 | 8707 | 17816 | 17816 | 17816 | 10 |
| flow.cancel under 1000 | 114 | 8773 | 8939 | 13970 | 13970 | 13970 | 10 |
| flow.cancel_many batch=10 under 1000 | 96 | 10363 | 9459 | 17396 | 17396 | 17396 | 10 |
| flow.rewind under 1000 | 134 | 7453 | 6242 | 16333 | 16333 | 16333 | 10 |
