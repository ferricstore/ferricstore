# Flow 100k State Baseline

- started_at: 2026-05-08T19:38:48.916479Z
- backlog: 100000
- iterations: 100
- shards: 4
- partitions: 4
- seed_concurrency: 32
- create_many_batch: 100
- transition_many_batch: 100
- terminal_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 100
- beam_memory_before: 83464667
- beam_memory_after_seed: 492895760
- beam_memory_delta: 409431093

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 189 | 5304 | 4509 | 9014 | 9587 | 10041 | 100 |
| flow.create_many batch=100 under 100000 | 111 | 8991 | 8901 | 10173 | 12497 | 13509 | 100 |
| flow.get from 100000 | 347222 | 3 | 2 | 5 | 10 | 17 | 100 |
| flow.list count=100 from 100000 | 3826 | 261 | 258 | 296 | 354 | 364 | 100 |
| flow.info over 100000 | 169205 | 6 | 5 | 10 | 15 | 20 | 100 |
| flow.history count=10 under 100000 | 3487 | 287 | 222 | 327 | 690 | 5438 | 100 |
| flow.history include_cold count=10 under 100000 | 7644 | 131 | 125 | 179 | 191 | 205 | 100 |
| flow.history cold_consistent count=10 under 100000 | 9967 | 100 | 101 | 134 | 142 | 147 | 100 |
| flow.stuck count=100 under 100000 | 5666 | 176 | 167 | 345 | 641 | 647 | 100 |
| flow.claim_due limit=100 from 100000 | 72 | 13888 | 13406 | 16499 | 20756 | 21443 | 100 |
| flow.transition under 100000 | 122 | 8220 | 8382 | 11366 | 14460 | 21071 | 100 |
| flow.transition_many batch=100 under 100000 | 112 | 8919 | 8442 | 11147 | 15480 | 15483 | 100 |
| flow.complete under 100000 | 162 | 6190 | 4502 | 10179 | 21267 | 21866 | 100 |
| flow.complete_many batch=100 under 100000 | 86 | 11568 | 9694 | 16774 | 22186 | 22312 | 100 |
| flow.retry under 100000 | 145 | 6885 | 7644 | 9864 | 14050 | 24461 | 100 |
| flow.retry_many batch=100 under 100000 | 99 | 10141 | 9428 | 14715 | 16731 | 17098 | 100 |
| flow.fail under 100000 | 121 | 8273 | 8546 | 13462 | 16256 | 17225 | 100 |
| flow.fail_many batch=100 under 100000 | 89 | 11220 | 9815 | 16520 | 18887 | 27820 | 100 |
| flow.cancel under 100000 | 123 | 8114 | 8298 | 12772 | 17507 | 18564 | 100 |
| flow.cancel_many batch=100 under 100000 | 104 | 9571 | 8671 | 13145 | 20216 | 22779 | 100 |
| flow.rewind under 100000 | 125 | 7981 | 8232 | 12621 | 15030 | 15102 | 100 |
