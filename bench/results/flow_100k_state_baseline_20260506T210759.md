# Flow 100k State Baseline

- started_at: 2026-05-06T21:07:59.358735Z
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
- beam_memory_before: 81409572
- beam_memory_after_seed: 448922759
- beam_memory_delta: 367513187

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 126 | 7926 | 8335 | 11690 | 15725 | 16953 | 200 |
| flow.create_many batch=100 under 100000 | 74 | 13491 | 9308 | 14576 | 152674 | 241110 | 200 |
| flow.get from 100000 | 264550 | 4 | 3 | 6 | 9 | 31 | 200 |
| flow.list count=100 from 100000 | 3657 | 273 | 265 | 336 | 391 | 461 | 200 |
| flow.info over 100000 | 154083 | 6 | 6 | 12 | 18 | 22 | 200 |
| flow.history count=10 under 100000 | 6431 | 155 | 131 | 174 | 209 | 4333 | 200 |
| flow.history include_cold count=10 under 100000 | 20182 | 50 | 48 | 63 | 87 | 170 | 200 |
| flow.history cold_consistent count=10 under 100000 | 19761 | 51 | 50 | 60 | 74 | 75 | 200 |
| flow.stuck count=100 under 100000 | 5217 | 192 | 181 | 273 | 380 | 423 | 200 |
| flow.claim_due limit=100 from 100000 | 91 | 10937 | 9747 | 16957 | 21132 | 22298 | 200 |
| flow.transition under 100000 | 123 | 8141 | 8192 | 13354 | 16869 | 17261 | 200 |
| flow.transition_many batch=100 under 100000 | 72 | 13891 | 9296 | 17173 | 158190 | 230684 | 200 |
| flow.complete under 100000 | 110 | 9053 | 9299 | 13605 | 17128 | 17887 | 200 |
| flow.complete_many batch=100 under 100000 | 95 | 10489 | 9602 | 14831 | 17924 | 20554 | 200 |
| flow.retry under 100000 | 111 | 8977 | 8868 | 15748 | 20595 | 39315 | 200 |
| flow.retry_many batch=100 under 100000 | 100 | 10015 | 9236 | 12946 | 16583 | 16957 | 200 |
| flow.fail under 100000 | 109 | 9148 | 9336 | 15720 | 17450 | 18304 | 200 |
| flow.fail_many batch=100 under 100000 | 94 | 10673 | 9802 | 15239 | 17318 | 18692 | 200 |
| flow.cancel under 100000 | 118 | 8510 | 8419 | 14110 | 17284 | 17909 | 200 |
| flow.cancel_many batch=100 under 100000 | 97 | 10363 | 9456 | 14623 | 17195 | 27190 | 200 |
| flow.rewind under 100000 | 104 | 9655 | 8996 | 16413 | 21034 | 49368 | 200 |
