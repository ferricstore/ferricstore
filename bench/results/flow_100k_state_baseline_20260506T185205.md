# Flow 100k State Baseline

- started_at: 2026-05-06T18:52:05.579303Z
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
- claim_limits: 10,100
- beam_memory_before: 85289555
- beam_memory_after_seed: 444042045
- beam_memory_delta: 358752490

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 130 | 7675 | 6877 | 11699 | 13912 | 43531 | 200 |
| flow.create_many batch=100 under 100000 | 101 | 9862 | 9628 | 11616 | 14513 | 20219 | 200 |
| flow.get from 100000 | 91116 | 11 | 8 | 16 | 62 | 315 | 200 |
| flow.list count=100 from 100000 | 2873 | 348 | 302 | 567 | 982 | 1246 | 200 |
| flow.info over 100000 | 106383 | 9 | 9 | 14 | 27 | 66 | 200 |
| flow.history count=10 under 100000 | 4396 | 227 | 151 | 377 | 485 | 9308 | 200 |
| flow.stuck count=100 under 100000 | 3465 | 289 | 232 | 540 | 741 | 756 | 200 |
| flow.claim_due limit=10 from 100000 | 76 | 13077 | 9932 | 13125 | 122038 | 153947 | 200 |
| flow.claim_due limit=100 from 100000 | 76 | 13108 | 9600 | 13399 | 160795 | 179951 | 200 |
| flow.transition under 100000 | 119 | 8398 | 8783 | 12308 | 16962 | 62819 | 200 |
| flow.transition_many batch=100 under 100000 | 106 | 9422 | 9424 | 10415 | 10486 | 10933 | 200 |
| flow.complete under 100000 | 98 | 10256 | 9481 | 18733 | 26360 | 32734 | 200 |
| flow.complete_many batch=100 under 100000 | 81 | 12312 | 10748 | 17005 | 19584 | 56555 | 200 |
| flow.retry under 100000 | 123 | 8126 | 8112 | 11945 | 15756 | 85154 | 200 |
| flow.retry_many batch=100 under 100000 | 105 | 9489 | 9398 | 10485 | 12408 | 18330 | 200 |
| flow.fail under 100000 | 90 | 11124 | 10408 | 18834 | 22614 | 32782 | 200 |
| flow.fail_many batch=100 under 100000 | 90 | 11059 | 10009 | 14503 | 16948 | 22564 | 200 |
| flow.cancel under 100000 | 89 | 11296 | 9926 | 20596 | 40196 | 73362 | 200 |
| flow.cancel_many batch=100 under 100000 | 68 | 14744 | 9909 | 17149 | 227091 | 253171 | 200 |
| flow.rewind under 100000 | 113 | 8836 | 8575 | 15069 | 17522 | 87668 | 200 |
