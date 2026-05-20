# Flow 100k State Baseline

- started_at: 2026-05-06T20:15:22.287096Z
- backlog: 1000
- iterations: 5
- shards: 4
- partitions: 4
- seed_concurrency: 8
- create_many_batch: 10
- transition_many_batch: 10
- terminal_many_batch: 10
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 10
- beam_memory_before: 85963619
- beam_memory_after_seed: 90925685
- beam_memory_delta: 4962066

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 1000 | 77 | 13055 | 12529 | 16440 | 16440 | 16440 | 5 |
| flow.create_many batch=10 under 1000 | 98 | 10223 | 9832 | 13872 | 13872 | 13872 | 5 |
| flow.get from 1000 | 15198 | 66 | 36 | 183 | 183 | 183 | 5 |
| flow.list count=100 from 1000 | 735 | 1361 | 1344 | 1845 | 1845 | 1845 | 5 |
| flow.info over 1000 | 19380 | 52 | 27 | 136 | 136 | 136 | 5 |
| flow.history count=10 under 1000 | 381 | 2625 | 803 | 10126 | 10126 | 10126 | 5 |
| flow.history include_cold count=10 under 1000 | 1095 | 913 | 300 | 3417 | 3417 | 3417 | 5 |
| flow.history cold_consistent count=10 under 1000 | 2443 | 409 | 280 | 890 | 890 | 890 | 5 |
| flow.stuck count=100 under 1000 | 12920 | 77 | 62 | 153 | 153 | 153 | 5 |
| flow.claim_due limit=10 from 1000 | 63 | 15828 | 9771 | 28091 | 28091 | 28091 | 5 |
| flow.transition under 1000 | 96 | 10404 | 11753 | 16033 | 16033 | 16033 | 5 |
| flow.transition_many batch=10 under 1000 | 64 | 15628 | 11865 | 23566 | 23566 | 23566 | 5 |
| flow.complete under 1000 | 74 | 13442 | 15249 | 19377 | 19377 | 19377 | 5 |
| flow.complete_many batch=10 under 1000 | 98 | 10208 | 10567 | 16905 | 16905 | 16905 | 5 |
| flow.retry under 1000 | 51 | 19772 | 20668 | 26715 | 26715 | 26715 | 5 |
| flow.retry_many batch=10 under 1000 | 77 | 12936 | 12731 | 15917 | 15917 | 15917 | 5 |
| flow.fail under 1000 | 95 | 10572 | 8533 | 18131 | 18131 | 18131 | 5 |
| flow.fail_many batch=10 under 1000 | 9 | 117156 | 151657 | 224847 | 224847 | 224847 | 5 |
| flow.cancel under 1000 | 118 | 8500 | 9197 | 13306 | 13306 | 13306 | 5 |
| flow.cancel_many batch=10 under 1000 | 74 | 13458 | 11996 | 19319 | 19319 | 19319 | 5 |
| flow.rewind under 1000 | 93 | 10788 | 10308 | 12212 | 12212 | 12212 | 5 |
