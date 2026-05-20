# Flow 100k State Baseline

- started_at: 2026-05-06T20:08:25.928389Z
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
- claim_limits: 10,100
- beam_memory_before: 82487969
- beam_memory_after_seed: 437119148
- beam_memory_delta: 354631179

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 113 | 8888 | 8601 | 17563 | 18624 | 52165 | 100 |
| flow.create_many batch=100 under 100000 | 86 | 11574 | 10218 | 17058 | 22212 | 30692 | 100 |
| flow.get from 100000 | 114811 | 9 | 7 | 13 | 38 | 71 | 100 |
| flow.list count=100 from 100000 | 2392 | 418 | 387 | 628 | 732 | 848 | 100 |
| flow.info over 100000 | 134228 | 7 | 6 | 11 | 24 | 34 | 100 |
| flow.history count=10 under 100000 | 1930 | 518 | 243 | 533 | 1191 | 23036 | 100 |
| flow.history include_cold count=10 under 100000 | 14198 | 70 | 68 | 90 | 117 | 129 | 100 |
| flow.history cold_consistent count=10 under 100000 | 16046 | 62 | 60 | 79 | 88 | 102 | 100 |
| flow.stuck count=100 under 100000 | 4000 | 250 | 237 | 409 | 506 | 514 | 100 |
| flow.claim_due limit=10 from 100000 | 79 | 12680 | 11042 | 21978 | 26810 | 37663 | 100 |
| flow.claim_due limit=100 from 100000 | 87 | 11521 | 11630 | 15568 | 19595 | 19681 | 100 |
| flow.transition under 100000 | 150 | 6679 | 4960 | 11768 | 13406 | 17652 | 100 |
| flow.transition_many batch=100 under 100000 | 90 | 11059 | 10415 | 14630 | 15176 | 23425 | 100 |
| flow.complete under 100000 | 137 | 7306 | 5247 | 12471 | 15103 | 55792 | 100 |
| flow.complete_many batch=100 under 100000 | 106 | 9421 | 8627 | 12515 | 13189 | 13519 | 100 |
| flow.retry under 100000 | 152 | 6563 | 4526 | 11786 | 14841 | 31070 | 100 |
| flow.retry_many batch=100 under 100000 | 106 | 9421 | 8693 | 12743 | 15044 | 15928 | 100 |
| flow.fail under 100000 | 72 | 13831 | 5216 | 15417 | 149189 | 153598 | 100 |
| flow.fail_many batch=100 under 100000 | 89 | 11299 | 11727 | 14214 | 14713 | 15744 | 100 |
| flow.cancel under 100000 | 139 | 7210 | 4805 | 13378 | 14340 | 73013 | 100 |
| flow.cancel_many batch=100 under 100000 | 102 | 9774 | 9296 | 12335 | 12814 | 13138 | 100 |
| flow.rewind under 100000 | 170 | 5898 | 4701 | 10071 | 12293 | 13822 | 100 |
