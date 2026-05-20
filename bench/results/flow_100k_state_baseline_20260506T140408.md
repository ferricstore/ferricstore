# Flow 100k State Baseline

- started_at: 2026-05-06T14:04:08.299435Z
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
- beam_memory_before: 82533053
- beam_memory_after_seed: 453962543
- beam_memory_delta: 371429490

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 111 | 9022 | 8536 | 15859 | 17106 | 18994 | 200 |
| flow.create_many batch=100 under 100000 | 96 | 10457 | 9276 | 14735 | 18059 | 19296 | 200 |
| flow.get from 100000 | 330579 | 3 | 3 | 6 | 12 | 20 | 200 |
| flow.list count=100 from 100000 | 3772 | 265 | 253 | 355 | 413 | 426 | 200 |
| flow.info over 100000 | 99900 | 10 | 9 | 16 | 22 | 42 | 200 |
| flow.history count=10 under 100000 | 6943 | 144 | 103 | 157 | 224 | 7391 | 200 |
| flow.stuck count=100 under 100000 | 5727 | 175 | 166 | 227 | 363 | 568 | 200 |
| flow.claim_due limit=100 from 100000 | 75 | 13298 | 12580 | 19368 | 20845 | 22265 | 200 |
| flow.transition under 100000 | 86 | 11588 | 10611 | 20622 | 22355 | 30918 | 200 |
| flow.transition_many batch=100 under 100000 | 74 | 13479 | 13345 | 18263 | 20944 | 23685 | 200 |
| flow.complete under 100000 | 82 | 12238 | 11075 | 21442 | 23995 | 35436 | 200 |
| flow.complete_many batch=100 under 100000 | 91 | 10986 | 9677 | 16661 | 22721 | 24921 | 200 |
| flow.retry under 100000 | 68 | 14778 | 10610 | 20107 | 156040 | 228525 | 200 |
| flow.retry_many batch=100 under 100000 | 70 | 14191 | 15684 | 20065 | 23500 | 29710 | 200 |
| flow.fail under 100000 | 89 | 11284 | 10115 | 19295 | 22945 | 23985 | 200 |
| flow.fail_many batch=100 under 100000 | 88 | 11345 | 9946 | 17477 | 19933 | 27238 | 200 |
| flow.cancel under 100000 | 87 | 11519 | 10605 | 19941 | 24498 | 43745 | 200 |
| flow.cancel_many batch=100 under 100000 | 87 | 11502 | 9985 | 17622 | 22000 | 28955 | 200 |
| flow.rewind under 100000 | 84 | 11954 | 10805 | 19024 | 25077 | 40229 | 200 |
