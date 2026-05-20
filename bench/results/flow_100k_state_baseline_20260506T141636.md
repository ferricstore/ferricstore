# Flow 100k State Baseline

- started_at: 2026-05-06T14:16:36.924612Z
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
- beam_memory_before: 85387671
- beam_memory_after_seed: 451715201
- beam_memory_delta: 366327530

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 124 | 8050 | 8154 | 13545 | 17476 | 24742 | 100 |
| flow.create_many batch=100 under 100000 | 98 | 10179 | 9110 | 14405 | 15965 | 18093 | 100 |
| flow.get from 100000 | 366300 | 3 | 2 | 6 | 11 | 16 | 100 |
| flow.list count=100 from 100000 | 4288 | 233 | 231 | 260 | 292 | 309 | 100 |
| flow.info over 100000 | 97847 | 10 | 8 | 19 | 27 | 36 | 100 |
| flow.history count=10 under 100000 | 6643 | 151 | 74 | 172 | 318 | 5948 | 100 |
| flow.stuck count=100 under 100000 | 11763 | 85 | 56 | 179 | 266 | 280 | 100 |
| flow.claim_due limit=100 from 100000 | 84 | 11901 | 10816 | 17448 | 21014 | 21503 | 100 |
| flow.transition under 100000 | 125 | 7993 | 8264 | 12805 | 15777 | 15833 | 100 |
| flow.transition_many batch=100 under 100000 | 58 | 17296 | 10367 | 20778 | 155212 | 163547 | 100 |
| flow.complete under 100000 | 108 | 9217 | 9750 | 16974 | 17341 | 21866 | 100 |
| flow.complete_many batch=100 under 100000 | 93 | 10700 | 9769 | 14968 | 17014 | 18520 | 100 |
| flow.retry under 100000 | 116 | 8646 | 8510 | 16396 | 17221 | 22455 | 100 |
| flow.retry_many batch=100 under 100000 | 80 | 12523 | 11780 | 17474 | 18226 | 26842 | 100 |
| flow.fail under 100000 | 117 | 8525 | 8508 | 14158 | 16631 | 19741 | 100 |
| flow.fail_many batch=100 under 100000 | 92 | 10894 | 9806 | 14594 | 18029 | 18182 | 100 |
| flow.cancel under 100000 | 108 | 9218 | 9487 | 16704 | 20833 | 25095 | 100 |
| flow.cancel_many batch=100 under 100000 | 90 | 11145 | 10012 | 15368 | 18949 | 19840 | 100 |
| flow.rewind under 100000 | 112 | 8907 | 9001 | 14360 | 17263 | 20923 | 100 |
