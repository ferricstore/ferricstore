# Flow 100k State Baseline

- started_at: 2026-05-06T14:03:38.344179Z
- backlog: 100
- iterations: 3
- shards: 4
- partitions: 4
- seed_concurrency: 32
- create_many_batch: 5
- transition_many_batch: 5
- terminal_many_batch: 5
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 10
- beam_memory_before: 86030748
- beam_memory_after_seed: 89427990
- beam_memory_delta: 3397242

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100 | 229 | 4368 | 5058 | 5151 | 5151 | 5151 | 3 |
| flow.create_many batch=5 under 100 | 139 | 7174 | 8271 | 8892 | 8892 | 8892 | 3 |
| flow.get from 100 | 166667 | 6 | 2 | 14 | 14 | 14 | 3 |
| flow.list count=100 from 100 | 11719 | 85 | 82 | 95 | 95 | 95 | 3 |
| flow.info over 100 | 48387 | 21 | 18 | 31 | 31 | 31 | 3 |
| flow.history count=10 under 100 | 1244 | 804 | 178 | 2105 | 2105 | 2105 | 3 |
| flow.stuck count=100 under 100 | 76923 | 13 | 11 | 20 | 20 | 20 | 3 |
| flow.claim_due limit=10 from 100 | 121 | 8251 | 9019 | 10215 | 10215 | 10215 | 3 |
| flow.transition under 100 | 81 | 12380 | 10730 | 21753 | 21753 | 21753 | 3 |
| flow.transition_many batch=5 under 100 | 115 | 8731 | 5017 | 16760 | 16760 | 16760 | 3 |
| flow.complete under 100 | 94 | 10594 | 10445 | 11694 | 11694 | 11694 | 3 |
| flow.complete_many batch=5 under 100 | 143 | 7013 | 5120 | 11372 | 11372 | 11372 | 3 |
| flow.retry under 100 | 128 | 7834 | 8707 | 9768 | 9768 | 9768 | 3 |
| flow.retry_many batch=5 under 100 | 107 | 9307 | 11387 | 11657 | 11657 | 11657 | 3 |
| flow.fail under 100 | 93 | 10751 | 9866 | 16535 | 16535 | 16535 | 3 |
| flow.fail_many batch=5 under 100 | 98 | 10248 | 9821 | 11271 | 11271 | 11271 | 3 |
| flow.cancel under 100 | 104 | 9584 | 9990 | 13637 | 13637 | 13637 | 3 |
| flow.cancel_many batch=5 under 100 | 94 | 10588 | 9834 | 13084 | 13084 | 13084 | 3 |
| flow.rewind under 100 | 237 | 4217 | 4172 | 4450 | 4450 | 4450 | 3 |
