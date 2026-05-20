# Flow 100k State Baseline

- started_at: 2026-05-06T06:01:03.052502Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 10,100
- beam_memory_before: 85251171
- beam_memory_after_seed: 451687195
- beam_memory_delta: 366436024

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 99 | 10051 | 7661 | 11387 | 151848 | 226468 | 200 |
| flow.create_many batch=100 under 100000 | 94 | 10649 | 9876 | 14610 | 16687 | 19826 | 200 |
| flow.get from 100000 | 346021 | 3 | 2 | 5 | 9 | 23 | 200 |
| flow.list count=100 from 100000 | 3745 | 267 | 262 | 312 | 369 | 451 | 200 |
| flow.info over 100000 | 345 | 2901 | 54 | 83 | 142057 | 143296 | 200 |
| flow.history count=10 under 100000 | 9514 | 105 | 81 | 151 | 251 | 3343 | 200 |
| flow.stuck count=100 under 100000 | 7089 | 141 | 136 | 176 | 252 | 274 | 200 |
| flow.claim_due limit=10 from 100000 | 137 | 7280 | 7933 | 11887 | 15181 | 16837 | 200 |
| flow.claim_due limit=100 from 100000 | 90 | 11165 | 10222 | 16885 | 17986 | 21976 | 200 |
| flow.transition under 100000 | 137 | 7290 | 7263 | 10241 | 13407 | 16200 | 200 |
| flow.transition_many batch=100 under 100000 | 109 | 9136 | 8683 | 12404 | 17277 | 17793 | 200 |
| flow.complete under 100000 | 146 | 6865 | 7903 | 9693 | 13092 | 13779 | 200 |
| flow.retry under 100000 | 130 | 7689 | 8000 | 14027 | 16960 | 22157 | 200 |
| flow.fail under 100000 | 149 | 6710 | 7610 | 9886 | 12881 | 21780 | 200 |
| flow.cancel under 100000 | 144 | 6952 | 6711 | 12353 | 15176 | 16151 | 200 |
| flow.rewind under 100000 | 98 | 10163 | 7734 | 12990 | 149506 | 230365 | 200 |
