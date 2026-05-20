# Flow 100k State Baseline

- started_at: 2026-05-08T19:36:01.961776Z
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
- beam_memory_before: 76999100
- beam_memory_after_seed: 438995714
- beam_memory_delta: 361996614

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 158 | 6319 | 6111 | 7276 | 7305 | 7325 | 100 |
| flow.create_many batch=100 under 100000 | 105 | 9501 | 9133 | 11259 | 11604 | 13569 | 100 |
| flow.get from 100000 | 182815 | 5 | 5 | 8 | 18 | 35 | 100 |
| flow.list count=100 from 100000 | 3146 | 318 | 307 | 372 | 520 | 547 | 100 |
| flow.info over 100000 | 159744 | 6 | 6 | 10 | 15 | 32 | 100 |
| flow.history count=10 under 100000 | 4244 | 236 | 180 | 283 | 399 | 4588 | 100 |
| flow.history include_cold count=10 under 100000 | 4968 | 201 | 149 | 202 | 243 | 5756 | 100 |
| flow.history cold_consistent count=10 under 100000 | 8245 | 121 | 127 | 154 | 168 | 181 | 100 |
| flow.stuck count=100 under 100000 | 8095 | 124 | 118 | 155 | 203 | 213 | 100 |
| flow.claim_due limit=100 from 100000 | 69 | 14517 | 14162 | 18266 | 20472 | 20572 | 100 |
| flow.transition under 100000 | 145 | 6896 | 6206 | 7370 | 16002 | 41240 | 100 |
| flow.transition_many batch=100 under 100000 | 102 | 9786 | 9053 | 13152 | 14075 | 14158 | 100 |
| flow.complete under 100000 | 144 | 6924 | 6199 | 9536 | 12073 | 34963 | 100 |
| flow.complete_many batch=100 under 100000 | 77 | 13011 | 12278 | 16188 | 17662 | 18677 | 100 |
| flow.retry under 100000 | 144 | 6940 | 7015 | 7894 | 8212 | 15109 | 100 |
| flow.retry_many batch=100 under 100000 | 83 | 12074 | 11863 | 14695 | 17033 | 17949 | 100 |
| flow.fail under 100000 | 140 | 7157 | 6619 | 10149 | 10783 | 15388 | 100 |
| flow.fail_many batch=100 under 100000 | 76 | 13079 | 12504 | 15699 | 19465 | 39769 | 100 |
| flow.cancel under 100000 | 134 | 7449 | 7145 | 9600 | 10681 | 10903 | 100 |
| flow.cancel_many batch=100 under 100000 | 87 | 11543 | 10807 | 14898 | 16767 | 21061 | 100 |
| flow.rewind under 100000 | 137 | 7276 | 6619 | 10209 | 14648 | 17848 | 100 |
