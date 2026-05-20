# Flow 100k State Baseline

- started_at: 2026-05-06T05:33:50.565433Z
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
- beam_memory_before: 85443588
- beam_memory_after_seed: 455291956
- beam_memory_delta: 369848368

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 103 | 9680 | 9335 | 15857 | 16503 | 16897 | 200 |
| flow.create_many batch=100 under 100000 | 93 | 10747 | 9818 | 13908 | 14677 | 18160 | 200 |
| flow.get from 100000 | 333890 | 3 | 3 | 5 | 12 | 20 | 200 |
| flow.list count=100 from 100000 | 3289 | 304 | 252 | 294 | 389 | 9778 | 200 |
| flow.info over 100000 | 294 | 3396 | 61 | 93 | 164289 | 171279 | 200 |
| flow.history count=10 under 100000 | 8655 | 116 | 75 | 128 | 177 | 7082 | 200 |
| flow.stuck count=100 under 100000 | 6739 | 148 | 143 | 179 | 247 | 342 | 200 |
| flow.claim_due limit=10 from 100000 | 103 | 9679 | 9220 | 15797 | 16821 | 20161 | 200 |
| flow.claim_due limit=100 from 100000 | 94 | 10672 | 9489 | 16542 | 17688 | 19695 | 200 |
| flow.transition under 100000 | 110 | 9052 | 8558 | 14614 | 16089 | 16340 | 200 |
| flow.transition_many batch=100 under 100000 | 102 | 9773 | 8766 | 13738 | 20060 | 22806 | 200 |
| flow.complete under 100000 | 105 | 9517 | 9080 | 14806 | 15981 | 17393 | 200 |
| flow.retry under 100000 | 107 | 9378 | 8667 | 14664 | 15681 | 16013 | 200 |
| flow.fail under 100000 | 78 | 12900 | 9389 | 14903 | 161629 | 234504 | 200 |
| flow.cancel under 100000 | 101 | 9900 | 9809 | 15374 | 16972 | 17137 | 200 |
| flow.rewind under 100000 | 105 | 9558 | 9434 | 14862 | 15739 | 19785 | 200 |
