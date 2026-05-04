# Flow Commands Baseline

- started_at: 2026-05-04T19:31:21.597135Z
- iterations: 200
- shards: 4
- partitions: 4
- claim_limits: 10,100

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create | 201 | 4981 | 4932 | 5767 | 6222 | 10241 | 200 |
| flow.get | 382409 | 3 | 2 | 7 | 14 | 15 | 200 |
| flow.list count=100 | 7156 | 140 | 139 | 156 | 172 | 223 | 200 |
| flow.info | 28998 | 34 | 23 | 32 | 465 | 673 | 200 |
| flow.history count=10 | 56948 | 18 | 17 | 20 | 29 | 84 | 200 |
| flow.stuck count=100 | 6719 | 149 | 144 | 186 | 246 | 327 | 200 |
| flow.claim_due limit=10 | 142 | 7048 | 7920 | 9825 | 10091 | 11090 | 200 |
| flow.claim_due limit=100 | 108 | 9224 | 8989 | 10954 | 12734 | 16000 | 200 |
| flow.transition | 213 | 4693 | 4661 | 5513 | 7021 | 8288 | 200 |
| flow.complete | 222 | 4495 | 4432 | 5430 | 5694 | 7068 | 200 |
| flow.retry | 237 | 4213 | 4139 | 5308 | 6211 | 7264 | 200 |
| flow.fail | 109 | 9188 | 9343 | 11674 | 14031 | 15003 | 200 |
| flow.cancel | 117 | 8538 | 8415 | 12681 | 13951 | 17973 | 200 |
| flow.rewind | 83 | 11994 | 12464 | 16608 | 19411 | 20954 | 200 |
