# Flow Commands Baseline

- started_at: 2026-05-04T19:31:03.599842Z
- iterations: 5
- shards: 1
- partitions: 1
- claim_limits: 2

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create | 184 | 5434 | 4191 | 10016 | 10016 | 10016 | 5 |
| flow.get | 277778 | 4 | 1 | 13 | 13 | 13 | 5 |
| flow.list count=100 | 4088 | 245 | 216 | 352 | 352 | 352 | 5 |
| flow.info | 9671 | 103 | 30 | 395 | 395 | 395 | 5 |
| flow.history count=10 | 43860 | 23 | 16 | 49 | 49 | 49 | 5 |
| flow.stuck count=100 | 37879 | 26 | 19 | 60 | 60 | 60 | 5 |
| flow.claim_due limit=2 | 183 | 5470 | 5322 | 6332 | 6332 | 6332 | 5 |
| flow.transition | 207 | 4829 | 4861 | 5500 | 5500 | 5500 | 5 |
| flow.complete | 211 | 4743 | 4761 | 5042 | 5042 | 5042 | 5 |
| flow.retry | 208 | 4808 | 5052 | 5100 | 5100 | 5100 | 5 |
| flow.fail | 198 | 5046 | 4997 | 5422 | 5422 | 5422 | 5 |
| flow.cancel | 223 | 4492 | 4486 | 4915 | 4915 | 4915 | 5 |
| flow.rewind | 210 | 4758 | 4720 | 5690 | 5690 | 5690 | 5 |
