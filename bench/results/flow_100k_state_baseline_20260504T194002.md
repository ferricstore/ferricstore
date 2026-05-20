# Flow 100k State Baseline

- started_at: 2026-05-04T19:40:02.412330Z
- backlog: 100
- iterations: 5
- shards: 4
- partitions: 4
- claim_limits: 2
- beam_memory_before: 83293205
- beam_memory_after_seed: 85225789
- beam_memory_delta: 1932584

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100 | 213 | 4698 | 4789 | 5244 | 5244 | 5244 | 5 |
| flow.get from 100 | 142857 | 7 | 3 | 22 | 22 | 22 | 5 |
| flow.list count=100 from 100 | 9042 | 111 | 110 | 135 | 135 | 135 | 5 |
| flow.info over 100 | 5701 | 175 | 210 | 276 | 276 | 276 | 5 |
| flow.history count=10 under 100 | 80 | 12575 | 12425 | 13646 | 13646 | 13646 | 5 |
| flow.stuck count=100 under 100 | 94340 | 11 | 8 | 19 | 19 | 19 | 5 |
| flow.claim_due limit=2 from 100 | 239 | 4178 | 4092 | 4729 | 4729 | 4729 | 5 |
| flow.transition under 100 | 109 | 9177 | 9753 | 9881 | 9881 | 9881 | 5 |
| flow.complete under 100 | 74 | 13587 | 13722 | 14369 | 14369 | 14369 | 5 |
| flow.retry under 100 | 75 | 13403 | 13107 | 14235 | 14235 | 14235 | 5 |
| flow.fail under 100 | 74 | 13493 | 13502 | 13794 | 13794 | 13794 | 5 |
| flow.cancel under 100 | 112 | 8950 | 8883 | 9792 | 9792 | 9792 | 5 |
| flow.rewind under 100 | 56 | 17963 | 17902 | 19680 | 19680 | 19680 | 5 |
