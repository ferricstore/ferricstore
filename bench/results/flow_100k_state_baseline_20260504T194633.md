# Flow 100k State Baseline

- started_at: 2026-05-04T19:46:33.614315Z
- backlog: 200
- iterations: 5
- shards: 4
- partitions: 4
- seed_concurrency: 16
- claim_limits: 2
- beam_memory_before: 84121156
- beam_memory_after_seed: 87528780
- beam_memory_delta: 3407624

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 200 | 95 | 10489 | 9814 | 14712 | 14712 | 14712 | 5 |
| flow.get from 200 | 125000 | 8 | 3 | 26 | 26 | 26 | 5 |
| flow.list count=100 from 200 | 4055 | 247 | 242 | 309 | 309 | 309 | 5 |
| flow.info over 200 | 4579 | 218 | 230 | 344 | 344 | 344 | 5 |
| flow.history count=10 under 200 | 35 | 28943 | 26470 | 36065 | 36065 | 36065 | 5 |
| flow.stuck count=100 under 200 | 61728 | 16 | 13 | 33 | 33 | 33 | 5 |
| flow.claim_due limit=2 from 200 | 82 | 12191 | 10288 | 15705 | 15705 | 15705 | 5 |
| flow.transition under 200 | 46 | 21922 | 22984 | 27192 | 27192 | 27192 | 5 |
| flow.complete under 200 | 32 | 31378 | 32299 | 36058 | 36058 | 36058 | 5 |
| flow.retry under 200 | 37 | 26969 | 26845 | 31416 | 31416 | 31416 | 5 |
| flow.fail under 200 | 33 | 30368 | 30086 | 35285 | 35285 | 35285 | 5 |
| flow.cancel under 200 | 51 | 19486 | 19661 | 19863 | 19863 | 19863 | 5 |
| flow.rewind under 200 | 26 | 38615 | 37938 | 43796 | 43796 | 43796 | 5 |
