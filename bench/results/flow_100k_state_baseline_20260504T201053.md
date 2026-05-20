# Flow 100k State Baseline

- started_at: 2026-05-04T20:10:53.306063Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- claim_limits: 100
- beam_memory_before: 84710228
- beam_memory_after_seed: 566692456
- beam_memory_delta: 481982228

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 198 | 5051 | 5000 | 6136 | 7307 | 10855 | 200 |
| flow.get from 100000 | 222222 | 5 | 4 | 9 | 19 | 26 | 200 |
| flow.list count=100 from 100000 | 3188 | 314 | 298 | 391 | 513 | 629 | 200 |
| flow.info over 100000 | 235 | 4254 | 23 | 39 | 253624 | 256643 | 200 |
| flow.history count=10 under 100000 | 54348 | 18 | 16 | 25 | 40 | 167 | 200 |
| flow.stuck count=100 under 100000 | 6910 | 145 | 142 | 171 | 196 | 235 | 200 |
| flow.claim_due limit=100 from 100000 | 107 | 9338 | 9320 | 10547 | 13053 | 13400 | 200 |
| flow.transition under 100000 | 220 | 4555 | 4502 | 5356 | 5798 | 8118 | 200 |
| flow.complete under 100000 | 221 | 4523 | 4431 | 5401 | 5803 | 6185 | 200 |
| flow.retry under 100000 | 210 | 4770 | 4679 | 5503 | 8423 | 17679 | 200 |
| flow.fail under 100000 | 223 | 4479 | 4422 | 5279 | 5590 | 6318 | 200 |
| flow.cancel under 100000 | 220 | 4548 | 4467 | 5461 | 5762 | 6276 | 200 |
| flow.rewind under 100000 | 20 | 49448 | 57153 | 61328 | 62433 | 65039 | 200 |
