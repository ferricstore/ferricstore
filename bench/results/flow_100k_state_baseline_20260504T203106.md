# Flow 100k State Baseline

- started_at: 2026-05-04T20:31:06.625104Z
- backlog: 100000
- iterations: 200
- shards: 4
- partitions: 4
- seed_concurrency: 32
- claim_limits: 100
- beam_memory_before: 83341550
- beam_memory_after_seed: 578075834
- beam_memory_delta: 494734284

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 100000 | 226 | 4427 | 4349 | 5299 | 5609 | 8548 | 200 |
| flow.get from 100000 | 128949 | 8 | 7 | 14 | 31 | 42 | 200 |
| flow.list count=100 from 100000 | 2759 | 362 | 328 | 549 | 739 | 842 | 200 |
| flow.info over 100000 | 414 | 2413 | 22 | 39 | 73753 | 166700 | 200 |
| flow.history count=10 under 100000 | 34083 | 29 | 28 | 40 | 60 | 101 | 200 |
| flow.stuck count=100 under 100000 | 4564 | 219 | 197 | 316 | 413 | 584 | 200 |
| flow.claim_due limit=100 from 100000 | 115 | 8678 | 8550 | 9606 | 10157 | 10270 | 200 |
| flow.transition under 100000 | 201 | 4973 | 4570 | 5685 | 20230 | 31924 | 200 |
| flow.complete under 100000 | 215 | 4645 | 4560 | 5612 | 5977 | 7862 | 200 |
| flow.retry under 100000 | 221 | 4522 | 4339 | 5459 | 5755 | 9253 | 200 |
| flow.fail under 100000 | 199 | 5020 | 4916 | 7031 | 8760 | 10130 | 200 |
| flow.cancel under 100000 | 206 | 4846 | 4765 | 5920 | 6811 | 7408 | 200 |
| flow.rewind under 100000 | 33 | 30568 | 29842 | 40140 | 41960 | 57429 | 200 |
