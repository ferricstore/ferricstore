# Flow 100k State Baseline

- started_at: 2026-05-05T05:47:54.075857Z
- backlog: 20000
- iterations: 100
- shards: 4
- partitions: 4
- seed_concurrency: 64
- create_many_batch: 100
- transition_many_batch: 100
- flow_lmdb_enabled: true
- flow_lmdb_mode: mirror
- claim_limits: 100
- beam_memory_before: 83496338
- beam_memory_after_seed: 176559402
- beam_memory_delta: 93063064

| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| flow.create under 20000 | 67 | 14959 | 8071 | 82552 | 151857 | 153678 | 100 |
| flow.create_many batch=100 under 20000 | 96 | 10417 | 9578 | 14375 | 15150 | 17404 | 100 |
| flow.get from 20000 | 13261 | 75 | 52 | 75 | 133 | 2062 | 100 |
| flow.list count=100 from 20000 | 2125 | 471 | 271 | 331 | 5222 | 5296 | 100 |
| flow.info over 20000 | 896 | 1116 | 22 | 51 | 36083 | 37668 | 100 |
| flow.history count=10 under 20000 | 50633 | 20 | 18 | 26 | 42 | 86 | 100 |
| flow.stuck count=100 under 20000 | 7345 | 136 | 81 | 110 | 1386 | 1577 | 100 |
| flow.claim_due limit=100 from 20000 | 94 | 10652 | 9624 | 16751 | 19692 | 21571 | 100 |
| flow.transition under 20000 | 124 | 8088 | 8045 | 14649 | 16639 | 17368 | 100 |
| flow.transition_many batch=100 under 20000 | 102 | 9849 | 8730 | 13138 | 15777 | 16285 | 100 |
| flow.complete under 20000 | 113 | 8880 | 8560 | 15478 | 19772 | 19984 | 100 |
| flow.retry under 20000 | 113 | 8815 | 8725 | 14665 | 16493 | 19187 | 100 |
| flow.fail under 20000 | 118 | 8454 | 8369 | 14830 | 19687 | 19788 | 100 |
| flow.cancel under 20000 | 124 | 8067 | 8225 | 12222 | 17017 | 19892 | 100 |
| flow.rewind under 20000 | 119 | 8403 | 8529 | 12736 | 15656 | 15882 | 100 |
