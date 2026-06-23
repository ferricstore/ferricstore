# Local Regression Baselines

Local benchmark baselines are for regression tracking on one developer machine.
They are not public benchmark claims because server and client usually run on
the same host over loopback.

## Current Transport

Current standalone FerricStore releases use the Ferric native binary protocol.
Older TCP benchmark baselines are not valid for current releases and should not
be used for performance claims or regression gates.

## Required Baseline Shape

For local KV baselines, use a native-protocol benchmark client or SDK runner and
record:

| Field | Required |
| --- | --- |
| Date, branch, commit | Yes |
| Server command/env | Yes |
| Client command/env | Yes |
| Transport | Ferric native TCP/TLS |
| Connections | Yes |
| Lanes per connection | Yes |
| In-flight requests per lane | Yes |
| Value size and key range | Yes |
| Throughput and p50/p95/p99/p99.9 | Yes |

For FerricFlow baselines, continue using the Python SDK workload scripts and
record the workload shape, flow count, producer/worker counts, partitions,
server shard count, and create/process/end-to-end rates.

## Runner Status

`bench/local_regression_baseline.py` still contains historical baseline
scaffolding. Update it to native protocol before using it as a current
regression gate.
