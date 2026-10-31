# Deployment Guide

This guide covers running FerricStore as a server: native release, Docker,
AWS Fargate, Kubernetes, and cluster layouts. For local development, start with
[Getting Started](getting-started.md). For production security, pair this guide
with [Security](security.md).

> **Beta:** FerricStore is currently a `0.x` beta. Use exact image/package
> versions and validate upgrades before critical production use. Compatibility
> guarantees will harden with the `1.0` release line.

Recommended path:

1. Use Docker for local smoke tests.
2. Use a native release or container image for production.
3. Put data on durable fast storage; use local NVMe for benchmarks.
4. Enable protected mode, ACL, and TLS before exposing the server.

## Native Release (Recommended for Benchmarks)

Build and run directly on the host for maximum performance:

```bash
MIX_ENV=prod mix release ferricstore
_build/prod/rel/ferricstore/bin/ferricstore start
```

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_NATIVE_PORT` | `6388` | Ferric native protocol TCP listen port |
| `FERRICSTORE_HEALTH_PORT` | `6380` | Legacy dashboard, metrics, and health port |
| `FERRICSTORE_HEALTH_PROBE_PORT` | `6381` | Isolated liveness/readiness port |
| `FERRICSTORE_HTTP_ENABLED` | `false` | Start the bundled HTTP/HTTPS command API |
| `FERRICSTORE_HTTP_BIND` | `127.0.0.1` | Numeric HTTP listener bind address |
| `FERRICSTORE_HTTP_PORT` | `8080` | HTTP/HTTPS API listener port |
| `FERRICSTORE_HTTP_TLS_ENABLED` | `false` | Require a configured certificate and key for the API listener |
| `FERRICSTORE_HTTP_TLS_CERT_FILE` | none | PEM certificate-chain path |
| `FERRICSTORE_HTTP_TLS_KEY_FILE` | none | Matching PEM private-key path |
| `FERRICSTORE_DATA_DIR` | `/data` | Bitcask + WAL data directory |
| `FERRICSTORE_SHARD_COUNT` | `0` (auto) | Number of shards (0 = CPU count) |
| `FERRICSTORE_PROTECTED_MODE` | `true` | Reject non-localhost without auth |
| `FERRICSTORE_NODE_NAME` | none | Full Erlang node name for clustering; releases automatically use it as `RELEASE_NODE`. |
| `FERRICSTORE_COOKIE` | `ferricstore` | Erlang distribution cookie. Override with a strong shared secret for any cluster; releases automatically use it as `RELEASE_COOKIE`. |
| `FERRICSTORE_CLUSTER_NODES` | none | Comma-separated peer node names |
| `FERRICSTORE_DISCOVERY` | `gossip` | Discovery strategy when `FERRICSTORE_NODE_NAME` is set. Use `dns` for Kubernetes. |
| `FERRICSTORE_EPMD_POLL_INTERVAL_MS` | `5000` | Retry interval for `epmd` discovery. Required for stable DNS names whose task IP can change. |
| `FERRICSTORE_GOSSIP_IF_ADDR` | `127.0.0.1` | Gossip bind interface. Set explicitly only for private LAN/container gossip. |
| `FERRICSTORE_GOSSIP_MULTICAST_IF` | same as `FERRICSTORE_GOSSIP_IF_ADDR` | Gossip multicast interface |
| `FERRICSTORE_GOSSIP_PORT` | `45892` | Gossip UDP port; firewall to FerricStore nodes only |

### BEAM VM Tuning

The release ships with `rel/vm.args.eex` containing production BEAM flags:

- `+P 1048576` -- max processes (headroom for many connections)
- `+Q 1048576` -- max ports/file descriptors
- `+stbt db` -- bind schedulers to CPU cores
- `+sbwt very_short` -- scheduler busy-wait for lower latency
- `+swt very_low` -- wake schedulers faster on new work
- `+sub true` -- scheduler utilization balancing
- `+A 128` -- async thread pool for file I/O
- `+MBas aobf` / `+MHas aobf` -- binary allocator strategy (address-order best-fit)
- `+Muacul 0` -- disable carrier utilization limit

### Socket Options

The TCP acceptor uses the following socket options (hardcoded in `ferricstore_server`):

| Option | Value | Purpose |
|--------|-------|---------|
| `nodelay` | `true` | Disable Nagle's algorithm for lower latency |
| `recbuf` | `65_536` | 64 KB receive buffer |
| `sndbuf` | `65_536` | 64 KB send buffer |
| `backlog` | `1024` | TCP listen backlog |
| `keepalive` | `true` | Detect dead connections |

## Docker

### Basic

```bash
docker run -p 6388:6388 \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -v ferricstore_data:/data \
  quay.io/ferricstore/ferricstore:0.12.16
```

The official image is published publicly to Quay.io:

```bash
docker pull quay.io/ferricstore/ferricstore:0.12.16
```

Current release images are published as multi-arch images for `linux/amd64`
and `linux/arm64`.

### HTTPS API

The image includes the HTTP application and exposes port `8080`, but does not
start that listener by default. Mount a certificate and key read-only, then set
the HTTP TLS variables:

```bash
docker run --rm -p 6388:6388 -p 8080:8080 \
  -e FERRICSTORE_HTTP_ENABLED=true \
  -e FERRICSTORE_HTTP_TLS_ENABLED=true \
  -e FERRICSTORE_HTTP_TLS_CERT_FILE=/run/secrets/ferricstore/http-cert.pem \
  -e FERRICSTORE_HTTP_TLS_KEY_FILE=/run/secrets/ferricstore/http-key.pem \
  -v "$PWD/certs:/run/secrets/ferricstore:ro" \
  -v ferricstore_data:/data \
  quay.io/ferricstore/ferricstore:0.12.16
```

Create a least-privilege ACL identity before sending requests. Certificate
generation, client verification, command examples, and the complete listener
configuration are in [HTTP and HTTPS API](http-api.md).

### Docker Production Notes

For write-heavy workloads, prefer a direct data mount on durable fast storage and
make sure the container runtime allows io_uring syscalls.

```bash
docker run -p 6388:6388 \
  --security-opt seccomp=unconfined \
  -e FERRICSTORE_PROTECTED_MODE=true \
  -v /mnt/nvme/ferricstore:/data \
  quay.io/ferricstore/ferricstore:0.12.16
```

#### Give the Container Time to Stop

A graceful stop closes and syncs every shard. On about 10 GB of data it takes
15 to 20 seconds, longer than Docker's default 10-second grace period, after
which Docker kills the process and the next start recovers as it would after a
crash. Allow two minutes: `docker stop -t 120`, `stop_grace_period: 2m` in
Compose, or `terminationGracePeriodSeconds: 120` in Kubernetes. The Fargate
stack already sets `stopTimeout = 120`.

#### Why io_uring Matters

Docker's default seccomp profile blocks the `io_uring_setup`, `io_uring_enter`,
and `io_uring_register` syscalls. Without them, FerricStore falls back to
synchronous `pwrite` + `fdatasync` for Bitcask writes — roughly 2-3x slower
for write-heavy workloads.

Options (pick one):
1. **`seccomp:unconfined`** — disables all seccomp filtering (simplest)
2. **Custom seccomp profile** — add only the 3 io_uring syscalls:

```json
{
  "defaultAction": "SCMP_ACT_ALLOW",
  "syscalls": [
    {"names": ["io_uring_setup", "io_uring_enter", "io_uring_register"],
     "action": "SCMP_ACT_ALLOW"}
  ]
}
```

```yaml
security_opt:
  - seccomp:./ferricstore-seccomp.json
```

#### Why NVMe Direct Mount Matters

Docker's overlay filesystem adds a VFS layer between the application and disk.
For a storage engine that does its own caching (ETS) and write-ahead logging
(WARaft segment log + Bitcask), this overhead is pure waste.

Mount the NVMe partition directly:

```bash
-v /mnt/nvme/ferricstore:/data
```

Or for maximum IOPS, use a RAM-backed tmpfs (data lost on restart):

```bash
docker run --tmpfs /data:size=8g ...
```

Cluster container examples will be documented after that layout is tested as
part of the release process.

## AWS Fargate

The repository includes two deployable ECS/Fargate profiles for FerricStore
OSS.

The disposable single-task profile is in
[`deploy/aws/fargate`](https://github.com/ferricstore/ferricstore/tree/main/deploy/aws/fargate). It uses an internal
Network Load Balancer, encrypted task-local ephemeral storage mounted at
`/data`, the isolated readiness endpoint, CloudWatch Logs, and ECS Exec.

```bash
cd deploy/aws/fargate
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

FerricStore `0.11.11+` can expose the authenticated HTTP command API from the
same task. Set `http_enabled = true` to add a private HTTP listener and
`http_endpoint` output. For production client encryption, also set a matching
`http_hostname`, an ACM/IAM `http_tls_certificate_arn`, and normally
`http_listener_port = 443`; the existing NLB terminates TLS and forwards to the
task over the private VPC. See the stack README for ACL bootstrap, unauthenticated
health/metrics routes, and the exact TLS boundary.

All data is lost when the task stops or is replaced, so this layout is for
disposable development, demos, SDK integration, CI, caches, and similar OSS
workloads. EFS is not used because FerricStore's LMDB projection cannot safely
run on NFS. Fargate cannot reattach an existing ECS-managed EBS data volume to
a replacement task.

The stack keeps the ECS desired count at one and serializes replacements. A
service replica count greater than one is not a FerricStore cluster. The stack
does not create S3, DynamoDB, EFS, or EBS resources. Use the native-release or
Kubernetes local/block-storage layouts for durable workloads. See the stack
README for networking, security, cost, and cleanup notes, and the
[single-task support contract](../docs/aws-fargate-single-task.md) for the exact
failure behavior.

The clustered profile is in
[`deploy/aws/fargate-cluster`](https://github.com/ferricstore/ferricstore/tree/main/deploy/aws/fargate-cluster). It runs
three stable logical node slots across three availability zones. Each slot has
one desired-count-one ECS service and one task-local replica. Cloud Map moves
the stable node name when ECS assigns a replacement IP, periodic EPMD discovery
reconnects it, and Raft rebuilds one blank replacement from the two survivors.

```bash
cd deploy/aws/fargate-cluster
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

The cluster profile has the same opt-in HTTP/HTTPS endpoint. One HTTP target
group follows each stable node slot, so the NLB endpoint stays fixed when Cloud
Map and ECS move a replacement task to a new IP. On a running cluster, adding
or removing those target groups must use the documented staged apply plus
`scripts/deploy-sequential.sh`; an unrestricted target-group change can deploy
all three stateful services concurrently.

Image changes are registered by Terraform but deployed only through
`scripts/deploy-sequential.sh`. It replaces one node and verifies full local
catch-up before touching the next. The cluster has no S3, DynamoDB, EFS, or EBS
data dependency. It tolerates one task-disk loss; simultaneous loss or upgrade
of two replicas is unsupported, and loss of all three loses all data. See the
[cluster support contract](../docs/aws-fargate-cluster.md) for discovery,
health, rollout, and failure details.

## Kubernetes

### Basic Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ferricstore
spec:
  replicas: 3
  template:
    spec:
      terminationGracePeriodSeconds: 120
      containers:
        - name: ferricstore
          image: quay.io/ferricstore/ferricstore:0.12.16
          ports:
            - name: native
              containerPort: 6388
            - name: health-probe
              containerPort: 6381
          env:
            - name: FERRICSTORE_PROTECTED_MODE
              value: "false"
            - name: FERRICSTORE_NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: FERRICSTORE_COOKIE
              valueFrom:
                secretKeyRef:
                  name: ferricstore-secrets
                  key: erlang-cookie
            - name: FERRICSTORE_DISCOVERY
              value: "dns"
            - name: FERRICSTORE_DNS_NAME
              value: "ferricstore-headless"
          volumeMounts:
            - name: data
              mountPath: /data
          livenessProbe:
            httpGet:
              path: /health/live
              port: health-probe
          readinessProbe:
            httpGet:
              path: /health/ready
              port: health-probe
      volumes:
        - name: data
          emptyDir: {}
```

For cluster deployments, create the `ferricstore-secrets` Secret with a strong
shared cookie before starting pods:

```bash
kubectl create secret generic ferricstore-secrets \
  --from-literal=erlang-cookie="$(openssl rand -base64 32)"
```

Use DNS discovery in Kubernetes. Gossip discovery is loopback-bound by default;
only set `FERRICSTORE_GOSSIP_IF_ADDR`/`FERRICSTORE_GOSSIP_MULTICAST_IF` when
you intentionally want multicast gossip on a private pod/node network and have
firewalled `FERRICSTORE_GOSSIP_PORT`.

### Optimized for Production

#### Enable io_uring

Kubernetes uses the container runtime's seccomp profile. To allow io_uring:

**Option A: Pod-level seccomp (Kubernetes 1.19+)**

```yaml
spec:
  securityContext:
    seccompProfile:
      type: Unconfined    # or use a custom profile
```

**Option B: Custom seccomp profile**

Place the profile on each node at `/var/lib/kubelet/seccomp/ferricstore.json`:

```json
{
  "defaultAction": "SCMP_ACT_ALLOW",
  "syscalls": [
    {"names": ["io_uring_setup", "io_uring_enter", "io_uring_register"],
     "action": "SCMP_ACT_ALLOW"}
  ]
}
```

```yaml
spec:
  securityContext:
    seccompProfile:
      type: Localhost
      localhostProfile: ferricstore.json
```

#### NVMe Storage

Use a StorageClass backed by local NVMe SSDs:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: nvme-local
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer

---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: nvme-pv
spec:
  capacity:
    storage: 100Gi
  storageClassName: nvme-local
  local:
    path: /mnt/nvme
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values: ["node-with-nvme"]

---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ferricstore-data
spec:
  storageClassName: nvme-local
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 100Gi
```

Then in the deployment:
```yaml
volumeMounts:
  - name: data
    mountPath: /data
volumes:
  - name: data
    persistentVolumeClaim:
      claimName: ferricstore-data
```

#### CPU Pinning

For consistent latency, pin FerricStore pods to dedicated CPUs:

```yaml
resources:
  requests:
    cpu: "4"
    memory: "8Gi"
  limits:
    cpu: "4"
    memory: "8Gi"
```

With `static` CPU manager policy on the kubelet, this guarantees exclusive cores.

## Data Formats

Each data directory records its on-disk format in a `data_format` file.

- **Format 2**, the default for new data directories, keys Flow runs with
  128-bit digests (22 characters). Measured on completed runs, that is about
  1.7 KB less disk and 360 B less keydir memory per run than format 1. It also
  stores record sidecars as compact terms and writes Raft log entries in a
  compact form (Flow entries about 40% smaller), which only releases with
  format 2 can read.
- **Format 1** is what data directories created before format 2 use (full
  256-bit digests, 43 characters). They keep it: upgrading the release changes
  nothing on disk.

A node runs in its data directory's format, and a node in a different format
cannot join the cluster. The `:data_format` setting only chooses the format of
a new data directory.

### Moving Data to Format 2

A format 1 cluster moves to format 2 by copying its data into a new cluster:

1. Upgrade the old cluster to this release. It keeps running in format 1.
2. Stop the applications that write to it.
3. Export from any node of the old cluster:

   ```bash
   bin/ferricstore rpc 'IO.inspect(FerricStore.export("/backup/store.ffx"))'
   ```

4. Start a new cluster on empty data directories; they are created in format 2.
5. Restore on any node of the new cluster:

   ```bash
   bin/ferricstore rpc 'IO.inspect(FerricStore.restore("/backup/store.ffx"))'
   ```

6. Point the applications at the new cluster.

Both calls return
`{:ok, %{keys: n, flows: n, shared_values: n, policies: n, governance: n}}`.

What moves: every key (strings, bitmaps, HyperLogLogs, t-digests, hashes,
lists, sets, sorted sets, streams with their consumer groups, pending entries
and consumers, Bloom and cuckoo filters, count-min sketches and top-k) with its
expiry; the invocation definitions; every run, schedules included, with its id, type, state, version,
attempts, lease, timestamps, values, history and signal idempotency keys; the
standalone shared values; the type policies; governance state: circuits,
budgets with their reservations, limits with their leases and reservations,
approvals, and each run's effects and governance ledger. A worker holding a
lease from the old cluster can finish its run on the new one, and its effects
and its running-limit slot carry over with it.

What to know:

- A probabilistic structure travels as one replicated command, so it must fit
  the new cluster's command size limits.
- Value refs encode the key layout, so they change. Refs stored inside runs are
  rewritten; refs an application kept from `flow_get` or `flow_value_put`
  point at the old cluster.
- Policy generations start over. A running limit (`limits: %{"running" => ...}`
  in a policy) picks up the new cluster's policy on its next claim.
- Limit-cache sessions belong to the old cluster's nodes and are not carried.
  Credit a node had prefetched but not used returns when its lease expires.
- The export waits for limit releases that finished runs queued and refuses if
  they are still pending after about 20 seconds; run it again once they drain.
- The restore refuses, before writing anything, a file with a limit lease on a
  logical shard the new cluster does not have (a new cluster with fewer
  shards).
- The export refuses Flow data it cannot carry (for example a policy migration
  in progress) and names sample keys.
- The restore checks the file's checksum before writing anything and refuses a
  cluster that already holds keys or Flow data. If a restore stops partway, start the
  new cluster again on empty data directories and rerun it; the export file is
  unchanged and can be restored any number of times.

## Flows Without a State Record

A Flow keeps its state record in memory or, once finished or hibernated, in the
Flow LMDB mirror. Older releases could lose that record while the Flow's
registry marker, retention guard, catalog entry and values stayed. Such a Flow
cannot be read back, and retention, which starts from the state record, never
removes what it left. The policy catalog backfill skips them and logs how many it
skipped per shard.

List them, then remove what they left (the purge needs feature level 3 and goes
through Raft; a Flow whose retention guard changed since it was found is left
alone):

```bash
bin/ferricstore rpc 'IO.inspect(FerricStore.flow_purge_orphans(), limit: 5)'
# {:ok, %{dry_run?: true, orphans: [%{shard_index: 4, state_key: ..., ...}, ...], purged: 0, skipped: 0}}

bin/ferricstore rpc 'IO.inspect(FerricStore.flow_purge_orphans(dry_run: false), limit: 5)'
# {:ok, %{dry_run?: false, orphans: [...], purged: 982, skipped: 0}}
```

## Starting a New Cluster

A new cluster starts at feature level 0, the behavior every release applies
the same way, and stays there until you raise it, even when every node runs
the same release. Several fixes only take effect from a level on: at level 4
HDEL, HSETNX, HGETDEL and ZADD/GEOADD with NX, XX, GT or LT run as one
replicated entry (below it, concurrent clients can each see the same field
deleted or set), member writes check the key's type, and INCRBYFLOAT stores
"3" rather than "3.0"; at level 6 a member added to a collection that has a
TTL takes it, INCR and HINCRBY refuse stored integers Redis does not read
("05", "+5"), and TDIGEST writes and DEL each run as one replicated entry
(below it, clients on different nodes can lose each other's TDIGEST values
or both count the same deleted key); at level 7 a claim without a partition
key takes due Flows that were hibernated (below it, a Flow created without a
partition key and due more than the hot window away, five minutes by default,
is never claimed by one); at level 8 every replica claims the same Flows
(below it, a node whose LMDB writer is behind, or that restarted, can claim a
different hibernated Flow than the others); at level 9 a large sorted set lives
in its own counted tree file, so ZRANK and ZRANGE by index stay O(log N) (below
it they walk the set from its start); at level 10 MULTI takes ZUNION, ZINTER,
ZDIFF, their STORE forms, ZINTERCARD, ZRANGESTORE, ZRANGEBYLEX, ZREVRANGEBYLEX,
ZLEXCOUNT, the ZREMRANGEBY* commands and ZMPOP; at level 11 DEL, UNLINK, SET,
RENAME, COPY, EXPIRE, PERSIST and the STORE forms work on collections past the
per-command member bound (below it they are refused there); at level 12
EXPIRE on a collection, PERSIST and EXPIRE with a flag change the TTL in one
compound entry (below it they run as one-command transactions, 20-40 ms
each on macOS). After the first
start of a new cluster, once every node is up, raise it to the level the
release supports:

```bash
bin/ferricstore rpc 'IO.inspect(FerricStore.cluster_feature_level())'
# {:ok, %{supported: 12, active: 0, shards: [0, 0, 0, 0]}}

bin/ferricstore rpc 'IO.inspect(FerricStore.raise_cluster_feature_level(11))'
```

## Upgrading a Cluster

Upgrade nodes one at a time (rolling upgrade). New behavior that older releases
would apply differently stays off until you raise the cluster's feature level,
so a mixed-version cluster keeps applying every write the same way on every
node.

The first start on a release after 0.12.8 reads each shard's log files
instead of the hint files an older release wrote, once, so it takes longer
than later starts. Releases before 0.12.7 could write a hint that lists a
promoted collection's rows as the shard's own; 0.12.7 and 0.12.8 trusted such
a hint and stopped at start with "failed to rebuild logical key index on shard
N: :invalid_type_metadata_values", as does a restart of the release that wrote
it. A node stuck there starts once its hint files are gone: stop it, back up
the data directory, delete `data/shard_*/*.hint` and `data/shard_*/*.hint.meta`
under it, and start it again. The shard then reads its log files instead.

After every node runs the new release, check and raise the level:

```bash
bin/ferricstore rpc 'IO.inspect(FerricStore.cluster_feature_level())'
# {:ok, %{supported: 2, active: 0, shards: [0, 0, 0, 0]}}

bin/ferricstore rpc 'IO.inspect(FerricStore.raise_cluster_feature_level(2))'
# {:ok, %{supported: 2, active: 2, shards: [2, 2, 2, 2]}}
```

The same from a client (both commands need the `@admin` ACL category, and
`CLUSTER.FEATURES.RAISE` also `@dangerous`):

```text
> CLUSTER.FEATURES
supported: 2
active: 0
shard_0: 0
...
> CLUSTER.FEATURES.RAISE 2
supported: 2
active: 2
shard_0: 2
...
```

- The raise checks every member first. If a node still runs an older release it
  returns `{:error, {:members_below_feature_level, level, [{node, supported}]}}`;
  if a node is unreachable it returns `{:error, {:feature_level_probe_failed, ...}}`.
  Nothing changes in either case.
- Each level turns on:
  - 1: `flow_policy_set(type, replace: true, ...)` repairs a corrupt stored
    policy.
  - 2: running Flows, schedules included, release the values their trimmed
    history no longer refers to (without it a schedule's keydir grows with every
    fire and every overlap check), and rewinds refuse events whose values were
    released.
  - 3: `FerricStore.flow_purge_orphans(dry_run: false)` removes what a Flow left
    behind once its state record is gone (see "Flows Without a State Record").
  - 4: a write of hash, set or sorted set members checks the key's type in the
    same replicated entry. HINCRBY, HINCRBYFLOAT, HSETNX, HSETEX, ZADD and
    GEOADD with options and SMOVE check the type before they write; without
    level 4 a SET landing between the check and the write leaves the members
    hidden under the string, to come back when the key is recreated. Level 4
    also stores INCRBYFLOAT and HINCRBYFLOAT results as Redis does ("3",
    "5200"), so INCR and HINCRBY work on them afterwards, and runs HDEL,
    HSETNX, HGETDEL and ZADD and GEOADD with options as one replicated entry
    each, so concurrent clients see one outcome (one HDEL removes the field)
    across the whole cluster. Inside MULTI, EXPIRE with several flags
    (`EXPIRE key 100 XX GT`), BITCOUNT ranges and BITPOS are queued only at
    level 4: older releases abort or answer such a transaction differently.
  - 5: Redis 8 behavior that older releases apply differently, so it waits
    until every node runs this release:
    - MULTI queues `ZRANK`/`ZREVRANK ... WITHSCORE`, `PFMERGE` with no
      source, `HEXPIRE`/`HPEXPIRE` with NX, XX, GT or LT or a TTL of 0,
      `HSETEX`'s Redis 8 form, `HGETEX` with a TTL of 0, `ZADD ... INCR`,
      `ZRANGE ... BYLEX` and `SET ... IFEQ/IFNE`.
    - Inside MULTI, BITOP deletes its destination for an empty result,
      GEOSEARCHSTORE empties its destination for a missing source and
      measures with Redis's Earth radius, and HGETEX with a past time deletes
      the fields, as they do outside MULTI.
    - HSETEX's Redis form and a conditional `ZADD ... INCR` run as one
      replicated entry, so concurrent clients see one outcome.
    - PFADD and PFMERGE keep the sketch's TTL.
    - WATCH records a replicated write version for each watched key and EXEC
      compares them, so every replica reaches the same decision. Below level
      5 a watched EXEC on a promoted hash, set or sorted set, or on a key a
      node had relocated, could apply on some replicas and not others.
    - Apply's Flow history trim and duplicate-retry check order one
      millisecond's events by version (`1000-9` before `1000-10`).
    - Flow signal idempotency digests and child group request hashes no
      longer depend on the OTP release, so a rolling OTP upgrade cannot make
      replicas disagree on a retried signal or spawn.
    - A Flow rewind carries its target event, so every replica restores the
      same event or refuses it alike; only events within the Flow's history
      cap can be restored.
    - A Flow create batch holding an idempotent retry, and a rewind's value
      refresh, find values the history projector has moved to the LMDB
      mirror, so replicas agree however far their projectors have got.
    - A Flow signal retried with its idempotency key and the same content is
      applied once.
  - 8: Flow hibernation keeps a Flow's keydir row and index entries and drops
    only its cached record, so every replica claims from the same Flows
    whenever its own LMDB writer gets to the Flow, and after a restart. Below
    level 8 each node removed them on its own schedule and a claim took the
    Flows still indexed first, so replicas could claim different Flows and a
    client could hold a lease the other replicas never granted. A hibernated
    Flow now keeps about 1 KB of memory for those entries. Raising to 8
    brings back, on each shard, the Flows hibernated before it: the shard
    reads each one's record once and applies nothing else meanwhile, so the
    raise takes longer on a shard with many of them.
  - 9: a sorted set past the promotion threshold (100 members by default)
    moves into its own file under `dedicated/shard_N/`: a counted B+tree read
    and written by stateless NIF calls, with no member kept in memory and
    nothing rebuilt after a restart. ZRANK, ZREVRANK and ZRANGE by index cost
    O(log N) instead of a walk from the set's start. A set promoted to a
    dedicated Bitcask before the raise moves on its first write after it. The
    storage compacts these files and removes the ones no set names; see
    `docs/sorted-set-tree-design.md`.
  - 10: MULTI queues ZUNION, ZINTER, ZDIFF, ZUNIONSTORE, ZINTERSTORE,
    ZDIFFSTORE, ZINTERCARD, ZRANGESTORE, ZRANGEBYLEX, ZREVRANGEBYLEX,
    ZLEXCOUNT, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX and ZMPOP;
    releases before it do not have these commands, so a transaction holding
    one would apply on them differently. The ones that write (the STORE
    forms, ZRANGESTORE, the ZREMRANGEBY* commands and ZMPOP) run as
    one-command transactions: the shard decides from what it has applied and
    writes with nothing in between. Below level 10 they read on the node the
    client is connected to, which may not have applied the latest write yet,
    and write as separate steps.
  - 11: one replicated entry applies at most `:compound_member_apply_budget`
    members (4,096 by default) and reads at most
    `:transaction_result_byte_budget` bytes (8 MiB). Commands whose members
    come from stored data rather than from the request apply past them:
    DEL, UNLINK and SET over a collection remove every member, a page of the
    member bound at a time; RENAME, RENAMENX, COPY, EXPIRE, PEXPIRE,
    EXPIREAT, PEXPIREAT, PERSIST, ZREMRANGEBY* and the STORE forms apply with
    the largest bounds, 100,000 members and 64 MiB. Past those they are
    refused whole with `ERR too many members in one command (the limit is
    100000)`. SUNIONSTORE, SINTERSTORE and SDIFFSTORE run as one-command
    transactions, as the sorted-set STORE forms do from level 10. Below
    level 11 these commands are refused for a collection past the configured
    bound. Applying one entry holds the shard's writes for its duration:
    about 0.7 s for a RENAME of 20,000 members on macOS. SPOP picks its
    members evenly from the set's members (below level 11 a set whose
    members share a prefix, such as `m1`..`m200`, gave up its first members
    in order); every replica still picks the same. XCLAIM, XAUTOCLAIM,
    XSETID and XGROUP DESTROY, CREATECONSUMER, DELCONSUMER and SETID are
    replicated stream operations older releases do not have: they run from
    level 11 and answer `ERR ... needs cluster feature level 11` below it.
  - 12: EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT and PERSIST on a collection,
    and each of them with NX, XX, GT or LT on any key, run as one compound
    command entry instead of a one-command transaction, whose journal costs
    two synced appends: on macOS EXPIRE on a hash fell from 20 to 0.3 ms.
    The entry keeps level 11's bounds, 100,000 members and 64 MiB, checked
    before a member's value is read. Older releases refuse the entry.
- Raising is safe to repeat, including after a partial failure.
- Levels never go down. Do not downgrade a node below the active level.
- Once a level is active, a node whose release does not support it cannot join.
- FLUSHDB keeps the feature level.

## Performance Checklist

Before benchmarking or going to production:

- [ ] **io_uring enabled** — check with `cat /proc/sys/kernel/io_uring_disabled` (should be `0`)
- [ ] **NVMe direct mount** — not Docker overlay or network block storage
- [ ] **CPU pinning** — FerricStore on dedicated cores, not shared
- [ ] **No swap** — `vm.swappiness=0` or `mem_swappiness: 0`
- [ ] **Network** — private network between cluster nodes, 10Gbps+
- [ ] **Shard count** — matches CPU count (default behavior)
- [ ] **Protected mode** — disabled if behind a firewall, or configure ACL
