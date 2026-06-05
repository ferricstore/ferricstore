# Deployment Guide

This guide covers running FerricStore as a server: native release, Docker, Kubernetes, and cluster layouts. For local development, start with [Getting Started](getting-started.md). For production security, pair this guide with [Security](security.md).

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
| `FERRICSTORE_PORT` | `6379` | TCP listen port |
| `FERRICSTORE_HEALTH_PORT` | `6380` | Health endpoint port |
| `FERRICSTORE_DATA_DIR` | `/data` | Bitcask + WAL data directory |
| `FERRICSTORE_SHARD_COUNT` | `0` (auto) | Number of shards (0 = CPU count) |
| `FERRICSTORE_PROTECTED_MODE` | `true` | Reject non-localhost without auth |
| `FERRICSTORE_NODE_NAME` | none | Erlang node name for clustering |
| `FERRICSTORE_COOKIE` | `ferricstore` | Erlang distribution cookie |
| `FERRICSTORE_CLUSTER_NODES` | none | Comma-separated peer node names |

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
docker run -p 6379:6379 \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -v ferricstore_data:/data \
  ghcr.io/ferricstore/ferricstore:0.4.1
```

The official image is published to GitHub Container Registry:

```bash
docker pull ghcr.io/ferricstore/ferricstore:0.4.1
```

Current release images are published for `linux/amd64`. Native `linux/arm64`
images should be built on native arm64 runners or added through a future
multi-arch manifest.

### Optimized for Production

For maximum write throughput, enable io_uring and use NVMe storage:

```yaml
# docker-compose.yml
services:
  ferricstore:
    image: ghcr.io/ferricstore/ferricstore:0.4.1
    ports:
      - "6379:6379"
    environment:
      FERRICSTORE_PROTECTED_MODE: "false"

    # Enable io_uring syscalls (blocked by Docker's default seccomp)
    security_opt:
      - seccomp:unconfined

    # Mount NVMe directly (bypass Docker overlay filesystem)
    volumes:
      - /mnt/nvme/ferricstore:/data

    # Pin to specific CPUs for consistent performance
    cpuset: "0-7"

    # Disable memory swap
    mem_swappiness: 0
```

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
```yaml
volumes:
  - /mnt/nvme/ferricstore:/data    # Direct mount, no overlay
```

Or for maximum IOPS, use a RAM-backed tmpfs (data lost on restart):
```yaml
tmpfs:
  - /data:size=8g
```

### 3-Node Cluster with HAProxy

```bash
docker compose up -d
```

See `docker-compose.yml` for the full configuration. HAProxy runs in TCP
mode (`mode tcp`) for zero-overhead load balancing.

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
      containers:
        - name: ferricstore
          image: ghcr.io/ferricstore/ferricstore:0.4.1
          ports:
            - containerPort: 6379
          env:
            - name: FERRICSTORE_PROTECTED_MODE
              value: "false"
            - name: FERRICSTORE_NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          emptyDir: {}
```

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

## Performance Checklist

Before benchmarking or going to production:

- [ ] **io_uring enabled** — check with `cat /proc/sys/kernel/io_uring_disabled` (should be `0`)
- [ ] **NVMe direct mount** — not Docker overlay or network block storage
- [ ] **CPU pinning** — FerricStore on dedicated cores, not shared
- [ ] **No swap** — `vm.swappiness=0` or `mem_swappiness: 0`
- [ ] **Network** — private network between cluster nodes, 10Gbps+
- [ ] **Shard count** — matches CPU count (default behavior)
- [ ] **Protected mode** — disabled if behind a firewall, or configure ACL
