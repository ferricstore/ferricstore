# AWS ECS-on-EC2 Cluster Support

Status: supported OSS profile with three retained, instance-local replica
volumes and an explicit one-replica replacement contract.

Implementation:
[`deploy/aws/ecs-cluster`](https://github.com/ferricstore/ferricstore/tree/main/deploy/aws/ecs-cluster)

The Terraform input intentionally has no default image. Use an image built from
this repository revision or a later release and pin its digest. The HTTP API
requires `0.11.11` or later. Older images also predate parts of the stable
release identity, periodic EPMD reconnect, and rollout recovery contract.

## What An ECS Task And Container Instance Mean

The closest Kubernetes mapping is:

| Amazon ECS | Kubernetes | FerricStore cluster profile |
|---|---|---|
| Task definition revision | Pod template | Image, ports, environment, health, and disk size for one node |
| Running task | Pod | One FerricStore process and its task-lifetime `/data` mount |
| ECS service | Stateful controller for one slot | Keeps exactly one logical node running and replaces it when necessary |
| ECS cluster | Scheduler boundary | Runs the three services; it is not the FerricStore Raft cluster |
| EC2 container instance | Node | One per AZ, owned by a per-slot capacity provider, with a retained encrypted root volume |
| Cloud Map service | Headless per-pod DNS identity | Moves one stable slot name to its current task IP |

A single ECS task cannot be a fault-tolerant cluster. This profile is one
Terraform deployment with three tasks on three EC2 instances, because a
three-voter Raft cluster needs three independently failing processes and disks.

## Architecture

```mermaid
flowchart TD
  C["Native or HTTP clients<br/>in private network"] --> NLB["Internal Network Load Balancer"]
  NLB --> N0["node-0 ECS service / AZ A"]
  NLB --> N1["node-1 ECS service / AZ B"]
  NLB --> N2["node-2 ECS service / AZ C"]
  CM["Cloud Map private DNS"] --> N0
  CM --> N1
  CM --> N2
  N0 <-->|"Raft + Erlang distribution"| N1
  N1 <-->|"Raft + Erlang distribution"| N2
  N2 <-->|"Raft + Erlang distribution"| N0
  N0 --> D0["instance-local /data"]
  N1 --> D1["instance-local /data"]
  N2 --> D2["instance-local /data"]
```

Each slot has one stable logical node name, one Cloud Map A record, one ECS
service, one task definition family, one EC2 capacity provider/Auto Scaling
group, one native NLB target group, and an optional HTTP target group. Its
private IP and instance-local disk are replaceable implementation details.

## How Nodes Discover One Another

1. ECS launches the task with an `awsvpc` task ENI on the per-slot EC2
   container instance. The IP is not stable and the ENI is removed when the
   task stops.
2. ECS registers that IP in the slot's Cloud Map service. The names are
   `node-0.<namespace>`, `node-1.<namespace>`, and `node-2.<namespace>`.
3. The task waits until its own stable DNS name resolves to its current IP. This
   prevents an old five-second DNS answer from becoming its advertised name.
4. FerricStore starts with the stable BEAM identity
   `ferricstore@node-N.<namespace>` and a strong cookie shared by all nodes.
   The task sets long-name release distribution plus
   `RELEASE_NODE`/`RELEASE_COOKIE` (before the BEAM starts) and FerricStore's
   matching runtime variables. Startup fails closed if those identities
   disagree.
5. Every node has the same explicit `FERRICSTORE_CLUSTER_NODES` list. The
   `epmd` libcluster strategy retries the list every five seconds, resolving DNS
   again on each connection attempt.
6. EPMD on TCP `4369` tells a peer to use the fixed Erlang distribution port
   `9100`. The task security group permits both ports only from itself.
7. A successful connection emits `nodeup`. `Ferricstore.Cluster.Manager`
   recognizes the name as a configured voter, cancels any delayed removal, and
   drives Raft recovery. A blank replacement receives missing snapshots and log
   entries from the surviving quorum.

The NLB DNS name is the stable client bootstrap endpoint. SDK route metadata
advertises the three per-slot Cloud Map names, so clients never need a raw task
IP.

### Protecting An Existing Cluster's Volumes

The launch template applies `delete_on_termination = false` to new EC2
instances. If a cluster was created before that setting was applied, update
the termination flag on each currently running `node-0`, `node-1`, and `node-2`
instance once, before any planned replacement:

```bash
for instance_id in $(aws ec2 describe-instances \
  --filters \
    'Name=tag:NodeSlot,Values=node-0,node-1,node-2' \
    'Name=instance-state-name,Values=running' \
  --query 'Reservations[].Instances[].InstanceId' \
  --output text); do
  root_device_name="$(aws ec2 describe-instances \
    --instance-ids "${instance_id}" \
    --query 'Reservations[0].Instances[0].RootDeviceName' \
    --output text)"
  aws ec2 modify-instance-attribute \
    --instance-id "${instance_id}" \
    --block-device-mappings "[{\"DeviceName\":\"${root_device_name}\",\"Ebs\":{\"DeleteOnTermination\":false}}]"
done
```

This changes only the EC2 termination policy; it does not stop the instance,
detach the volume, or modify FerricStore data. Verify the setting with
`aws ec2 describe-instances` before continuing a rollout.

AWS documents that each `awsvpc` ECS task receives its own ENI, ECS service
discovery registers the task private IP, and the DNS form is
`<service>.<namespace>`: [ECS task networking](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-networking.html),
[ECS service discovery](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-discovery.html).

## HTTP API And Client Discovery

Set `http_enabled = true` to run the in-process HTTP command listener on port
`8080` in each of the same three FerricStore containers. This does not create a
sidecar, another ECS service, or another FerricStore cluster. Terraform adds:

- the HTTP port and environment to every node task definition;
- one HTTP IP target group per stable slot;
- a second listener on the existing internal NLB; and
- a second ECS target-group registration for each service.

HTTP clients use the single `http_endpoint` output, not task IPs or per-node
Cloud Map names. ECS re-registers a replacement task's new ENI address in both
target groups, while the NLB DNS name remains unchanged. Amazon ECS supports
multiple target groups for ECS services and requires `ip` targets for
`awsvpc` tasks: [multiple target groups](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/register-multiple-targetgroups.html),
[NLB with ECS](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/nlb.html).

The API uses the same command gateway and replicated ACL catalog as the native
protocol. `POST /v1/commands` always requires HTTP Basic credentials. Create a
least-privilege ACL identity through a trusted native administration connection
before enabling application traffic. `GET /health`, `GET /ready`, and the HTTP
listener's `GET /metrics` do not require credentials, so the NLB must remain
internal and `allowed_client_cidr_blocks` must be kept narrow.

The stack does not provision application users through Terraform. Send `ACL
SETUSER` once through a VPC-connected native FerricStore client after cluster
readiness; the committed user is then available on every node through the
replicated ACL catalog. See the stack
[Create An HTTP User](../deploy/aws/ecs-cluster/README.md#create-an-http-user)
procedure. A surviving quorum restores the user to a replacement node, while
loss of every instance-local replica loses it with the rest of the cluster data.

Plain HTTP is supported only for a trusted private network. For client TLS,
provide an ACM/IAM `http_tls_certificate_arn`, set `http_hostname` to a DNS name
covered by that certificate, and normally use `http_listener_port = 443`. The
operator must create that private DNS alias to the NLB. The NLB then terminates
TLS 1.2/1.3 and forwards plaintext TCP to task port `8080` inside the private
VPC. Application-native TLS is still required when policy demands encryption
all the way to the task. AWS documents NLB certificate termination and TCP
targets in [TLS listeners](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-listeners.html).

### Changing HTTP Mode On A Running Cluster

Adding or removing an ECS target group starts a service deployment. A normal
Terraform apply can update all three services concurrently, which is unsafe for
instance-local replica disks. For a live cluster, stage the change and let the
guarded rollout combine the desired task definition and target-group list one
slot at a time:

1. Edit `http_enabled` and the related listener/TLS variables.
2. When enabling, use a targeted apply for `aws_ecs_task_definition.node`,
   `aws_lb_target_group.http`, and the two optional HTTP listener resources.
   When disabling, target only `aws_ecs_task_definition.node` first.
3. Run `scripts/deploy-sequential.sh`. It updates the task definition and exact
   native/HTTP target-group list for one service, waits for ECS stability and
   full replica recovery, and only then continues.
4. Run a normal `terraform apply` to reconcile or remove remaining resources.

The stack README contains the exact commands. This migration is separate from
an ordinary image-only apply because the ECS load-balancer configuration itself
can replace tasks.

## Replacement Sequence

For one failed slot:

1. The old task stops; its IP and ENI are lost. Its `/data` remains when the EC2
   container instance survives. If the instance is replaced, AWS retains the
   old encrypted root volume as a tagged unattached EBS volume, while the new
   instance starts with a fresh root and an empty `/data` directory.
2. The other two voters retain quorum and continue serving.
3. The slot's ECS service starts a blank replacement in its assigned AZ.
4. ECS changes the Cloud Map record to the replacement IP.
5. Peer discovery retries the unchanged logical node name and reconnects.
6. Raft catches the replacement up from the surviving nodes.
7. The recovery check becomes true only when all configured nodes are connected,
   all shards have full membership, and the replacement's durable position has
   reached within ten entries of each shard leader's durable position. A small
   bound is necessary because normal background work can advance a leader
   between the local and remote samples; a snapshot-scale lag remains closed.

The repository includes a cluster integration test that kills a node, writes
while it is absent, restarts the same logical identity with a new empty data
directory, and verifies both old and intervening data on the replacement.

## Failure And Change Contract

| Event | Expected behavior | Supported? |
|---|---|---|
| Process or task failure in one slot | ECS creates a replacement; two survivors keep quorum; replacement catches up | Yes |
| One AZ unavailable | Its pinned slot remains absent; the other two nodes keep quorum | Yes, while the remaining nodes and network are healthy |
| One task receives a new IP | Cloud Map moves the stable name; periodic EPMD discovery reconnects | Yes |
| Sequential image upgrade | One blank task at a time, with a full-recovery gate between slots | Yes, through the supplied script |
| DNS briefly returns an old address | Task startup waits for its own address; peers retry every five seconds | Yes |
| Temporary loss of quorum | Readiness fails, but liveness does not ask ECS to destroy more replica disks | Degraded until quorum returns |
| Two instance/root volumes lost or replaced together | Only one old replica may remain; quorum and safe automatic recovery are not guaranteed | No |
| All three active instances are lost | No automatic quorum/data recovery; retained old EBS volumes require manual recovery before serving the original database | Not automatic; restore manually |
| Retained EBS volumes are manually deleted without a snapshot | No remaining source exists from which to rebuild | Data loss |
| Autoscaling or desired count other than one per slot | Duplicate identities or uncoordinated members | No |
| Parallel `aws ecs update-service` on multiple slots | Multiple local copies disappear together | No |
| Client caches a raw task IP | Connection breaks after replacement | No; clients must use NLB/Cloud Map names |

ECS task retirement and EC2 container-instance replacement are normal platform
events, not rare disasters. AWS describes task maintenance in
[ECS task maintenance](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-maintenance.html).

## Why ECS Health Uses Liveness

Kubernetes separates readiness from restart: an unready Pod can stay alive and
recover. An ECS service can treat failed container or load-balancer health as a
reason to stop and replace its task. With instance-local disks, a readiness-induced
replacement loop would repeatedly erase a recovering replica and could cascade
during a quorum outage.

Therefore the ECS container and both native/HTTP NLB target groups check the
isolated `/health/live` route on port `6381`. They do not use the API listener's
overload-sensitive `/health` route. Operators can use `/health/ready` and the
HTTP API's `/ready`; sequential upgrades use the stricter
`Ferricstore.Cluster.Recovery.ready?()` check. This prioritizes retaining local
replicas over hiding every recovering target from the NLB. Applications should
retry transient errors during a replacement.

AWS documents unhealthy task replacement and deployment percentages in
[ECS service behavior](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs_services.html).

## Upgrade Safety

Each ECS service uses minimum healthy percent `0` and maximum percent `100`.
That intentionally stops the old task before starting its replacement, because
two simultaneous tasks with the same Erlang node name are unsafe.

Terraform ignores service `task_definition` changes. Applying a new image only
registers new revisions, and `skip_destroy` keeps the service's previous
revision active so ECS can still replace its old task before the rollout reaches
that slot. The supplied rollout script then:

1. updates one service with the latest task definition and desired native/HTTP
   target-group list;
2. waits for ECS stability;
3. uses ECS Exec to poll the node's strict recovery status;
4. refuses to continue if recovery does not converge; and
5. repeats for the next slot.

This protects image changes and the explicitly staged HTTP-mode migration.
Other Terraform changes to an ECS service, load balancer, network, or service
registry can independently start a deployment; those changes require their own
one-slot-at-a-time maintenance plan.

## Storage And The No-S3/No-DynamoDB Decision

FerricStore data is stored only on three encrypted EC2 root volumes. There is
no S3, DynamoDB, or service-managed EBS data plane in this profile. S3 and
DynamoDB are not required for node discovery or normal Raft operation. The
launch template sets `delete_on_termination = false` and tags each root volume
with its node slot, so an instance replacement or stack teardown does not
physically delete the volume. A replacement instance still starts with its own
fresh root and is rebuilt from the surviving replicas; the retained old volume
is a manual recovery source, not an automatic attachment.

The tradeoff is mathematical rather than AWS-specific: replication can rebuild
one missing copy only while enough other copies remain. An orchestrator can
restart tasks, but cannot reconstruct data after all authoritative copies are
gone from the active cluster. An instance replacement retains the old root
volume but starts the replacement node on a fresh root, which is rebuilt from
the surviving replicas. See
[ECS container instances](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs_container_instances.html).

AWS Secrets Manager is used only for the Erlang cookie so all nodes can
authenticate distribution connections without writing the secret into
Terraform state. It does not store FerricStore data or membership.

## Prometheus And ECS Telemetry

FerricStore exposes Prometheus text metrics at `GET /metrics` on the dashboard
port, `6380`. Prometheus should scrape every stable Cloud Map node name directly:

- `node-0.ferricstore.local:6380`
- `node-1.ferricstore.local:6380`
- `node-2.ferricstore.local:6380`

Do not scrape only the NLB. The NLB can route consecutive scrapes to different
tasks, which hides a missing replica and mixes three processes into one target.
The optional HTTP API's `/metrics` route is also load-balanced and focuses on
HTTP listener activity; it is not a replacement for per-node cluster scraping.
The scraper must run in the VPC, a connected network that can resolve the
private namespace, or as another ECS service in the VPC.

### Permit Only The Prometheus Scraper

The cluster task security group does not admit port `6380` by default. Add a
security-group-to-security-group rule rather than opening metrics to the whole
VPC. For an existing Prometheus ECS service, this Terraform extension is enough:

```hcl
variable "prometheus_security_group_id" {
  description = "Security group attached to the private Prometheus scraper."
  type        = string
}

resource "aws_vpc_security_group_ingress_rule" "prometheus_metrics" {
  security_group_id            = aws_security_group.task.id
  description                  = "FerricStore metrics from Prometheus only"
  from_port                    = 6380
  to_port                      = 6380
  ip_protocol                  = "tcp"
  referenced_security_group_id = var.prometheus_security_group_id
}
```

The supplied FerricStore profile sets protected mode to `false` and relies on
private networking. If protected mode is enabled, `/metrics` requires an
authorized observability identity; configure the scraper credentials and TLS
according to the security deployment rather than making the endpoint public.

### Prometheus Scrape Configuration

Use the stable DNS names as targets. DNS is resolved again after a failed or
closed connection, so a target continues to work when Cloud Map moves its name
to a replacement task IP.

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - /etc/prometheus/ferricstore-alerts.yml

scrape_configs:
  - job_name: ferricstore
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets: ["node-0.ferricstore.local:6380"]
        labels:
          node_slot: node-0
      - targets: ["node-1.ferricstore.local:6380"]
        labels:
          node_slot: node-1
      - targets: ["node-2.ferricstore.local:6380"]
        labels:
          node_slot: node-2
```

The scrape exports process, client, memory, persistence, replay-lag, Flow, and
quorum-write metrics. Prometheus automatically adds the `up` metric for each
target, so `up{job="ferricstore"} == 0` identifies the exact unavailable slot.

### Probe Readiness Separately

`/metrics` can remain reachable while a node is alive but unable to serve
because it lacks quorum. Run Prometheus Blackbox Exporter in the private network
and probe each node's isolated `GET /health/ready` endpoint on port `6381`:

```yaml
  - job_name: ferricstore-readiness
    metrics_path: /probe
    params:
      module: [http_2xx]
    static_configs:
      - targets:
          - http://node-0.ferricstore.local:6381/health/ready
          - http://node-1.ferricstore.local:6381/health/ready
          - http://node-2.ferricstore.local:6381/health/ready
    relabel_configs:
      - source_labels: [__address__]
        target_label: __param_target
      - source_labels: [__param_target]
        target_label: instance
      - target_label: __address__
        replacement: blackbox-exporter.monitoring.local:9115
```

Replace the exporter hostname with its private service-discovery name. The
ECS stack permits port `6381` from within its VPC. A failed probe
is exported as `probe_success == 0`. The Blackbox Exporter pattern and relabeling
are documented by [Prometheus](https://prometheus.io/docs/guides/multi-target-exporter/).

### Starter Alert Rules

Store the following as `/etc/prometheus/ferricstore-alerts.yml` and route the
alerts through Alertmanager:

```yaml
groups:
  - name: ferricstore-ecs
    rules:
      - alert: FerricStoreNodeMetricsDown
        expr: up{job="ferricstore"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "FerricStore metrics unavailable on {{ $labels.node_slot }}"

      - alert: FerricStoreNodeNotReady
        expr: probe_success{job="ferricstore-readiness"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "FerricStore readiness failed for {{ $labels.instance }}"

      - alert: FerricStoreQuorumWriteErrors
        expr: sum by (node_slot) (rate(ferricstore_quorum_submit_total{status=~"error|unknown"}[5m])) > 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "FerricStore quorum writes are failing on {{ $labels.node_slot }}"

      - alert: FerricStoreLocalApplyTimeouts
        expr: sum by (node_slot) (increase(ferricstore_batcher_local_apply_timeout_total[5m])) > 0
        labels:
          severity: warning
        annotations:
          summary: "FerricStore local apply timed out on {{ $labels.node_slot }}"

      - alert: FerricStoreReplaySafeLag
        expr: max by (node_slot) (ferricstore_bitcask_replay_safe_lag) > 1000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "FerricStore durable projection is lagging on {{ $labels.node_slot }}"

      - alert: FerricStoreTaskRestarted
        expr: resets(ferricstore_uptime_seconds[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: "FerricStore task restarted on {{ $labels.node_slot }}"
```

Tune the lag and timing thresholds against normal production load. Keep the
`up`, readiness, and quorum alerts per node; aggregating away `node_slot` can
make a two-of-three cluster look healthy while one replica repeatedly fails.

### What Counts As Telemetry

The example separates four signals:

| Signal | Source | Destination |
|---|---|---|
| FerricStore application metrics | Per-node `/metrics` | Prometheus-compatible scraper |
| Readiness and quorum symptoms | Per-node `/health/ready` | Blackbox Exporter and Prometheus |
| Task CPU, memory, network, desired/running count | ECS Container Insights | CloudWatch Metrics |
| Application and ECS startup/replacement logs | `awslogs` driver | `/ecs/<name-prefix>-cluster` CloudWatch log group |

FerricStore does not currently export OTLP distributed traces. “Telemetry” in
this profile therefore means Prometheus metrics, readiness probes, ECS
Container Insights, and CloudWatch logs—not request traces.

A standalone Prometheus server running on ECS also has disposable local
storage. Use `remote_write` to durable monitoring storage or an external
Prometheus-compatible service for production history. AWS documents an ECS
ECS collection path using the AWS Distro for OpenTelemetry collector and
Amazon Managed Service for Prometheus in its
[ECS metrics ingestion guide](https://docs.aws.amazon.com/prometheus/latest/userguide/AMP-onboard-ingest-metrics-OpenTelemetry-ECS.html).

Do not run `terraform destroy` for a live database. It destroys the ECS
services, networking, and cluster control plane. The retained root volumes are
left unattached in EBS and must be manually recovered or snapshotted; they are
not an active service after teardown. Delete them only after confirming that
the data is no longer needed.

## What Is Still Not Provided

- Recovery from simultaneous loss of two or three replica disks.
- Cross-region replication or disaster recovery.
- Autoscaling beyond the fixed three-voter topology.
- Zero-error traffic draining while a live node is still catching up; the NLB
  uses liveness to avoid destructive replacement loops.
- Automatic serialization of arbitrary infrastructure changes outside the
  supplied task-definition/HTTP-mode rollout.
- Durable ACL bootstrap independent of the three instance root volumes. ACL changes are
  replicated, but losing all replicas loses that catalog with the data.
- Native-protocol TLS or end-to-end application TLS. The optional NLB TLS
  listener protects HTTP clients but forwards over the private VPC in plaintext.

These require a durable recovery source, stronger external orchestration, or a
different deployment platform/storage contract. They cannot be honestly
provided by three disposable EC2 root volumes alone.
