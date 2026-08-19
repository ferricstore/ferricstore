# AWS Fargate Three-Node Cluster

> This is the FerricStore OSS clustered Fargate profile. Read the exact
> [support and failure contract](../../../docs/aws-fargate-cluster.md) before
> using it for data you cannot recreate.

This Terraform stack is one managed deployment containing three FerricStore
nodes. Each node is one Fargate task kept alive by its own desired-count-one
ECS service. An internal Network Load Balancer presents one client endpoint.

The separate services deliberately emulate the stable slots of a Kubernetes
StatefulSet:

| Slot | ECS service | Stable BEAM identity | Failure domain |
|---|---|---|---|
| `node-0` | `<prefix>-node-0` | `ferricstore@node-0.<namespace>` | AZ 0 |
| `node-1` | `<prefix>-node-1` | `ferricstore@node-1.<namespace>` | AZ 1 |
| `node-2` | `<prefix>-node-2` | `ferricstore@node-2.<namespace>` | AZ 2 |

ECS replaces a stopped task and assigns a new private IP. Cloud Map changes
the slot's A record to that IP. FerricStore retries the stable peer names every
five seconds, reconnects through EPMD on `4369` and a fixed distribution port
on `9100`, then Raft transfers the missing snapshot/log from the two surviving
nodes to the replacement's empty `/data` volume.

## Guarantees And Limits

- One failed task or one failed AZ leaves two of three voters and can continue
  serving. When that slot returns, its empty disk is rebuilt from the survivors.
- ECS maintains each slot's desired count and restarts failed tasks.
- Image upgrades are supported only through `scripts/deploy-sequential.sh`,
  which replaces one slot and verifies full local catch-up before continuing.
- Two simultaneous task-disk losses are not supported. They can remove quorum
  and are a data-loss risk. Loss of all three task disks loses all data.
- There is no S3, DynamoDB, EFS, or EBS data dependency. The only external
  secret is the shared Erlang cookie in AWS Secrets Manager.
- This profile has exactly three nodes. Do not attach autoscaling or change a
  service's desired count.

## Quick Start

You need Terraform 1.5+, AWS CLI v2, the Session Manager plugin for guarded
rollouts, and AWS credentials allowed to create the resources.

Create a strong cluster cookie. Its value is not placed in Terraform state:

```bash
COOKIE_ARN="$(aws secretsmanager create-secret \
  --name ferricstore-cluster-cookie \
  --secret-string "$(openssl rand -hex 32)" \
  --query ARN \
  --output text)"
```

Configure and create the cluster:

```bash
cd deploy/aws/fargate-cluster
cp terraform.tfvars.example terraform.tfvars
# Put COOKIE_ARN into cluster_cookie_secret_arn.
# Build this repository revision (or use a later compatible release), push it,
# and pin its digest in container_image. The older 0.11.5 image does not contain
# the Fargate cluster discovery/recovery changes.
terraform init
terraform plan
terraform apply
terraform output -raw endpoint
```

The endpoint and the three Cloud Map names are private to the VPC. Clients must
run in the VPC, a peered VPC, or a network connected through VPN/Direct Connect.

## Safe Upgrade

Do not let Terraform or ECS roll all three services together.

1. Change `container_image` to an exact tag or digest.
2. Run `terraform plan` and `terraform apply`. Terraform registers three new
   task-definition revisions but intentionally ignores the services'
   `task_definition` field. Previous revisions remain active so ECS can still
   replace a failed old task before the guarded rollout reaches that slot.
3. Run the guarded rollout:

```bash
./scripts/deploy-sequential.sh
```

The command updates `node-0`, waits for ECS stability, checks
`Ferricstore.Cluster.Recovery.ready?()` inside the replacement, and only then
does the same for `node-1` and `node-2`. Recovery requires the full three-node
mesh and membership plus a bounded local apply lag of at most ten entries on
every shard. It stops if discovery, quorum, snapshot transfer, or local apply
has not converged within 30 minutes. Set
`RECOVERY_TIMEOUT_SECONDS` for an intentionally larger data set.

Changing an ECS service, its network, target group, or service registry can
also trigger a deployment. Apply such changes to one slot at a time or use a
maintenance procedure; the image rollout script cannot serialize arbitrary
Terraform service replacements.

## Health And Traffic

ECS container checks and NLB target checks use `/health/live`. This is
intentional: unlike Kubernetes readiness, an unhealthy ECS load-balancer target
can cause task replacement. Using `/health/ready` would turn a temporary quorum
outage into repeated destruction of task-local replica disks.

During replacement, the NLB can briefly send a connection to a live node that
is still recovering. Use FerricStore's topology-aware SDK behavior and retry
transient connection/reroute errors. `/health/ready` remains available for
ordinary operator checks; the rollout uses the stricter
mesh/membership/replica-lag recovery check.

## Prometheus And Telemetry

FerricStore exposes Prometheus-compatible application metrics at `/metrics` on
port `6380`. Scrape the three stable Cloud Map names individually rather than
the NLB so one missing or lagging node cannot be hidden by the other two:

```yaml
scrape_configs:
  - job_name: ferricstore
    metrics_path: /metrics
    static_configs:
      - targets: ["node-0.ferricstore.local:6380"]
        labels: {node_slot: "node-0"}
      - targets: ["node-1.ferricstore.local:6380"]
        labels: {node_slot: "node-1"}
      - targets: ["node-2.ferricstore.local:6380"]
        labels: {node_slot: "node-2"}
```

The scraper must run in, or have DNS/network access to, the stack VPC. Port
`6380` is not open by default. Permit it only from the scraper's security group.
The complete [Prometheus, readiness-probe, alert-rule, and Fargate telemetry
example](../../../docs/aws-fargate-cluster.md#prometheus-and-fargate-telemetry)
also explains CloudWatch logs, Container Insights, authentication, and durable
metrics storage.

## Cost And Cleanup

The stack creates three Fargate tasks, an NLB, three NAT gateways, Cloud Map,
CloudWatch Logs, and related VPC resources. Three NAT gateways preserve image
pull and control-plane egress independently in each AZ but are often a material
part of the cost. A private-ECR design can replace them with the appropriate VPC
endpoints.

Destroying the stack destroys every replica and all FerricStore data:

```bash
terraform destroy
```

Task definitions use `skip_destroy` for rollout safety, so old inactive/unused
revisions remain registered after replacement or stack destruction. Deregister
only revisions that no ECS service currently references.
