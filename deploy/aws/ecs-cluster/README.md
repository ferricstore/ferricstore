# AWS ECS-on-EC2 Three-Node Cluster

> This is the FerricStore OSS clustered ECS-on-EC2 profile. Read the exact
> [support and failure contract](../../../docs/aws-ecs-cluster.md) before
> using it for data you cannot recreate.

This Terraform stack is one managed deployment containing three FerricStore
nodes. Each node is one ECS task on a dedicated per-AZ EC2 capacity provider,
kept alive by its own desired-count-one ECS service. An internal Network Load
Balancer presents stable native and optional HTTP/HTTPS endpoints.

The separate services deliberately emulate the stable slots of a Kubernetes
StatefulSet:

| Slot | ECS service | Stable BEAM identity | Failure domain |
|---|---|---|---|
| `node-0` | `<prefix>-node-0` | `ferricstore@node-0.<namespace>` | AZ 0 |
| `node-1` | `<prefix>-node-1` | `ferricstore@node-1.<namespace>` | AZ 1 |
| `node-2` | `<prefix>-node-2` | `ferricstore@node-2.<namespace>` | AZ 2 |

ECS replaces a stopped task or container instance and assigns a new private IP.
Cloud Map changes the slot's A record to that IP. FerricStore retries the stable
peer names every five seconds, reconnects through EPMD on `4369` and a fixed
distribution port on `9100`, then Raft transfers the missing snapshot/log from
the two surviving nodes to the replacement's empty `/data` volume.

## Guarantees And Limits

- One failed task or one failed AZ leaves two of three voters and can continue
  serving. When that slot returns, its empty disk is rebuilt from the survivors.
- ECS maintains each slot's desired count and restarts failed tasks.
- Image upgrades are supported only through `scripts/deploy-sequential.sh`,
  which replaces one slot and verifies full local catch-up before continuing.
- Two simultaneous instance/root-volume losses are not supported. They can
  remove quorum and are a data-loss risk. Irrecoverably deleting all three
  retained root volumes loses all data.
- There is no S3, DynamoDB, or EFS data dependency. Each node uses an encrypted
  gp3 root volume on its EC2 instance. The volume is retained when an instance
  is terminated, while a replacement starts with a fresh root and rebuilds its
  active replica from the other two nodes. The retained old volume is available
  for manual recovery and is tagged `Retained=true`.
- This profile has exactly three nodes. Do not change a service's desired count.
  The per-AZ capacity providers already use Auto Scaling for failed-instance
  replacement.

## Quick Start

You need Terraform 1.5+, AWS CLI v2, `jq`, the Session Manager plugin for
guarded rollouts, and AWS credentials allowed to create the resources. The
deployment also creates three ECS-optimized EC2 container instances; choose an
instance type with enough CPU, memory, local storage, and ENI capacity for one
FerricStore node per AZ. The AMI parameter is selected automatically from
`cpu_architecture` unless `ec2_optimized_ami_parameter` is set explicitly.

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
cd deploy/aws/ecs-cluster
cp terraform.tfvars.example terraform.tfvars
# Put COOKIE_ARN into cluster_cookie_secret_arn.
# Build this repository revision (or use a later compatible release), push it,
# and pin its digest in container_image. HTTP requires FerricStore 0.11.11 or
# later; older images do not contain the in-process HTTP application.
terraform init
terraform plan
terraform apply
terraform output -raw endpoint
```

The endpoint and the three Cloud Map names are private to the VPC. Clients must
run in the VPC, a peered VPC, or a network connected through VPN/Direct Connect.

### Protect An Existing Cluster's Volumes

The launch template applies `delete_on_termination = false` to new instances.
If this stack was already running before that setting was applied, update the
termination flag on the three current instances once, before any planned
replacement:

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
detach the volume, or modify FerricStore data. Verify the flag with
`aws ec2 describe-instances` before continuing a rollout.

## HTTP Command API

For a new cluster, opt in before the first apply:

```hcl
http_enabled       = true
http_listener_port = 8080
```

The same task runs the native and HTTP listeners; this does not create another
FerricStore container or another cluster. Terraform adds one HTTP target group
per stable node slot and a second NLB listener, then ECS registers each
replacement task IP in both its native and HTTP target groups. Clients keep one
stable endpoint while task and instance IPs change:

```bash
terraform output -raw http_endpoint
terraform output -raw http_readiness_endpoint
```

`POST /v1/commands` always requires HTTP Basic credentials backed by the
replicated FerricStore ACL catalog. The listener's `GET /health`, `GET /ready`,
and `GET /metrics` routes do not require credentials, so the NLB and task
security rules must remain private and scoped with
`allowed_client_cidr_blocks`.

### Create An HTTP User

The HTTP API cannot create its own first user because `/v1/commands` already
requires authentication. After the three nodes report ready, run an official
FerricStore SDK or another trusted native administration client from the
VPC-connected network and connect it to:

```bash
terraform output -raw endpoint
```

Submit this as one native command. The array below shows the command arguments;
it is not a shell command, and the leading `>` on the password is an ACL rule:

```text
["ACL", "SETUSER", "web-api", "on", "resetpass",
 ">replace-with-a-long-random-password", "resetkeys",
 "+GET", "+SET", "+DEL", "~web-api:*"]
```

The successful command is committed through the replicated ACL catalog, so all
three nodes authenticate the same HTTP user regardless of which node the NLB
selects. Run it once, wait for success, and then start application traffic. Add
only the commands and key patterns the application needs. The
[HTTP API guide](../../../guides/http-api.md#send-a-command-batch) contains a
complete authenticated request, the [security guide](../../../guides/security.md#access-control-lists-acl)
documents all ACL rules, and published clients are listed under
[Interfaces And Published SDKs](../../../README.md#interfaces-and-published-sdks).

Do not put the plaintext password in Terraform variables, task-definition
environment variables, or source control. A deployment pipeline can read it
from a secret manager and submit the command after cluster readiness succeeds.
One task or container-instance replacement restores the user from the surviving
replicas. Loss of all instance-local replicas loses the ACL catalog along with
the application data.
Creating the user does not make the native endpoint require authentication.

Plain HTTP is only for a trusted private network. For TLS 1.2/1.3 termination at
the NLB, use an ACM or IAM certificate whose SAN covers a private DNS alias:

```hcl
http_enabled             = true
http_listener_port       = 443
http_hostname            = "ferricstore.internal.example.com"
http_tls_certificate_arn = "arn:aws:acm:us-east-1:123456789012:certificate/REPLACE_ME"
```

Create the DNS alias to the internal NLB outside this stack. The private hop
from the NLB to task port `8080` is plaintext; use application-native TLS if
your policy requires end-to-end encryption. Certificate private keys are not
placed in the task definition or Terraform state.

### Enabling Or Disabling HTTP On A Running Cluster

Changing an ECS service's target groups starts a deployment. Do not flip
`http_enabled` with an unrestricted apply on an existing three-node cluster.
Use this guarded sequence after editing `terraform.tfvars`.

For `false` to `true`, first create the compatible task definitions and empty
HTTP load-balancer resources without changing the services:

```bash
terraform apply \
  -target='aws_ecs_task_definition.node' \
  -target='aws_lb_target_group.http' \
  -target='aws_lb_listener.http' \
  -target='aws_lb_listener.https'
```

For `true` to `false`, first register only the task definitions with the HTTP
listener disabled:

```bash
terraform apply -target='aws_ecs_task_definition.node'
```

Then, for either direction, let the guarded rollout update the task definition
and exact target-group list one slot at a time. It waits for full cluster
recovery before touching the next slot. Finish with a normal apply to reconcile
or remove the remaining load-balancer resources:

```bash
./scripts/deploy-sequential.sh
terraform apply
```

Terraform resource targeting is intentionally limited here to staging a
stateful migration; review both plans before approving them.

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

The command updates `node-0` with the latest task definition and desired native
and HTTP target-group list, waits for ECS stability, checks
`Ferricstore.Cluster.Recovery.ready?()` inside the replacement, and only then
does the same for `node-1` and `node-2`. Recovery requires the full three-node
mesh and membership plus a bounded local apply lag of at most ten entries on
every shard. It stops if discovery, quorum, snapshot transfer, or local apply
has not converged within 30 minutes. Set
`RECOVERY_TIMEOUT_SECONDS` for an intentionally larger data set.

Changing an ECS service, its network, target group, or service registry can
also trigger a deployment. The HTTP migration above stages its target groups
and lets the rollout script attach them one slot at a time. Use a separate
one-slot-at-a-time maintenance procedure for other service changes; the script
cannot safely infer arbitrary Terraform replacements.

## Health And Traffic

ECS container checks and both NLB target groups use the isolated
`/health/live` endpoint on port `6381`. This is intentional: unlike Kubernetes
readiness, an unhealthy ECS load-balancer target can cause task replacement.
Using either node readiness or the API listener's overload-sensitive health
route would turn a temporary problem into destruction of instance-local replica
disks.

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

The optional HTTP endpoint also has `/metrics`, but that route is load-balanced
and focuses on HTTP listener activity. It is not a replacement for per-node
cluster scraping.

The scraper must run in, or have DNS/network access to, the stack VPC. Port
`6380` is not open by default. Permit it only from the scraper's security group.
The complete [Prometheus, readiness-probe, alert-rule, and ECS telemetry
example](../../../docs/aws-ecs-cluster.md#prometheus-and-ecs-telemetry)
also explains CloudWatch logs, Container Insights, authentication, and durable
metrics storage.

## Cost And Cleanup

The stack creates three ECS tasks, three EC2 container instances, an NLB, three
NAT gateways, Cloud Map, CloudWatch Logs, and related VPC resources. Three NAT gateways preserve image
pull and control-plane egress independently in each AZ but are often a material
part of the cost. A private-ECR design can replace them with the appropriate VPC
endpoints.

Do not run `terraform destroy` for a live database. It destroys the ECS
services, networking, and cluster control plane. The EC2 root volumes are
intentionally configured with `delete_on_termination = false`, so they remain
as unattached, tagged EBS volumes instead of being physically deleted; the
database is not automatically served after teardown and must be manually
recovered or snapshotted. Delete those retained volumes only after confirming
that the data is no longer needed.

```bash
terraform destroy
```

Task definitions use `skip_destroy` for rollout safety, so old inactive/unused
revisions remain registered after replacement or stack destruction. Deregister
only revisions that no ECS service currently references.
