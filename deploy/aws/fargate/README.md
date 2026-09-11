# AWS Fargate Single-Task Deployment

> This is the supported ephemeral Fargate profile for FerricStore OSS. Its
> precise scope and failure behavior are documented in
> [AWS Fargate Single-Task Support](../../../docs/aws-fargate-single-task.md).

This Terraform stack runs FerricStore OSS in exactly one Amazon ECS Fargate
task. It is intended for disposable development, demos, SDK integration, CI,
caches, and similar workloads. It creates:

- a VPC with public and private subnets in two or three availability zones;
- one NAT gateway so private tasks can pull the published Quay.io image;
- an internal Network Load Balancer with the native endpoint and an optional
  HTTP/HTTPS endpoint;
- one ECS service with a fixed desired count of one and no autoscaling;
- encrypted Fargate ephemeral storage mounted at `/data`;
- isolated readiness checks on port `6381`, CloudWatch logs, and ECS Exec.

It does not create S3, DynamoDB, EFS, EBS, service discovery, or FerricStore
cluster resources.

## Data Is Ephemeral

All FerricStore data and ACL changes in this layout are lost whenever the
Fargate task stops or is replaced. Do not use this stack for irreplaceable or
durable data.

EFS is deliberately not used. FerricStore's Flow query projection uses LMDB,
whose own documentation forbids remote filesystems such as NFS; EFS is an NFS
filesystem. ECS-managed EBS is also not a persistence solution for this service
because Fargate tasks can attach only new volumes, not reattach an existing data
volume after task replacement.

The ECS service serializes replacement (`minimum_healthy_percent = 0`,
`maximum_percent = 100`) so two independent FerricStore datasets are never
routed at the same time. A replacement still starts empty and causes a short
outage.

## Quick Start

You need Terraform 1.5 or newer and AWS credentials with permission to create
the resources in this stack.

```bash
cd deploy/aws/fargate
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

Get the private endpoint:

```bash
terraform output -raw endpoint
```

The default NLB is internal, so run a FerricStore SDK client from the VPC, a
peered VPC, or a network connected through VPN/Direct Connect.

## HTTP Command API

FerricStore `0.11.11` and later include the HTTP command API. It remains
disabled unless you opt in:

```hcl
container_image    = "quay.io/ferricstore/ferricstore:0.11.14"
http_enabled       = true
http_listener_port = 8080
```

After `terraform apply`, get the stable private endpoints:

```bash
terraform output -raw http_endpoint
terraform output -raw http_readiness_endpoint
```

Enabling or disabling HTTP changes the task definition and replaces the one
running task. The replacement starts with empty `/data`, so any existing data
and ACL identities are lost; bootstrap the HTTP ACL identity after the new task
is ready.

The stack opens task port `8080`, starts the in-process HTTP listener on every
replacement task, registers it in a separate NLB target group, and checks the
isolated node readiness endpoint before sending traffic. `POST /v1/commands`
always requires HTTP Basic credentials backed by a FerricStore ACL identity.

### Create An HTTP User

The HTTP API cannot create its own first user because `/v1/commands` already
requires authentication. After `terraform apply`, run an official FerricStore
SDK or another trusted native administration client from the VPC-connected
network and connect it to:

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

This creates an enabled user that can only run `GET`, `SET`, and `DEL` against
keys beginning with `web-api:`. Add only the commands and key patterns that the
application actually needs. Then use `web-api` and the same password as HTTP
Basic credentials; the [HTTP API guide](../../../guides/http-api.md#send-a-command-batch)
contains a complete request example, and the [security guide](../../../guides/security.md#access-control-lists-acl)
documents all user, command, key, and password rules. Published native SDKs are
listed under [Interfaces And Published SDKs](../../../README.md#interfaces-and-published-sdks).

Do not put the plaintext password in Terraform variables, task-definition
environment variables, or source control. A deployment pipeline can read it
from a secret manager and submit the command after readiness succeeds. In this
single-task profile the ACL catalog is task-local, so repeat that bootstrap
after every replacement; creating the user does not make the native endpoint
require authentication.

Plain HTTP is appropriate only inside a trusted private network. To terminate
TLS 1.2/1.3 at the NLB, use an ACM or IAM certificate and a DNS name covered by
that certificate:

```hcl
http_enabled             = true
http_listener_port       = 443
http_hostname            = "ferricstore.internal.example.com"
http_tls_certificate_arn = "arn:aws:acm:us-east-1:123456789012:certificate/REPLACE_ME"
```

Create the private DNS alias from `http_hostname` to the NLB outside this
stack. TLS is terminated at the NLB; the hop to FerricStore remains plaintext
inside the private VPC. Use application-native TLS instead if policy requires
encryption all the way to the task. The stack does not store certificate keys
in the task definition or Terraform state.

## Security

The stack always creates an internal NLB, disables protected mode for the native
listener, and permits client traffic from the stack VPC by default so a new
disposable task is immediately usable. HTTP command requests still require ACL
credentials. Restrict `allowed_client_cidr_blocks` to the application subnets
that need native or HTTP access. Do not route a plaintext endpoint from an
untrusted network or send sensitive data through it.

ACL changes would disappear with the rest of `/data` after task replacement.
Use a durable deployment layout when persisted credentials or durable data are
required. NLB TLS protects HTTP clients in transit but does not make the
task-local ACL catalog durable.

## Storage And Performance

`ephemeral_storage_gib` controls task-local storage from 21 through 200 GiB.
Fargate encrypts task ephemeral storage, but discards it with the task. Fargate
also cannot install FerricStore's custom `io_uring` seccomp profile, so the
storage engine may use its portable fallback.

For durable workloads use the native release or Kubernetes with a supported
local/block filesystem. Those layouts also provide the direct local-NVMe path
recommended for latency-sensitive and write-heavy deployments.

Do not change `desired_count` or the deployment percentages to run overlapping
tasks. Multiple service tasks would be independent databases, not a
FerricStore cluster.

## Operations

The native and HTTP target groups check `GET /health/ready` on the isolated
probe port. The HTTP listener also exposes unauthenticated `GET /health`,
`GET /ready`, and `GET /metrics`; keep those endpoints inside the same approved
network boundary. Application logs are written to `/ecs/<name_prefix>` in
CloudWatch Logs. ECS Container Insights and ECS Exec are enabled.

To open a shell, first get the running task id:

```bash
TASK_ID="$(aws ecs list-tasks \
  --cluster ferricstore \
  --service-name ferricstore \
  --query 'taskArns[0]' \
  --output text)"

aws ecs execute-command \
  --cluster ferricstore \
  --task "$TASK_ID" \
  --container ferricstore \
  --interactive \
  --command /bin/bash
```

## Cost And Cleanup

This stack creates billable Fargate, NLB, NAT Gateway, and CloudWatch resources.
Review the plan and current AWS pricing before applying it. Destroy the stack
when it is no longer needed:

```bash
terraform destroy
```
