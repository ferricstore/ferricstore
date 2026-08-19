# AWS Fargate Single-Task Deployment

> This is the supported ephemeral Fargate profile for FerricStore OSS. Its
> precise scope and failure behavior are documented in
> [AWS Fargate Single-Task Support](../../../docs/aws-fargate-single-task.md).

This Terraform stack runs FerricStore OSS in exactly one Amazon ECS Fargate
task. It is intended for disposable development, demos, SDK integration, CI,
caches, and similar workloads. It creates:

- a VPC with public and private subnets in two or three availability zones;
- one NAT gateway so private tasks can pull the published GHCR image;
- an internal Network Load Balancer by default;
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

## Security

The stack always creates an internal NLB, disables protected mode, and permits
native-protocol traffic from the stack VPC by default so a new disposable task
is immediately usable. Restrict
`allowed_client_cidr_blocks` to the application subnets that need access. Do not
route the endpoint from an untrusted network or send sensitive data to this
service.

ACL changes would disappear with the rest of `/data` after task replacement.
Use a durable deployment layout when authentication, persisted credentials, or
native TLS are required.

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

The NLB checks `GET /health/ready` on the isolated probe port. Application logs
are written to `/ecs/<name_prefix>` in CloudWatch Logs. ECS Container Insights
and ECS Exec are enabled.

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
