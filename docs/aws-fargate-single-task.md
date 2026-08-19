# AWS Fargate Single-Task Support

Status: **supported OSS deployment profile with ephemeral data**

Implementation: [`deploy/aws/fargate`](https://github.com/ferricstore/ferricstore/tree/main/deploy/aws/fargate)

## Contract

This profile runs FerricStore OSS as exactly one Amazon ECS service with a
desired count of one and exactly one active Fargate task. It deliberately uses
only task-local storage:

- no DynamoDB;
- no S3 backup or restore;
- no EFS;
- no ECS-managed EBS volume;
- no FerricStore cluster or service autoscaling.

The profile is intended for development, demos, SDK integration, CI, caches,
and other workloads whose data may be discarded. It is not a durable database
profile: stopping or replacing the task destroys all FerricStore data and ACL
changes.

## Architecture

```mermaid
flowchart LR
  C["VPC-connected OSS client"] --> NLB["Internal Network Load Balancer"]
  NLB --> T["ECS service<br/>desired count = 1"]
  T --> F["One Fargate task"]
  F --> I["Short-lived data-init container"]
  F --> A["FerricStore OSS container<br/>UID 10001"]
  A --> D["Encrypted task-local /data<br/>21-200 GiB"]
  A --> L["CloudWatch Logs"]
```

The init and FerricStore containers are part of the same Fargate task. The init
container only sets ownership on `/data`, exits successfully, and then the
FerricStore container starts.

## Deployment Behavior

The ECS service hard-codes `desired_count = 1`. Its deployment limits are:

```text
minimum healthy percent = 0
maximum percent = 100
```

ECS therefore stops the old task before starting its replacement. This creates
an outage, but it prevents two unrelated empty FerricStore datasets from being
available behind the load balancer at the same time.

The task runs in private subnets without a public IP. An internal Network Load
Balancer provides a stable native-protocol endpoint. The default client access
is limited to the stack VPC and can be narrowed with
`allowed_client_cidr_blocks`.

## Storage Behavior

`/data` is an encrypted Fargate ephemeral volume. The configured allocation is
shared with container image layers and can be set from 21 through 200 GiB.

Data survives ordinary process restarts inside the same running task. Data does
not survive:

- an ECS deployment;
- a Fargate task replacement or retirement;
- scaling the service down;
- destroying the Terraform stack;
- an Availability Zone or host event that replaces the task.

EFS is not used because FerricStore's Flow projection uses LMDB, whose upstream
documentation warns against remote filesystems. An ECS-managed EBS volume is
not used because a service replacement gets a new volume and the service-owned
volume is deleted with the task.

## Health And Lifecycle

The task and NLB check the isolated `GET /health/ready` endpoint on port 6381.
Client traffic is sent to native protocol port 6388 only after local shards and
Raft leaders are ready.

FerricStore receives up to 120 seconds after SIGTERM. The server suspends its
listeners, sends native GOAWAY notifications, waits for active clients, and
flushes local storage before exit. Because storage is task-local, this graceful
shutdown improves consistency during the task lifetime but does not make a
replacement durable.

## Security Boundary

The NLB is always internal, the task has no public IP, and the dashboard is not
exposed by the load balancer. Protected mode is disabled so a fresh ephemeral
task can accept native connections without a persistent bootstrap credential.
This is acceptable only inside an explicitly trusted network boundary.

For the initial OSS profile:

- restrict `allowed_client_cidr_blocks` to application networks;
- do not expose the NLB through public routing;
- do not store sensitive or irreplaceable data;
- use an exact image release or digest;
- treat ECS Exec as privileged break-glass access;
- destroy the stack when the environment is no longer needed.

Native TLS and durable ACL bootstrap are outside this disposable profile. A
deployment that requires either should use a durable FerricStore layout.

## AWS Resources

The Terraform stack creates:

- one VPC with public and private subnets in two or three Availability Zones;
- one internet gateway and one NAT gateway for image and AWS API egress;
- one internal NLB, listener, and IP target group;
- one ECS cluster, task definition, and fixed single-task service;
- one task-local data volume and short-lived initialization container;
- execution and task IAM roles;
- one CloudWatch log group and Container Insights configuration.

It does not create any remote data store, backup service, discovery service, or
cluster coordination service.

## Operational Expectations

| Event | Result |
| --- | --- |
| FerricStore process restart inside the same task | Local data remains |
| ECS replaces the task | Brief outage; replacement starts empty |
| Task health check fails repeatedly | ECS replaces it; data is lost |
| Deployment changes the task definition | Old task stops; new empty task starts |
| NLB is unavailable | Data remains in the running task but clients cannot connect |
| NAT gateway is unavailable after startup | Running data path continues; image pulls and some management operations may fail |
| Terraform destroy | All stack resources and data are removed |

## Validation Requirements

Repository changes to this profile must pass:

- `terraform fmt -check -recursive`;
- `terraform validate`;
- an image start and `/health/ready` smoke test;
- confirmation that the task definition has Fargate compatibility, `awsvpc`
  networking, one `/data` mount, and a non-root FerricStore process;
- confirmation that the ECS service remains fixed at desired count one with no
  overlapping deployment capacity.

## Primary References

- [AWS Fargate task ephemeral storage](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-storage.html)
- [Amazon EBS volumes with ECS](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ebs-volumes.html)
- [Fargate task definition parameters](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html)
- [ECS service replacement behavior](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/update-service-parameters.html)
- [OpenLDAP LMDB filesystem caveat](https://github.com/openldap/openldap/blob/master/libraries/liblmdb/lmdb.h)
