# Azure Benchmark Lab

This directory contains Terraform for reproducing FerricStore benchmark runs on Azure. It is benchmark lab infrastructure, not production deployment guidance.

Use the production guides for normal deployments:

- [Deployment](../../guides/deployment.md)
- [Configuration](../../guides/configuration.md)
- [Security](../../guides/security.md)
- [Benchmarks](../../docs/benchmarks.md)

## What This Creates

| Resource | Purpose |
| --- | --- |
| FerricStore server VM(s) | Run the FerricStore server under benchmark load. |
| Client VM | Run native benchmark clients and SDK workload scripts. |
| VNet/subnet/NSG | Private benchmark network plus SSH access. |
| Local NVMe mount | Benchmark data directory mounted at `/data`. |

The cloud-init setup is intentionally strict: if it cannot find an unmounted local NVMe data disk, setup fails instead of silently benchmarking on the OS managed disk.

## Public Repo Hygiene

Do not commit generated or local Terraform files:

```text
terraform.tfstate
terraform.tfstate.backup
terraform.tfvars
*.tfplan
.terraform/
```

Use `terraform.tfvars.example` as the template for local variables.

## Quick Start

```bash
cd deploy/azure
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars with your SSH key, region, VM sizes, and node count
terraform init
terraform apply
```

Destroy resources when finished:

```bash
terraform destroy
```

## Common Shapes

Single server plus one client:

```hcl
node_count = 1
server_vm_size = "Standard_L4as_v4"
client_vm_size = "Standard_D2as_v4"
data_filesystem = "ext4"
```

Larger Flow benchmark server plus one small client:

```hcl
node_count = 1
server_vm_size = "REPLACE_WITH_16_VCPU_LOCAL_NVME_SKU"
client_vm_size = "Standard_D2as_v4"
data_filesystem = "ext4"
```

Three server nodes for cluster experiments:

```hcl
node_count = 3
```

## Filesystem Sweep

To compare filesystems, replace the server VM while keeping the rest of the lab:

```bash
terraform apply -replace='azurerm_linux_virtual_machine.bench[0]' -var='data_filesystem=ext4'
terraform apply -replace='azurerm_linux_virtual_machine.bench[0]' -var='data_filesystem=xfs'
```

## Starting FerricStore On The Server

The server cloud-init installs dependencies, clones the repo, prepares `/data`, and installs a systemd service.

```bash
sudo systemctl start ferricstore
sudo systemctl status ferricstore
```

Manual start example:

```bash
cd ~/ferricstore
ERL_FLAGS="+sbt db +sbwt very_short +swt very_low +K true +A 128" \
FERRICSTORE_DATA_DIR=/data/ferricstore \
FERRICSTORE_SHARD_COUNT=0 \
elixir --sname ferricstore --cookie ferricstore_bench -S mix run --no-halt
```

`FERRICSTORE_SHARD_COUNT=0` lets FerricStore choose the default based on the VM. Override it only when reproducing a specific shard sweep.

## Running Benchmarks From The Client

Get private/public IPs:

```bash
terraform output
```

SSH to the client VM and run benchmark scripts from `~/ferricstore` or the Python SDK repository, depending on the workload being reproduced.

KV SET/GET benchmark shape should use a native-protocol SDK/client. Record the
connection count, lanes per connection, in-flight requests per lane, value size,
key range, throughput, and p50/p95/p99/p99.9 latency.

FerricFlow benchmark shapes are documented in [Benchmarks](../../docs/benchmarks.md). Use the Python SDK benchmark scripts for queue/workflow runs.

## Notes

- This lab is optimized for reproducibility, not minimal cloud cost.
- Always confirm the data directory is on local NVMe before using numbers publicly.
- Always destroy the lab after benchmark runs.
