# FerricStore Azure Benchmark Deployment

## Quick Start

### Single node benchmark

```bash
cd deploy/azure
terraform init
terraform apply -var="node_count=1"
```

`terraform.tfvars` defaults the data mount to XFS:

```hcl
data_filesystem = "xfs"
```

To compare filesystems, recreate the server VM with the same client VM:

```bash
terraform apply -replace='azurerm_linux_virtual_machine.bench[0]' -var='data_filesystem=ext4'
terraform apply -replace='azurerm_linux_virtual_machine.bench[0]' -var='data_filesystem=xfs'
```

### 3-node cluster benchmark

```bash
terraform apply -var="node_count=3"
```

### Destroy when done

```bash
terraform destroy
```

## What gets created

| Resource | Spec | Purpose |
|----------|------|---------|
| N × L4s_v3 VMs | 4 vCPU, 32GB RAM, local NVMe | FerricStore server nodes |
| 1 × D2s_v5 VM | 2 vCPU, 8GB RAM | Benchmark client (memtier + Benchee) |
| VNet + Subnet | 10.0.0.0/16 | Internal network |
| NSG | SSH + 6379 + Erlang ports | Firewall rules |

## What gets configured automatically

**Server VMs (cloud-init):**
- OTP 28 + Elixir 1.19 via asdf
- Rust stable (for NIF)
- FerricStore cloned and compiled
- Local NVMe formatted + mounted at `/data`
  - The setup rejects boot if it cannot find an unmounted local NVMe data disk, so benchmarks do not silently fall back to the OS managed disk
  - Filesystem is controlled by `data_filesystem` (`xfs` or `ext4`)
- OS tuning: huge pages, swappiness=1, THP disabled, NVMe scheduler
- BEAM VM flags: `+sbt db +sbwt very_short +swt very_low +K true +A 128`
- systemd service ready

**Client VM (cloud-init):**
- OTP 28 + Elixir 1.19 via asdf
- memtier_benchmark built from source
- FerricStore cloned (for bench scripts)

## Running Benchmarks

### 1. SSH into nodes

```bash
# Get IPs
terraform output

# SSH to server
ssh ferric@<server_public_ip>

# SSH to client
ssh ferric@<client_public_ip>
```

### 2. Start FerricStore (on server)

```bash
sudo systemctl start ferricstore

# Or manually with custom flags:
cd ~/ferricstore
ERL_FLAGS="+sbt db +sbwt very_short +swt very_low +K true +A 128" \
FERRICSTORE_DATA_DIR=/data/ferricstore \
FERRICSTORE_SHARD_COUNT=8 \
elixir --sname ferricstore --cookie ferricstore_bench -S mix run --no-halt
```

### 3. Run benchmarks (from client)

```bash
# TCP benchmark
cd ~/ferricstore
bash bench/tcp_throughput.sh <server_private_ip> 6379

# erpc benchmark
elixir --sname bench --cookie ferricstore_bench \
  -S mix run bench/erpc_throughput.exs --remote ferricstore@ferricstore-0

# Cluster benchmark (3-node)
elixir --sname bench --cookie ferricstore_bench \
  -S mix run bench/cluster_throughput.exs \
  --node ferricstore@ferricstore-0 \
  --node ferricstore@ferricstore-1 \
  --node ferricstore@ferricstore-2
```

## Cost

| Config | VMs | Cost/hr |
|--------|-----|---------|
| Single node | 1×L4s + 1×D2s | ~$0.46/hr |
| 3-node cluster | 3×L4s + 1×D2s | ~$1.08/hr |

Remember to `terraform destroy` when done!
