variable "location" {
  description = "Azure region"
  type        = string
  default     = "northcentralus"
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
  default     = "ferricstore-bench"
}

variable "vm_size" {
  description = "VM size — L4as_v4 has local NVMe (4 vCPU, 32GB, 600GB NVMe)"
  type        = string
  default     = "Standard_L4as_v4"
}

variable "client_vm_size" {
  description = "Client VM size for benchmark runner"
  type        = string
  default     = "Standard_D2as_v4"
}

variable "node_count" {
  description = "Number of FerricStore nodes (1 for single, 3 for cluster)"
  type        = number
  default     = 1
}

variable "admin_username" {
  description = "SSH admin username"
  type        = string
  default     = "ferric"
}

variable "ssh_public_key_path" {
  description = "Path to SSH public key"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "elixir_version" {
  description = "Elixir version to install"
  type        = string
  default     = "1.19.5"
}

variable "otp_version" {
  description = "Erlang/OTP version to install"
  type        = string
  default     = "28.3.1"
}

variable "shard_count" {
  description = "FerricStore shard count (0 = auto = schedulers_online)"
  type        = number
  default     = 0
}

variable "data_filesystem" {
  description = "Filesystem used for the FerricStore local NVMe data mount"
  type        = string
  default     = "xfs"

  validation {
    condition     = contains(["ext4", "xfs"], var.data_filesystem)
    error_message = "data_filesystem must be either ext4 or xfs."
  }
}

variable "ferricstore_git_branch" {
  description = "FerricStore git branch used by benchmark VMs"
  type        = string
  default     = "main"
}

variable "ferricstore_port" {
  description = "FerricStore TCP port"
  type        = number
  default     = 6379
}
