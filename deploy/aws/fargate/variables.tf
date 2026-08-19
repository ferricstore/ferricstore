variable "aws_region" {
  description = "AWS region in which to create the Fargate service."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Short name used to prefix AWS resources."
  type        = string
  default     = "ferricstore"

  validation {
    condition     = can(regex("^[a-zA-Z0-9]([a-zA-Z0-9-]{0,18}[a-zA-Z0-9])?$", var.name_prefix))
    error_message = "name_prefix must contain 1-20 letters, numbers, or internal hyphens."
  }
}

variable "vpc_cidr" {
  description = "CIDR for the VPC created by this stack."
  type        = string
  default     = "10.42.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.vpc_cidr))
    error_message = "vpc_cidr must be a valid IPv4 CIDR."
  }
}

variable "availability_zone_count" {
  description = "Number of availability zones used for public and private networking."
  type        = number
  default     = 2

  validation {
    condition     = contains([2, 3], var.availability_zone_count)
    error_message = "availability_zone_count must be 2 or 3."
  }
}

variable "container_image" {
  description = "Exact FerricStore OSS image to run. Pin a release or digest for repeatable deployments."
  type        = string
  default     = "quay.io/ferricstore/ferricstore:0.11.6"

  validation {
    condition     = trimspace(var.container_image) != ""
    error_message = "container_image must not be empty."
  }
}

variable "cpu" {
  description = "Fargate task CPU units. The default is 2 vCPU."
  type        = number
  default     = 2048

  validation {
    condition     = contains([256, 512, 1024, 2048, 4096, 8192, 16384, 32768], var.cpu)
    error_message = "cpu must be a Fargate-supported CPU size."
  }
}

variable "memory" {
  description = "Fargate task memory in MiB. It must be valid for the selected CPU size."
  type        = number
  default     = 4096

  validation {
    condition     = var.memory >= 512 && floor(var.memory) == var.memory
    error_message = "memory must be an integer number of MiB greater than or equal to 512."
  }
}

variable "ephemeral_storage_gib" {
  description = "Task-local ephemeral data storage in GiB. All contents are lost when the task stops."
  type        = number
  default     = 40

  validation {
    condition     = var.ephemeral_storage_gib >= 21 && var.ephemeral_storage_gib <= 200 && floor(var.ephemeral_storage_gib) == var.ephemeral_storage_gib
    error_message = "ephemeral_storage_gib must be an integer between 21 and 200 GiB."
  }
}

variable "cpu_architecture" {
  description = "Fargate task CPU architecture. The published image supports both values."
  type        = string
  default     = "X86_64"

  validation {
    condition     = contains(["X86_64", "ARM64"], var.cpu_architecture)
    error_message = "cpu_architecture must be X86_64 or ARM64."
  }
}

variable "shard_count" {
  description = "FerricStore shard count. Zero lets FerricStore match the available schedulers."
  type        = number
  default     = 0

  validation {
    condition     = var.shard_count >= 0 && floor(var.shard_count) == var.shard_count
    error_message = "shard_count must be a non-negative integer."
  }
}

variable "allowed_client_cidr_blocks" {
  description = "CIDRs allowed to reach the native port. An empty list uses the stack VPC CIDR."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for cidr in var.allowed_client_cidr_blocks : can(cidrnetmask(cidr))])
    error_message = "Every allowed client entry must be a valid IPv4 CIDR."
  }
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention in days."
  type        = number
  default     = 30

  validation {
    condition = contains(
      [1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653],
      var.log_retention_days
    )
    error_message = "log_retention_days must be a CloudWatch Logs supported retention value."
  }
}

variable "tags" {
  description = "Additional tags applied to resources."
  type        = map(string)
  default     = {}
}
