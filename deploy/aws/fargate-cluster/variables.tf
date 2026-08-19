variable "aws_region" {
  description = "AWS region in which to create the three-node cluster."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Short name used to prefix AWS resources."
  type        = string
  default     = "ferricstore"

  validation {
    condition     = can(regex("^[a-zA-Z0-9]([a-zA-Z0-9-]{0,14}[a-zA-Z0-9])?$", var.name_prefix))
    error_message = "name_prefix must contain 1-16 letters, numbers, or internal hyphens."
  }
}

variable "vpc_cidr" {
  description = "CIDR for the VPC created by this stack."
  type        = string
  default     = "10.43.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.vpc_cidr))
    error_message = "vpc_cidr must be a valid IPv4 CIDR."
  }
}

variable "service_discovery_namespace" {
  description = "Private Cloud Map DNS namespace used for stable FerricStore node identities."
  type        = string
  default     = "ferricstore.local"

  validation {
    condition     = can(regex("^[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?$", var.service_discovery_namespace))
    error_message = "service_discovery_namespace must be a valid private DNS name."
  }
}

variable "container_image" {
  description = "Exact cluster-capable FerricStore OSS image built from this revision or a later release. Pin a digest."
  type        = string

  validation {
    condition     = trimspace(var.container_image) != ""
    error_message = "container_image must not be empty."
  }
}

variable "cluster_cookie_secret_arn" {
  description = "ARN of an existing Secrets Manager secret containing the shared Erlang cluster cookie."
  type        = string
  sensitive   = true

  validation {
    condition     = can(regex("^arn:[^:]+:secretsmanager:", var.cluster_cookie_secret_arn))
    error_message = "cluster_cookie_secret_arn must be an AWS Secrets Manager secret ARN."
  }
}

variable "cluster_cookie_kms_key_arn" {
  description = "Optional customer-managed KMS key ARN used by the cookie secret. Leave null for the AWS managed key."
  type        = string
  default     = null
}

variable "cpu" {
  description = "Fargate CPU units for each FerricStore node."
  type        = number
  default     = 2048

  validation {
    condition     = contains([256, 512, 1024, 2048, 4096, 8192, 16384], var.cpu)
    error_message = "cpu must be a Fargate-supported CPU size."
  }
}

variable "memory" {
  description = "Fargate memory in MiB for each FerricStore node."
  type        = number
  default     = 4096

  validation {
    condition     = var.memory >= 512 && floor(var.memory) == var.memory
    error_message = "memory must be an integer number of MiB greater than or equal to 512."
  }
}

variable "ephemeral_storage_gib" {
  description = "Task-local storage per node. It is rebuilt from surviving replicas after replacement."
  type        = number
  default     = 80

  validation {
    condition     = var.ephemeral_storage_gib >= 21 && var.ephemeral_storage_gib <= 200 && floor(var.ephemeral_storage_gib) == var.ephemeral_storage_gib
    error_message = "ephemeral_storage_gib must be an integer between 21 and 200 GiB."
  }
}

variable "cpu_architecture" {
  description = "Fargate task CPU architecture."
  type        = string
  default     = "X86_64"

  validation {
    condition     = contains(["X86_64", "ARM64"], var.cpu_architecture)
    error_message = "cpu_architecture must be X86_64 or ARM64."
  }
}

variable "shard_count" {
  description = "Fixed FerricStore shard count shared by all three nodes."
  type        = number
  default     = 16

  validation {
    condition     = var.shard_count > 0 && floor(var.shard_count) == var.shard_count
    error_message = "shard_count must be a positive integer."
  }
}

variable "allowed_client_cidr_blocks" {
  description = "CIDRs allowed to reach the native endpoint. An empty list uses the stack VPC CIDR."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for cidr in var.allowed_client_cidr_blocks : can(cidrnetmask(cidr))])
    error_message = "Every allowed client entry must be a valid IPv4 CIDR."
  }
}

variable "cluster_remove_delay_ms" {
  description = "How long a missing stable node remains in Raft membership before removal is attempted."
  type        = number
  default     = 600000

  validation {
    condition     = var.cluster_remove_delay_ms >= 60000 && floor(var.cluster_remove_delay_ms) == var.cluster_remove_delay_ms
    error_message = "cluster_remove_delay_ms must be an integer of at least 60000."
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
