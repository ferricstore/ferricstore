variable "aws_region" {
  description = "AWS region in which to create the three-node ECS-on-EC2 cluster."
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
  description = "ECS task CPU units for each FerricStore node."
  type        = number
  default     = 2048

  validation {
    condition     = contains([256, 512, 1024, 2048, 4096, 8192, 16384], var.cpu)
    error_message = "cpu must be an ECS task CPU size from 256 through 16384 units."
  }
}

variable "memory" {
  description = "ECS task memory in MiB for each FerricStore node. The EC2 instance must have enough allocatable memory."
  type        = number
  default     = 4096

  validation {
    condition     = var.memory >= 512 && floor(var.memory) == var.memory
    error_message = "memory must be an integer number of MiB greater than or equal to 512."
  }
}

variable "ec2_instance_type" {
  description = "EC2 instance type for each per-AZ ECS capacity provider. Size it for one FerricStore node and ECS overhead."
  type        = string
  default     = "m7i.large"

  validation {
    condition     = trimspace(var.ec2_instance_type) != ""
    error_message = "ec2_instance_type must not be empty."
  }
}

variable "ec2_optimized_ami_parameter" {
  description = "Optional SSM parameter containing the ECS-optimized AMI ID. When null, the parameter is selected from cpu_architecture."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.ec2_optimized_ami_parameter == null || startswith(var.ec2_optimized_ami_parameter, "/aws/service/ecs/optimized-ami/")
    error_message = "ec2_optimized_ami_parameter must be null or reference the ECS optimized AMI public SSM path."
  }
}

variable "ec2_root_volume_gib" {
  description = "Encrypted gp3 root volume size for each ECS container instance. Node data is lost if that instance is replaced."
  type        = number
  default     = 80

  validation {
    condition     = var.ec2_root_volume_gib >= 30 && var.ec2_root_volume_gib <= 16384 && floor(var.ec2_root_volume_gib) == var.ec2_root_volume_gib
    error_message = "ec2_root_volume_gib must be an integer between 30 and 16384 GiB."
  }
}

variable "ec2_max_size" {
  description = "Maximum instances per per-AZ capacity provider. Keep at least one spare slot for rolling replacement."
  type        = number
  default     = 2

  validation {
    condition     = var.ec2_max_size >= 1 && floor(var.ec2_max_size) == var.ec2_max_size
    error_message = "ec2_max_size must be a positive integer."
  }
}

variable "cpu_architecture" {
  description = "ECS task and container-instance CPU architecture."
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
  description = "CIDRs allowed to reach the native endpoint and the optional HTTP endpoint. An empty list uses the stack VPC CIDR."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for cidr in var.allowed_client_cidr_blocks : can(cidrnetmask(cidr))])
    error_message = "Every allowed client entry must be a valid IPv4 CIDR."
  }
}

variable "http_enabled" {
  description = "Expose FerricStore's authenticated HTTP command API through the internal NLB."
  type        = bool
  default     = false
}

variable "http_listener_port" {
  description = "Client-facing NLB port for HTTP or HTTPS. Each task listener remains on unprivileged port 8080."
  type        = number
  default     = 8080

  validation {
    condition = (
      var.http_listener_port >= 1 &&
      var.http_listener_port <= 65535 &&
      floor(var.http_listener_port) == var.http_listener_port &&
      var.http_listener_port != 6388
    )
    error_message = "http_listener_port must be an integer from 1 through 65535 and must not collide with native port 6388."
  }
}

variable "http_tls_certificate_arn" {
  description = "Optional ACM or IAM certificate ARN. When set, the NLB terminates TLS before forwarding over the private VPC."
  type        = string
  default     = null

  validation {
    condition     = var.http_tls_certificate_arn == null || can(regex("^arn:[^:]+:(acm|iam):", var.http_tls_certificate_arn))
    error_message = "http_tls_certificate_arn must be null or an ACM/IAM certificate ARN."
  }
}

variable "http_tls_security_policy" {
  description = "NLB TLS security policy used when http_tls_certificate_arn is set."
  type        = string
  default     = "ELBSecurityPolicy-TLS13-1-2-Res-2021-06"

  validation {
    condition     = startswith(var.http_tls_security_policy, "ELBSecurityPolicy-")
    error_message = "http_tls_security_policy must be an Elastic Load Balancing security policy name."
  }
}

variable "http_hostname" {
  description = "Optional client DNS name used in the HTTP endpoint output. Required for TLS and must resolve to the NLB outside this stack."
  type        = string
  default     = null

  validation {
    condition = (
      var.http_hostname == null ||
      can(regex("^[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?$", var.http_hostname))
    )
    error_message = "http_hostname must be null or a valid DNS hostname."
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
