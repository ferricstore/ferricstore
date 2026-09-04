output "endpoint" {
  description = "Private Ferric native protocol endpoint."
  value       = "ferric://${aws_lb.this.dns_name}:${local.native_port}"
}

output "http_endpoint" {
  description = "Private HTTP command API endpoint, or null when http_enabled is false."
  value = var.http_enabled ? format(
    "%s://%s:%d",
    var.http_tls_certificate_arn == null ? "http" : "https",
    var.http_hostname == null ? aws_lb.this.dns_name : var.http_hostname,
    var.http_listener_port
  ) : null
}

output "http_readiness_endpoint" {
  description = "Unauthenticated HTTP API readiness endpoint, or null when HTTP is disabled."
  value = var.http_enabled ? format(
    "%s://%s:%d/ready",
    var.http_tls_certificate_arn == null ? "http" : "https",
    var.http_hostname == null ? aws_lb.this.dns_name : var.http_hostname,
    var.http_listener_port
  ) : null
}

output "http_enabled" {
  description = "Whether the desired task and ECS service configuration includes the HTTP API."
  value       = var.http_enabled
}

output "native_target_group_arns" {
  description = "Native target group ARN by stable node slot, used by the guarded rollout."
  value       = { for slot, target_group in aws_lb_target_group.native : slot => target_group.arn }
}

output "http_target_group_arns" {
  description = "HTTP target group ARN by stable node slot, used by the guarded rollout when HTTP is enabled."
  value       = { for slot, target_group in aws_lb_target_group.http : slot => target_group.arn }
}

output "aws_region" {
  value = var.aws_region
}

output "name_prefix" {
  value = var.name_prefix
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "ecs_service_names" {
  value = { for slot, service in aws_ecs_service.node : slot => service.name }
}

output "stable_node_names" {
  value = local.node_names
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "deployment_profile" {
  value = "oss-three-node-ecs-ec2-cluster-ephemeral-replicas"
}

output "failure_contract" {
  value = "One task or container instance/AZ may be replaced and recover from the other two. Never replace or upgrade two slots together; simultaneous loss of two replicas can lose quorum or data. Loss of all three root volumes loses all data."
}

output "ecs_capacity_providers" {
  description = "Per-AZ ECS EC2 capacity providers used by the stable node services."
  value       = { for slot, provider in aws_ecs_capacity_provider.ec2 : slot => provider.name }
}

output "ecs_autoscaling_groups" {
  description = "Per-AZ ECS container-instance Auto Scaling groups."
  value       = { for slot, group in aws_autoscaling_group.ecs_instance : slot => group.name }
}
