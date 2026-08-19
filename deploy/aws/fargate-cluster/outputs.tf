output "endpoint" {
  description = "Private Ferric native protocol endpoint."
  value       = "ferric://${aws_lb.this.dns_name}:${local.native_port}"
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
  value = "oss-three-node-fargate-cluster-ephemeral-replicas"
}

output "failure_contract" {
  value = "One task/AZ may be replaced and recover from the other two. Never replace or upgrade two slots together; simultaneous loss of two replicas can lose quorum or data. Loss of all three tasks loses all data."
}
