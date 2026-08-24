output "endpoint" {
  description = "Ferric native protocol endpoint."
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

output "deployment_profile" {
  description = "FerricStore deployment contract implemented by this stack."
  value       = "oss-single-task-ephemeral"
}

output "data_persistence_warning" {
  description = "Data-lifecycle warning for operators and automation."
  value       = "All FerricStore data is lost when the Fargate task stops or is replaced."
}

output "load_balancer_dns_name" {
  description = "Network Load Balancer DNS name."
  value       = aws_lb.this.dns_name
}

output "ecs_cluster_name" {
  description = "ECS cluster name."
  value       = aws_ecs_cluster.this.name
}

output "ecs_service_name" {
  description = "ECS service name."
  value       = aws_ecs_service.this.name
}

output "private_subnet_ids" {
  description = "Private subnets in which Fargate tasks run."
  value       = aws_subnet.private[*].id
}

output "ecs_exec_command" {
  description = "Command template for opening a shell in the running task after replacing TASK_ID."
  value       = "aws ecs execute-command --region ${var.aws_region} --cluster ${aws_ecs_cluster.this.name} --task TASK_ID --container ${local.container_name} --interactive --command /bin/bash"
}
