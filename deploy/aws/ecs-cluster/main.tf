data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  node_slots = {
    node-0 = 0
    node-1 = 1
    node-2 = 2
  }

  availability_zones = slice(data.aws_availability_zones.available.names, 0, 3)
  client_cidr_blocks = length(var.allowed_client_cidr_blocks) > 0 ? var.allowed_client_cidr_blocks : [var.vpc_cidr]
  container_name     = "ferricstore"
  native_port        = 6388
  dashboard_port     = 6380
  health_probe_port  = 6381
  http_target_port   = 8080
  epmd_port          = 4369
  distribution_port  = 9100
  ecs_optimized_ami_parameter = var.ec2_optimized_ami_parameter != null ? var.ec2_optimized_ami_parameter : (
    var.cpu_architecture == "ARM64" ?
    "/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id" :
    "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id"
  )

  node_hosts = {
    for slot, _index in local.node_slots :
    slot => "${slot}.${var.service_discovery_namespace}"
  }

  node_names = {
    for slot, host in local.node_hosts :
    slot => "ferricstore@${host}"
  }

  cluster_nodes = join(",", [for slot in sort(keys(local.node_names)) : local.node_names[slot]])
}

data "aws_ssm_parameter" "ecs_optimized_ami" {
  name = local.ecs_optimized_ami_parameter
}

check "three_availability_zones" {
  assert {
    condition     = length(data.aws_availability_zones.available.names) >= 3
    error_message = "The selected region must expose at least three available availability zones."
  }
}

check "valid_ecs_task_size" {
  assert {
    condition = (
      var.cpu >= 256 && var.cpu <= 16384 && var.cpu % 256 == 0 &&
      var.memory >= 512 && var.memory <= 122880 && floor(var.memory) == var.memory
    )
    error_message = "cpu must be an ECS task size and memory must be an integer from 512 through 122880 MiB."
  }
}

check "valid_http_tls_configuration" {
  assert {
    condition = (
      var.http_tls_certificate_arn == null ||
      (var.http_enabled && var.http_hostname != null)
    )
    error_message = "http_tls_certificate_arn requires http_enabled=true and an http_hostname covered by the certificate."
  }
}

# -----------------------------------------------------------------------------
# Three-AZ network. Each logical node slot is pinned to one private subnet.
# -----------------------------------------------------------------------------

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${var.name_prefix}-vpc" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name_prefix}-igw" }
}

resource "aws_subnet" "public" {
  count = 3

  vpc_id                  = aws_vpc.this.id
  availability_zone       = local.availability_zones[count.index]
  cidr_block              = cidrsubnet(var.vpc_cidr, 4, count.index)
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.name_prefix}-public-${count.index}"
    Tier = "public"
  }
}

resource "aws_subnet" "private" {
  count = 3

  vpc_id                  = aws_vpc.this.id
  availability_zone       = local.availability_zones[count.index]
  cidr_block              = cidrsubnet(var.vpc_cidr, 4, count.index + 8)
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.name_prefix}-private-${count.index}"
    Tier = "private"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = { Name = "${var.name_prefix}-public" }
}

resource "aws_route_table_association" "public" {
  count = 3

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# A NAT gateway per AZ avoids making task recovery depend on another AZ. For a
# private ECR-only deployment these can be replaced with the required VPC
# endpoints, but a public Quay.io image needs internet egress.
resource "aws_eip" "nat" {
  count  = 3
  domain = "vpc"

  depends_on = [aws_internet_gateway.this]
  tags       = { Name = "${var.name_prefix}-nat-${count.index}" }
}

resource "aws_nat_gateway" "this" {
  count = 3

  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id

  depends_on = [aws_internet_gateway.this]
  tags       = { Name = "${var.name_prefix}-nat-${count.index}" }
}

resource "aws_route_table" "private" {
  count  = 3
  vpc_id = aws_vpc.this.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this[count.index].id
  }

  tags = { Name = "${var.name_prefix}-private-${count.index}" }
}

resource "aws_route_table_association" "private" {
  count = 3

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

# -----------------------------------------------------------------------------
# Security groups: client traffic is CIDR-scoped; cluster ports are self-only.
# -----------------------------------------------------------------------------

resource "aws_security_group" "task" {
  name_prefix = "${var.name_prefix}-task-"
  description = "FerricStore clustered ECS task traffic"
  vpc_id      = aws_vpc.this.id

  egress {
    description = "Outbound image, log, control-plane, and peer traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.name_prefix}-task" }
}

resource "aws_security_group" "instance" {
  name_prefix = "${var.name_prefix}-instance-"
  description = "ECS container instance control-plane egress"
  vpc_id      = aws_vpc.this.id

  egress {
    description = "Outbound ECS, image, log, and SSM traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.name_prefix}-instance" }
}

resource "aws_vpc_security_group_ingress_rule" "native" {
  for_each = toset(local.client_cidr_blocks)

  security_group_id = aws_security_group.task.id
  description       = "Ferric native protocol from approved clients"
  from_port         = local.native_port
  to_port           = local.native_port
  ip_protocol       = "tcp"
  cidr_ipv4         = each.value
}

resource "aws_vpc_security_group_ingress_rule" "http" {
  for_each = var.http_enabled ? toset(local.client_cidr_blocks) : toset([])

  security_group_id = aws_security_group.task.id
  description       = "Ferric HTTP API from approved clients"
  from_port         = local.http_target_port
  to_port           = local.http_target_port
  ip_protocol       = "tcp"
  cidr_ipv4         = each.value
}

resource "aws_vpc_security_group_ingress_rule" "health" {
  security_group_id = aws_security_group.task.id
  description       = "NLB liveness checks from within the VPC"
  from_port         = local.health_probe_port
  to_port           = local.health_probe_port
  ip_protocol       = "tcp"
  cidr_ipv4         = var.vpc_cidr
}

resource "aws_vpc_security_group_ingress_rule" "epmd" {
  security_group_id            = aws_security_group.task.id
  description                  = "Erlang port mapper between FerricStore nodes"
  from_port                    = local.epmd_port
  to_port                      = local.epmd_port
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.task.id
}

resource "aws_vpc_security_group_ingress_rule" "distribution" {
  security_group_id            = aws_security_group.task.id
  description                  = "Fixed Erlang distribution port between FerricStore nodes"
  from_port                    = local.distribution_port
  to_port                      = local.distribution_port
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.task.id
}

# -----------------------------------------------------------------------------
# Cloud Map gives each slot a stable name while ECS changes its task IP.
# -----------------------------------------------------------------------------

resource "aws_service_discovery_private_dns_namespace" "this" {
  name        = var.service_discovery_namespace
  description = "Stable FerricStore ECS node identities"
  vpc         = aws_vpc.this.id
}

resource "aws_service_discovery_service" "node" {
  for_each = local.node_slots

  name = each.key

  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.this.id

    dns_records {
      ttl  = 5
      type = "A"
    }

    routing_policy = "MULTIVALUE"
  }
}

# -----------------------------------------------------------------------------
# IAM, logs, ECS cluster, and one task definition per stable node identity.
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "ecs_task_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name_prefix        = "${var.name_prefix}-execution-"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "execution_secret" {
  statement {
    sid       = "ReadClusterCookie"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.cluster_cookie_secret_arn]
  }

  dynamic "statement" {
    for_each = var.cluster_cookie_kms_key_arn == null ? [] : [var.cluster_cookie_kms_key_arn]

    content {
      sid       = "DecryptClusterCookie"
      actions   = ["kms:Decrypt"]
      resources = [statement.value]
    }
  }
}

resource "aws_iam_role_policy" "execution_secret" {
  name_prefix = "${var.name_prefix}-cookie-"
  role        = aws_iam_role.execution.id
  policy      = data.aws_iam_policy_document.execution_secret.json
}

resource "aws_iam_role" "task" {
  name_prefix        = "${var.name_prefix}-task-"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
}

data "aws_iam_policy_document" "task" {
  statement {
    sid = "EcsExecChannels"
    actions = [
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "task" {
  name_prefix = "${var.name_prefix}-task-"
  role        = aws_iam_role.task.id
  policy      = data.aws_iam_policy_document.task.json
}

data "aws_iam_policy_document" "ecs_instance_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_instance" {
  name_prefix        = "${var.name_prefix}-instance-"
  assume_role_policy = data.aws_iam_policy_document.ecs_instance_assume.json
}

resource "aws_iam_instance_profile" "ecs_instance" {
  name_prefix = "${var.name_prefix}-instance-"
  role        = aws_iam_role.ecs_instance.name
}

resource "aws_iam_role_policy_attachment" "ecs_instance" {
  role       = aws_iam_role.ecs_instance.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role_policy_attachment" "ecs_instance_ssm" {
  role       = aws_iam_role.ecs_instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/ecs/${var.name_prefix}-cluster"
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_cluster" "this" {
  name = "${var.name_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_launch_template" "ecs_instance" {
  for_each = local.node_slots

  name_prefix   = "${var.name_prefix}-${each.key}-instance-"
  image_id      = data.aws_ssm_parameter.ecs_optimized_ami.value
  instance_type = var.ec2_instance_type

  iam_instance_profile {
    name = aws_iam_instance_profile.ecs_instance.name
  }

  vpc_security_group_ids = [aws_security_group.instance.id]

  user_data = base64encode(<<-EOF
    #!/bin/bash
    set -euo pipefail
    cat >/etc/ecs/ecs.config <<ECS_CONFIG
    ECS_CLUSTER=${aws_ecs_cluster.this.name}
    ECS_ENABLE_CONTAINER_METADATA=true
    ECS_CONFIG
  EOF
  )

  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      volume_size = var.ec2_root_volume_gib
      volume_type = "gp3"
      encrypted   = true
      # Keep the root volume when an ASG replaces an instance. The replacement
      # starts with a fresh root and rebuilds its replica from the surviving
      # nodes, but the old volume remains available for forensic recovery or a
      # manually coordinated restore instead of being physically deleted.
      delete_on_termination = false
    }
  }

  tag_specifications {
    resource_type = "instance"

    tags = {
      Name     = "${var.name_prefix}-${each.key}-ecs-instance"
      NodeSlot = each.key
    }
  }

  # Root volumes contain FerricStore's instance-local replica. Tag them so an
  # operator can find retained volumes after an instance replacement or stack
  # teardown; delete_on_termination is deliberately false above.
  tag_specifications {
    resource_type = "volume"

    tags = {
      Name     = "${var.name_prefix}-${each.key}-ferricstore-data"
      NodeSlot = each.key
      DataRole = "FerricStoreReplica"
      Retained = "true"
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.ecs_instance,
    aws_iam_role_policy_attachment.ecs_instance_ssm
  ]
}

resource "aws_autoscaling_group" "ecs_instance" {
  for_each = local.node_slots

  name_prefix           = "${var.name_prefix}-${each.key}-ecs-"
  min_size              = 1
  max_size              = var.ec2_max_size
  desired_capacity      = 1
  vpc_zone_identifier   = [aws_subnet.private[each.value].id]
  protect_from_scale_in = true

  launch_template {
    id      = aws_launch_template.ecs_instance[each.key].id
    version = aws_launch_template.ecs_instance[each.key].latest_version
  }

  tag {
    key                 = "Name"
    value               = "${var.name_prefix}-${each.key}-ecs-instance"
    propagate_at_launch = true
  }

  tag {
    key                 = "NodeSlot"
    value               = each.key
    propagate_at_launch = true
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_ecs_capacity_provider" "ec2" {
  for_each = local.node_slots

  name = "${var.name_prefix}-${each.key}-ec2"

  auto_scaling_group_provider {
    auto_scaling_group_arn         = aws_autoscaling_group.ecs_instance[each.key].arn
    managed_termination_protection = "ENABLED"

    managed_scaling {
      status                    = "ENABLED"
      target_capacity           = 100
      minimum_scaling_step_size = 1
      maximum_scaling_step_size = 1
    }
  }
}

resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.this.name
  capacity_providers = [for slot in sort(keys(local.node_slots)) : aws_ecs_capacity_provider.ec2[slot].name]
}

resource "aws_ecs_task_definition" "node" {
  for_each = local.node_slots

  family                   = "${var.name_prefix}-${each.key}"
  skip_destroy             = true
  requires_compatibilities = ["EC2"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.cpu)
  memory                   = tostring(var.memory)
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  volume {
    name      = "data"
    host_path = "/var/lib/ferricstore/${var.name_prefix}/${each.key}/data"
  }

  container_definitions = jsonencode([
    {
      name      = "data-init"
      image     = var.container_image
      essential = false
      user      = "0:0"
      # A host_path bind mount is expected here. Failing closed protects the
      # database if the instance bootstrap has not prepared the mount and ECS
      # would otherwise create an empty directory on the root filesystem.
      command = ["/bin/sh", "-ec", "grep -Eq '[[:space:]]/data[[:space:]]' /proc/self/mountinfo || { echo 'FerricStore data mount is missing' >&2; exit 1; }; chown 10001:10001 /data && chmod 0700 /data"]

      mountPoints = [{
        sourceVolume  = "data"
        containerPath = "/data"
        readOnly      = false
      }]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.this.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "${each.key}-data-init"
        }
      }
    },
    {
      name        = local.container_name
      image       = var.container_image
      essential   = true
      user        = "10001:10001"
      stopTimeout = 120

      # Cloud Map registration is configured while ECS prepares the task. Wait
      # until this slot name resolves to this task rather than a cached old IP.
      command = [
        "/bin/bash",
        "-ec",
        "task_ip=\"$(hostname -I)\"; task_ip=\"$${task_ip%% *}\"; for attempt in {1..180}; do if getent ahostsv4 \"$FERRICSTORE_NODE_HOST\" | grep -q \"^$${task_ip}[[:space:]]\"; then exec /app/bin/ferricstore start; fi; sleep 1; done; echo \"stable node DNS did not converge to $${task_ip}\" >&2; exit 1"
      ]

      dependsOn = [{
        containerName = "data-init"
        condition     = "SUCCESS"
      }]

      portMappings = concat([
        {
          name          = "native"
          containerPort = local.native_port
          hostPort      = local.native_port
          protocol      = "tcp"
        },
        {
          name          = "dashboard"
          containerPort = local.dashboard_port
          hostPort      = local.dashboard_port
          protocol      = "tcp"
        },
        {
          name          = "health-probe"
          containerPort = local.health_probe_port
          hostPort      = local.health_probe_port
          protocol      = "tcp"
        },
        {
          name          = "epmd"
          containerPort = local.epmd_port
          hostPort      = local.epmd_port
          protocol      = "tcp"
        },
        {
          name          = "distribution"
          containerPort = local.distribution_port
          hostPort      = local.distribution_port
          protocol      = "tcp"
        }
        ], var.http_enabled ? [
        {
          name          = "http-api"
          containerPort = local.http_target_port
          hostPort      = local.http_target_port
          protocol      = "tcp"
        }
      ] : [])

      environment = [
        { name = "FERRICSTORE_DATA_DIR", value = "/data" },
        { name = "FERRICSTORE_NATIVE_PORT", value = tostring(local.native_port) },
        { name = "FERRICSTORE_HEALTH_PORT", value = tostring(local.dashboard_port) },
        { name = "FERRICSTORE_HEALTH_PROBE_PORT", value = tostring(local.health_probe_port) },
        { name = "FERRICSTORE_HTTP_ENABLED", value = tostring(var.http_enabled) },
        { name = "FERRICSTORE_HTTP_BIND", value = "0.0.0.0" },
        { name = "FERRICSTORE_HTTP_PORT", value = tostring(local.http_target_port) },
        { name = "FERRICSTORE_HTTP_TLS_ENABLED", value = "false" },
        { name = "FERRICSTORE_SHARD_COUNT", value = tostring(var.shard_count) },
        { name = "FERRICSTORE_PROTECTED_MODE", value = "false" },
        { name = "FERRICSTORE_NODE_HOST", value = local.node_hosts[each.key] },
        { name = "FERRICSTORE_NODE_NAME", value = local.node_names[each.key] },
        { name = "RELEASE_NODE", value = local.node_names[each.key] },
        { name = "RELEASE_DISTRIBUTION", value = "name" },
        { name = "FERRICSTORE_NATIVE_ADVERTISE_HOST", value = local.node_hosts[each.key] },
        { name = "FERRICSTORE_CLUSTER_NODES", value = local.cluster_nodes },
        { name = "FERRICSTORE_CLUSTER_AUTO_JOIN", value = "true" },
        { name = "FERRICSTORE_CLUSTER_REMOVE_DELAY_MS", value = tostring(var.cluster_remove_delay_ms) },
        { name = "FERRICSTORE_DISCOVERY", value = "epmd" },
        { name = "FERRICSTORE_EPMD_POLL_INTERVAL_MS", value = "5000" },
        { name = "ELIXIR_ERL_OPTIONS", value = "+fnu -kernel inet_dist_listen_min ${local.distribution_port} inet_dist_listen_max ${local.distribution_port}" }
      ]

      secrets = [
        {
          name      = "FERRICSTORE_COOKIE"
          valueFrom = var.cluster_cookie_secret_arn
        },
        {
          name      = "RELEASE_COOKIE"
          valueFrom = var.cluster_cookie_secret_arn
        }
      ]

      mountPoints = [{
        sourceVolume  = "data"
        containerPath = "/data"
        readOnly      = false
      }]

      linuxParameters = { initProcessEnabled = true }

      # ECS treats load-balancer and container health failures as replacement
      # signals. Use liveness here so a temporary quorum outage does not cause
      # destructive replacement churn of instance-local replica disks.
      healthCheck = {
        command = [
          "CMD-SHELL",
          "bash -ec 'exec 3<>/dev/tcp/127.0.0.1/6381; printf \"GET /health/live HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n\" >&3; IFS= read -r status <&3; [[ \"$status\" == *\" 200 \"* ]]'"
        ]
        interval    = 30
        timeout     = 5
        retries     = 5
        startPeriod = 180
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.this.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = each.key
        }
      }
    }
  ])
}

# -----------------------------------------------------------------------------
# One target group and one desired-count-one ECS service per stable slot.
# -----------------------------------------------------------------------------

resource "aws_lb" "this" {
  name                             = "${var.name_prefix}-cluster"
  internal                         = true
  load_balancer_type               = "network"
  subnets                          = aws_subnet.private[*].id
  enable_cross_zone_load_balancing = true
}

resource "aws_lb_target_group" "native" {
  for_each = local.node_slots

  name        = "${var.name_prefix}-${replace(each.key, "node-", "n")}-native"
  port        = local.native_port
  protocol    = "TCP"
  target_type = "ip"
  vpc_id      = aws_vpc.this.id

  deregistration_delay = 30

  health_check {
    enabled             = true
    protocol            = "HTTP"
    port                = tostring(local.health_probe_port)
    path                = "/health/live"
    matcher             = "200"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_target_group" "http" {
  for_each = var.http_enabled ? local.node_slots : {}

  name               = "${var.name_prefix}-${replace(each.key, "node-", "n")}-http"
  port               = local.http_target_port
  protocol           = "TCP"
  target_type        = "ip"
  vpc_id             = aws_vpc.this.id
  preserve_client_ip = true

  deregistration_delay = 30

  # ECS replaces tasks that fail any attached target group's health check.
  # Use the isolated node liveness endpoint so API overload or a temporary
  # quorum/readiness outage cannot trigger destruction of replica disks.
  health_check {
    enabled             = true
    protocol            = "HTTP"
    port                = tostring(local.health_probe_port)
    path                = "/health/live"
    matcher             = "200"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "native" {
  load_balancer_arn = aws_lb.this.arn
  port              = local.native_port
  protocol          = "TCP"

  default_action {
    type = "forward"

    forward {
      target_group {
        arn    = aws_lb_target_group.native["node-0"].arn
        weight = 1
      }

      target_group {
        arn    = aws_lb_target_group.native["node-1"].arn
        weight = 1
      }

      target_group {
        arn    = aws_lb_target_group.native["node-2"].arn
        weight = 1
      }
    }
  }
}

resource "aws_lb_listener" "http" {
  count = var.http_enabled && var.http_tls_certificate_arn == null ? 1 : 0

  load_balancer_arn = aws_lb.this.arn
  port              = var.http_listener_port
  protocol          = "TCP"

  default_action {
    type = "forward"

    forward {
      target_group {
        arn    = aws_lb_target_group.http["node-0"].arn
        weight = 1
      }

      target_group {
        arn    = aws_lb_target_group.http["node-1"].arn
        weight = 1
      }

      target_group {
        arn    = aws_lb_target_group.http["node-2"].arn
        weight = 1
      }
    }
  }
}

resource "aws_lb_listener" "https" {
  count = var.http_enabled && var.http_tls_certificate_arn != null ? 1 : 0

  load_balancer_arn = aws_lb.this.arn
  port              = var.http_listener_port
  protocol          = "TLS"
  certificate_arn   = var.http_tls_certificate_arn
  ssl_policy        = var.http_tls_security_policy

  default_action {
    type = "forward"

    forward {
      target_group {
        arn    = aws_lb_target_group.http["node-0"].arn
        weight = 1
      }

      target_group {
        arn    = aws_lb_target_group.http["node-1"].arn
        weight = 1
      }

      target_group {
        arn    = aws_lb_target_group.http["node-2"].arn
        weight = 1
      }
    }
  }
}

resource "aws_ecs_service" "node" {
  for_each = local.node_slots

  name            = "${var.name_prefix}-${each.key}"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.node[each.key].arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.ec2[each.key].name
    weight            = 1
    base              = 1
  }

  enable_execute_command             = true
  health_check_grace_period_seconds  = 180
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  propagate_tags                     = "SERVICE"

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    # One subnet gives this stateful logical slot a deterministic AZ and keeps
    # the three replicas in separate failure domains.
    subnets          = [aws_subnet.private[each.value].id]
    security_groups  = [aws_security_group.task.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.native[each.key].arn
    container_name   = local.container_name
    container_port   = local.native_port
  }

  dynamic "load_balancer" {
    for_each = var.http_enabled ? [1] : []

    content {
      target_group_arn = aws_lb_target_group.http[each.key].arn
      container_name   = local.container_name
      container_port   = local.http_target_port
    }
  }

  service_registries {
    registry_arn = aws_service_discovery_service.node[each.key].arn
  }

  # Terraform registers new task-definition revisions, but intentionally does
  # not roll all three services in parallel. scripts/deploy-sequential.sh moves
  # one slot at a time and checks replica recovery before continuing.
  lifecycle {
    ignore_changes = [task_definition]
  }

  depends_on = [
    aws_iam_role_policy_attachment.execution,
    aws_iam_role_policy.execution_secret,
    aws_iam_role_policy.task,
    aws_iam_role_policy_attachment.ecs_instance,
    aws_ecs_cluster_capacity_providers.this,
    aws_lb_listener.native,
    aws_lb_listener.http,
    aws_lb_listener.https
  ]
}
