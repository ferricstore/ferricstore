data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  availability_zones = slice(
    data.aws_availability_zones.available.names,
    0,
    var.availability_zone_count
  )
  client_cidr_blocks = length(var.allowed_client_cidr_blocks) > 0 ? var.allowed_client_cidr_blocks : [var.vpc_cidr]
  container_name     = "ferricstore"
  native_port        = 6388
  dashboard_port     = 6380
  health_probe_port  = 6381
  http_target_port   = 8080
  ecs_optimized_ami_parameter = var.ec2_optimized_ami_parameter != null ? var.ec2_optimized_ami_parameter : (
    var.cpu_architecture == "ARM64" ?
    "/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id" :
    "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id"
  )
}

data "aws_ssm_parameter" "ecs_optimized_ami" {
  name = local.ecs_optimized_ami_parameter
}

check "valid_ecs_task_size" {
  assert {
    condition = (
      var.cpu >= 256 && var.cpu <= 16384 && var.cpu % 256 == 0 &&
      var.memory >= 512 && var.memory <= 122880 && floor(var.memory) == var.memory
    )
    error_message = "cpu must be a positive ECS task size and memory must be an integer from 512 through 122880 MiB."
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
# Network
# -----------------------------------------------------------------------------

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.name_prefix}-vpc"
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${var.name_prefix}-igw"
  }
}

resource "aws_subnet" "public" {
  count = var.availability_zone_count

  vpc_id                  = aws_vpc.this.id
  availability_zone       = local.availability_zones[count.index]
  cidr_block              = cidrsubnet(var.vpc_cidr, 4, count.index)
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.name_prefix}-public-${count.index + 1}"
    Tier = "public"
  }
}

resource "aws_subnet" "private" {
  count = var.availability_zone_count

  vpc_id                  = aws_vpc.this.id
  availability_zone       = local.availability_zones[count.index]
  cidr_block              = cidrsubnet(var.vpc_cidr, 4, count.index + 8)
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.name_prefix}-private-${count.index + 1}"
    Tier = "private"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = {
    Name = "${var.name_prefix}-public"
  }
}

resource "aws_route_table_association" "public" {
  count = var.availability_zone_count

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# The default Quay.io image requires internet egress. A single NAT gateway keeps
# this reference deployment compact; use an ECR image plus VPC endpoints or a
# NAT gateway per AZ when your availability/cost requirements differ.
resource "aws_eip" "nat" {
  domain = "vpc"

  depends_on = [aws_internet_gateway.this]

  tags = {
    Name = "${var.name_prefix}-nat"
  }
}

resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  depends_on = [aws_internet_gateway.this]

  tags = {
    Name = "${var.name_prefix}-nat"
  }
}

resource "aws_route_table" "private" {
  count = var.availability_zone_count

  vpc_id = aws_vpc.this.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this.id
  }

  tags = {
    Name = "${var.name_prefix}-private-${count.index + 1}"
  }
}

resource "aws_route_table_association" "private" {
  count = var.availability_zone_count

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

# -----------------------------------------------------------------------------
# Security groups
# -----------------------------------------------------------------------------

resource "aws_security_group" "task" {
  name_prefix = "${var.name_prefix}-task-"
  description = "FerricStore ECS task traffic"
  vpc_id      = aws_vpc.this.id

  egress {
    description = "Outbound image, log, and control-plane traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.name_prefix}-task"
  }
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

  tags = {
    Name = "${var.name_prefix}-instance"
  }
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
  description       = "NLB readiness checks from within the VPC"
  from_port         = local.health_probe_port
  to_port           = local.health_probe_port
  ip_protocol       = "tcp"
  cidr_ipv4         = var.vpc_cidr
}

# -----------------------------------------------------------------------------
# IAM, logging, and ECS task
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
  name              = "/ecs/${var.name_prefix}"
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_cluster" "this" {
  name = var.name_prefix

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_launch_template" "ecs_instance" {
  name_prefix   = "${var.name_prefix}-instance-"
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
      volume_size           = var.ec2_root_volume_gib
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }

  tag_specifications {
    resource_type = "instance"

    tags = {
      Name = "${var.name_prefix}-ecs-instance"
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.ecs_instance,
    aws_iam_role_policy_attachment.ecs_instance_ssm
  ]
}

resource "aws_autoscaling_group" "ecs_instance" {
  name_prefix           = "${var.name_prefix}-ecs-"
  min_size              = 1
  max_size              = var.ec2_max_size
  desired_capacity      = 1
  vpc_zone_identifier   = aws_subnet.private[*].id
  protect_from_scale_in = true

  launch_template {
    id      = aws_launch_template.ecs_instance.id
    version = aws_launch_template.ecs_instance.latest_version
  }

  tag {
    key                 = "Name"
    value               = "${var.name_prefix}-ecs-instance"
    propagate_at_launch = true
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_ecs_capacity_provider" "ec2" {
  name = "${var.name_prefix}-ec2"

  auto_scaling_group_provider {
    auto_scaling_group_arn         = aws_autoscaling_group.ecs_instance.arn
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
  capacity_providers = [aws_ecs_capacity_provider.ec2.name]

  default_capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.ec2.name
    weight            = 1
    base              = 1
  }
}

resource "aws_ecs_task_definition" "this" {
  family                   = var.name_prefix
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
    host_path = "/var/lib/ferricstore/${var.name_prefix}/data"
  }

  container_definitions = jsonencode([
    {
      name      = "data-init"
      image     = var.container_image
      essential = false
      user      = "0:0"
      command   = ["/bin/sh", "-c", "chown 10001:10001 /data && chmod 0700 /data"]

      mountPoints = [
        {
          sourceVolume  = "data"
          containerPath = "/data"
          readOnly      = false
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.this.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "data-init"
        }
      }
    },
    {
      name        = local.container_name
      image       = var.container_image
      essential   = true
      user        = "10001:10001"
      stopTimeout = 120

      dependsOn = [
        {
          containerName = "data-init"
          condition     = "SUCCESS"
        }
      ]

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
        { name = "FERRICSTORE_PROTECTED_MODE", value = "false" }
      ]

      mountPoints = [
        {
          sourceVolume  = "data"
          containerPath = "/data"
          readOnly      = false
        }
      ]

      linuxParameters = {
        initProcessEnabled = true
      }

      healthCheck = {
        command = [
          "CMD-SHELL",
          "bash -ec 'exec 3<>/dev/tcp/127.0.0.1/6381; printf \"GET /health/ready HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n\" >&3; IFS= read -r status <&3; [[ \"$status\" == *\" 200 \"* ]]'"
        ]
        interval    = 30
        timeout     = 5
        retries     = 5
        startPeriod = 120
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.this.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ferricstore"
        }
      }
    }
  ])
}

# -----------------------------------------------------------------------------
# Native and optional HTTP endpoints plus the single-task ECS service
# -----------------------------------------------------------------------------

resource "aws_lb" "this" {
  name                             = var.name_prefix
  internal                         = true
  load_balancer_type               = "network"
  subnets                          = aws_subnet.private[*].id
  enable_cross_zone_load_balancing = true
}

resource "aws_lb_target_group" "native" {
  name        = "${var.name_prefix}-native"
  port        = local.native_port
  protocol    = "TCP"
  target_type = "ip"
  vpc_id      = aws_vpc.this.id

  deregistration_delay = 120

  health_check {
    enabled             = true
    protocol            = "HTTP"
    port                = tostring(local.health_probe_port)
    path                = "/health/ready"
    matcher             = "200"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_target_group" "http" {
  count = var.http_enabled ? 1 : 0

  name               = "${var.name_prefix}-http"
  port               = local.http_target_port
  protocol           = "TCP"
  target_type        = "ip"
  vpc_id             = aws_vpc.this.id
  preserve_client_ip = true

  deregistration_delay = 120

  health_check {
    enabled             = true
    protocol            = "HTTP"
    port                = tostring(local.health_probe_port)
    path                = "/health/ready"
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
    type             = "forward"
    target_group_arn = aws_lb_target_group.native.arn
  }
}

resource "aws_lb_listener" "http" {
  count = var.http_enabled && var.http_tls_certificate_arn == null ? 1 : 0

  load_balancer_arn = aws_lb.this.arn
  port              = var.http_listener_port
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.http[0].arn
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
    type             = "forward"
    target_group_arn = aws_lb_target_group.http[0].arn
  }
}

resource "aws_ecs_service" "this" {
  name            = var.name_prefix
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.this.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.ec2.name
    weight            = 1
    base              = 1
  }

  enable_execute_command             = true
  health_check_grace_period_seconds  = 120
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  propagate_tags                     = "SERVICE"

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.task.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.native.arn
    container_name   = local.container_name
    container_port   = local.native_port
  }

  dynamic "load_balancer" {
    for_each = var.http_enabled ? [1] : []

    content {
      target_group_arn = aws_lb_target_group.http[0].arn
      container_name   = local.container_name
      container_port   = local.http_target_port
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.execution,
    aws_iam_role_policy.task,
    aws_iam_role_policy_attachment.ecs_instance,
    aws_ecs_cluster_capacity_providers.this,
    aws_lb_listener.native,
    aws_lb_listener.http,
    aws_lb_listener.https
  ]
}
