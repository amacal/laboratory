resource "aws_ecs_cluster" "cluster" {
  name               = "wikipedia"
  capacity_providers = ["FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 100
  }

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecr_repository" "repository" {
  name                 = "wikipedia"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }
}

resource "aws_ecs_task_definition" "task" {
  family                   = "wikipedia"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "1024"
  memory                   = "2048"
  task_role_arn            = aws_iam_role.ecs_role.arn
  execution_role_arn       = aws_iam_role.ecs_role.arn
  container_definitions    = <<DEFINITION
[
  {
    "name": "wikipedia",
    "image": "${aws_ecr_repository.repository.repository_url}:latest",
    "cpu": 1024,
    "memory": 2048,
    "networkMode": "awsvpc",
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-create-group": "true",
        "awslogs-region": "eu-west-1",
        "awslogs-group": "wikipedia",
        "awslogs-stream-prefix": "prefix"
      }
    }
  }
]
DEFINITION
}

resource "aws_iam_role" "ecs_role" {
  name               = "wikipedia-ecs-role"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      }
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "ecs_role_policy" {
  role       = aws_iam_role.ecs_role.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

resource "aws_ssm_parameter" "parameter_task" {
  type  = "String"
  name  = "/wikipedia/task_arn"
  value = aws_ecs_task_definition.task.arn
}

resource "aws_ssm_parameter" "parameter_cluster" {
  type  = "String"
  name  = "/wikipedia/cluster_arn"
  value = aws_ecs_cluster.cluster.arn
}
