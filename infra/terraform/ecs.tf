resource "aws_ecr_repository" "airflow" {
  name                 = "airflow-custom"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = true }
}

resource "aws_ecs_cluster" "airflow" {
  name = "airflow-cluster"
}

resource "aws_ecs_capacity_provider" "asg" {
  name = "${local.name}-cp"

  auto_scaling_group_provider {
    auto_scaling_group_arn         = aws_autoscaling_group.airflow.arn
    managed_termination_protection = "ENABLED"

    managed_scaling {
      status                    = "ENABLED"
      target_capacity           = 100
      minimum_scaling_step_size = 1
      maximum_scaling_step_size = 1
    }
  }
}

resource "aws_ecs_cluster_capacity_providers" "airflow" {
  cluster_name       = aws_ecs_cluster.airflow.name
  capacity_providers = [aws_ecs_capacity_provider.asg.name]

  default_capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.asg.name
    base              = 1
    weight            = 1
  }
}

resource "aws_ecs_task_definition" "airflow" {
  family             = "airflow-task"
  network_mode       = "bridge"
  execution_role_arn = aws_iam_role.ecs_task_execution.arn

  # No taskRoleArn: the containers inherit the EC2 instance role via IMDS.

  volume {
    name      = "dags"
    host_path = "/srv/airflow/dags"
  }
  volume {
    name      = "dbt"
    host_path = "/srv/airflow/dbt"
  }
  volume {
    name      = "logs"
    host_path = "/srv/airflow/logs"
  }
  volume {
    name      = "secrets"
    host_path = "/etc/airflow/.env"
  }
  volume {
    name      = "pgdata"
    host_path = "/srv/postgres-data"
  }

  container_definitions = jsonencode([
    {
      name      = "postgres"
      image     = "postgres:16"
      essential = true
      memory    = 1024
      cpu       = 256

      portMappings = [
        { containerPort = 5432, hostPort = 0, protocol = "tcp" }
      ]

      environment = [
        { name = "POSTGRES_USER", value = "airflow" },
        { name = "POSTGRES_DB", value = "airflow" },
      ]

      # POSTGRES_PASSWORD is sourced from the bind-mounted .env via the
      # entrypoint wrapper below.
      entryPoint = ["/bin/bash", "-lc"]
      command = [
        "set -a; . /run/secrets/airflow.env; set +a; exec docker-entrypoint.sh postgres"
      ]

      mountPoints = [
        { sourceVolume = "pgdata", containerPath = "/var/lib/postgresql/data" },
        { sourceVolume = "secrets", containerPath = "/run/secrets/airflow.env", readOnly = true },
      ]

      healthCheck = {
        command     = ["CMD-SHELL", "pg_isready -U airflow -d airflow || exit 1"]
        interval    = 15
        timeout     = 5
        retries     = 5
        startPeriod = 30
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ecs.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "postgres"
        }
      }
    },

    {
      name      = "airflow"
      image     = "${aws_ecr_repository.airflow.repository_url}:${var.image_tag}"
      essential = true
      memory    = 4096
      cpu       = 1024

      portMappings = [
        { containerPort = 8080, hostPort = 8080, protocol = "tcp" }
      ]

      links = ["postgres:postgres"]

      environment = [
        { name = "AIRFLOW__CORE__EXECUTOR", value = "LocalExecutor" },
        { name = "AIRFLOW__CORE__LOAD_EXAMPLES", value = "False" },
        { name = "AWS_DEFAULT_REGION", value = var.aws_region },

        { name = "AIRFLOW__LOGGING__REMOTE_LOGGING", value = "True" },
        {
          name  = "AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER",
          value = "cloudwatch://${aws_cloudwatch_log_group.airflow_tasks.arn}"
        },
        { name = "AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID", value = "" },
        { name = "AIRFLOW__LOGGING__LOGGING_LEVEL", value = "INFO" },

        { name = "ACME_RAW_BUCKET", value = local.raw_bucket },
        { name = "ACME_CURATED_BUCKET", value = local.curated_bucket },
        { name = "GLUE_JOB_NAME", value = var.glue_job_name },
      ]

      mountPoints = [
        { sourceVolume = "dags", containerPath = "/opt/airflow/dags", readOnly = true },
        { sourceVolume = "dbt", containerPath = "/opt/airflow/dbt", readOnly = true },
        { sourceVolume = "logs", containerPath = "/opt/airflow/logs", readOnly = false },
        { sourceVolume = "secrets", containerPath = "/run/secrets/airflow.env", readOnly = true },
      ]

      dependsOn = [
        { containerName = "postgres", condition = "HEALTHY" }
      ]

      healthCheck = {
        command     = ["CMD-SHELL", "curl -fsS http://localhost:8080/health || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 90
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ecs.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "airflow"
        }
      }
    }
  ])

  requires_compatibilities = ["EC2"]
}

resource "aws_ecs_service" "airflow" {
  name            = "airflow-svc"
  cluster         = aws_ecs_cluster.airflow.id
  task_definition = aws_ecs_task_definition.airflow.arn
  desired_count   = 1
  launch_type     = null # uses capacity provider strategy

  capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.asg.name
    base              = 1
    weight            = 1
  }

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  # Single host, single task — no placement constraints needed beyond default.
  ordered_placement_strategy {
    type  = "spread"
    field = "instanceId"
  }

  lifecycle {
    ignore_changes = [desired_count]
  }
}
