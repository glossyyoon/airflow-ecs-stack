# ------------------------------------------------------------------
# 1) EC2 host role + instance profile
# ------------------------------------------------------------------
data "aws_iam_policy_document" "ec2_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2_airflow_host" {
  name               = "ec2-airflow-host-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_trust.json
}

resource "aws_iam_role_policy_attachment" "ec2_ssm" {
  role       = aws_iam_role.ec2_airflow_host.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "ec2_ecs" {
  role       = aws_iam_role.ec2_airflow_host.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

data "aws_iam_policy_document" "airflow_runtime" {
  statement {
    sid    = "GlueInvoke"
    effect = "Allow"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:GetJob",
      "glue:BatchStopJobRun",
    ]
    resources = ["arn:aws:glue:${var.aws_region}:${local.account_id}:job/${var.glue_job_name}"]
  }

  statement {
    sid       = "PassGlueRoleToGlue"
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.glue_polars_job.arn]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["glue.amazonaws.com"]
    }
  }

  statement {
    sid       = "S3SensorList"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.raw.arn, aws_s3_bucket.curated.arn]
  }

  statement {
    sid     = "S3SensorObjects"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.raw.arn}/*",
      "${aws_s3_bucket.curated.arn}/*",
    ]
  }

  statement {
    sid    = "AirflowLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
      "logs:CreateLogGroup",
    ]
    resources = [
      "arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/airflow/*",
      "arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/airflow/*:*",
    ]
  }

  # Needed at boot for `aws ec2 attach-volume` in user_data.
  statement {
    sid       = "AttachDataVolume"
    effect    = "Allow"
    actions   = ["ec2:AttachVolume", "ec2:DescribeVolumes"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "airflow_runtime" {
  name   = "airflow-runtime"
  role   = aws_iam_role.ec2_airflow_host.id
  policy = data.aws_iam_policy_document.airflow_runtime.json
}

resource "aws_iam_instance_profile" "ec2_airflow_host" {
  name = "ec2-airflow-host-profile"
  role = aws_iam_role.ec2_airflow_host.name
}

# ------------------------------------------------------------------
# 2) ECS task execution role
# ------------------------------------------------------------------
data "aws_iam_policy_document" "ecs_tasks_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_task_execution" {
  name               = "ecs-task-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_trust.json
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ------------------------------------------------------------------
# 3) Glue Polars job role
# ------------------------------------------------------------------
data "aws_iam_policy_document" "glue_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_polars_job" {
  name               = "glue-polars-job-role"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_polars_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_polars_data" {
  statement {
    sid       = "ReadRaw"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]
  }

  statement {
    sid    = "WriteCurated"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [aws_s3_bucket.curated.arn, "${aws_s3_bucket.curated.arn}/*"]
  }

  statement {
    sid       = "GlueAssetsBucket"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.glue_assets.arn, "${aws_s3_bucket.glue_assets.arn}/*"]
  }
}

resource "aws_iam_role_policy" "glue_polars_data" {
  name   = "glue-polars-data-access"
  role   = aws_iam_role.glue_polars_job.id
  policy = data.aws_iam_policy_document.glue_polars_data.json
}
