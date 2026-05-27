# ------------------------------------------------------------------
# Log groups
# ------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "ecs" {
  name              = "/ecs/airflow"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "airflow_tasks" {
  name              = "/airflow/tasks"
  retention_in_days = var.log_retention_days
}

# ------------------------------------------------------------------
# SNS topic for ops alerts (subscription optional)
# ------------------------------------------------------------------
resource "aws_sns_topic" "ops" {
  name = "airflow-ops"
}

resource "aws_sns_topic_subscription" "ops_email" {
  count     = var.ops_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.ops.arn
  protocol  = "email"
  endpoint  = var.ops_email
}

# ------------------------------------------------------------------
# Alarms
# ------------------------------------------------------------------
# 1) EC2 status check fail -> SNS notify (ASG health_check_type=EC2 handles
#    actual instance replacement automatically). We can't use the
#    arn:aws:automate:...:ec2:recover action here because that requires the
#    alarm to be pinned to a specific InstanceId dimension, which we don't
#    know at plan time (ASG owns the instance lifecycle).
resource "aws_cloudwatch_metric_alarm" "instance_status" {
  alarm_name          = "airflow-instance-status"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "StatusCheckFailed"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  alarm_description   = "Any Airflow EC2 instance is failing status checks"
  alarm_actions       = [aws_sns_topic.ops.arn]
}

# 2) High CPU (notify only)
resource "aws_cloudwatch_metric_alarm" "cpu_high" {
  alarm_name          = "airflow-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Average"
  threshold           = 85
  alarm_description   = "Airflow host CPU > 85% for 15m"
  alarm_actions       = [aws_sns_topic.ops.arn]
}

# 3) Low disk on /srv/postgres-data (CWAgent metric)
resource "aws_cloudwatch_metric_alarm" "disk_low" {
  alarm_name          = "airflow-disk-low"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "disk_used_percent"
  namespace           = "Airflow/Host"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "Postgres EBS > 80% used"
  alarm_actions       = [aws_sns_topic.ops.arn]
  dimensions = {
    path = "/srv/postgres-data"
  }
}

# 4) Glue job failure (EventBridge -> SNS, not a metric alarm)
resource "aws_cloudwatch_event_rule" "glue_failed" {
  name        = "glue-polars-etl-failed"
  description = "Notify on Glue ${var.glue_job_name} FAILED/TIMEOUT/STOPPED"
  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Job State Change"]
    detail = {
      jobName = [var.glue_job_name]
      state   = ["FAILED", "TIMEOUT", "STOPPED"]
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_failed_sns" {
  rule = aws_cloudwatch_event_rule.glue_failed.name
  arn  = aws_sns_topic.ops.arn
}

# EventBridge needs explicit SNS topic policy to publish.
data "aws_iam_policy_document" "sns_eventbridge_publish" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.ops.arn]
  }
}

resource "aws_sns_topic_policy" "ops" {
  arn    = aws_sns_topic.ops.arn
  policy = data.aws_iam_policy_document.sns_eventbridge_publish.json
}
