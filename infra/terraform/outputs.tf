output "ecr_repository_url" {
  value = aws_ecr_repository.airflow.repository_url
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.airflow.name
}

output "asg_name" {
  value = aws_autoscaling_group.airflow.name
}

output "raw_bucket" {
  value = aws_s3_bucket.raw.id
}

output "curated_bucket" {
  value = aws_s3_bucket.curated.id
}

output "glue_assets_bucket" {
  value = aws_s3_bucket.glue_assets.id
}

output "glue_job_name" {
  value = aws_glue_job.smoke.name
}

output "sns_ops_topic_arn" {
  value = aws_sns_topic.ops.arn
}
