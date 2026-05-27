resource "aws_glue_job" "smoke" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "3.0"
  max_capacity      = 0.0625 # smallest Python Shell footprint
  timeout           = 10
  max_retries       = 0
  worker_type       = null
  number_of_workers = null

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.glue_assets.id}/${aws_s3_object.glue_script.key}"
  }

  default_arguments = {
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_assets.id}/tmp/"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}
