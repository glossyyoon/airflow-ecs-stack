resource "aws_glue_job" "polars_etl" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue_polars_job.arn
  glue_version      = "5.0"
  max_capacity      = 1.0
  timeout           = 60
  max_retries       = 0
  worker_type       = null
  number_of_workers = null

  command {
    name            = "pythonshell"
    python_version  = "3.11"
    script_location = "s3://${aws_s3_bucket.glue_assets.id}/${aws_s3_object.glue_script.key}"
  }

  default_arguments = {
    "--additional-python-modules"        = "polars==${var.polars_version},pyarrow==${var.pyarrow_version}"
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_assets.id}/tmp/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}
