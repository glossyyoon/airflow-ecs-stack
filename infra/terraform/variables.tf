variable "aws_region" {
  type    = string
  default = "ap-northeast-2"
}

variable "project" {
  type        = string
  description = "Name prefix for resources"
  default     = "airflow-ecs"
}

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

variable "public_subnet_cidr" {
  type    = string
  default = "10.0.1.0/24"
}

variable "az" {
  type        = string
  description = "Single availability zone for the stack"
  default     = "ap-northeast-2a"
}

variable "ssh_admin_cidrs" {
  type        = list(string)
  description = "CIDRs allowed to SSH the EC2 host (use SSM as the primary access path)"
  default     = ["0.0.0.0/32"] # deliberately unreachable until you set it
}

variable "ui_allowed_cidrs" {
  type        = list(string)
  description = "CIDRs allowed to reach Airflow UI on :8080"
  default     = ["0.0.0.0/32"]
}

variable "key_name" {
  type        = string
  description = "Existing EC2 keypair name. Empty = no keypair (SSM only)."
  default     = ""
}

variable "instance_type" {
  type    = string
  default = "t3.large"
}

variable "root_volume_gb" {
  type    = number
  default = 30
}

variable "data_volume_gb" {
  type    = number
  default = 20
}

variable "repo_url" {
  type        = string
  description = "Public/Private git URL of this repo (HTTPS) that the EC2 host will pull DAGs/dbt from"
}

variable "image_tag" {
  type        = string
  description = "ECR tag for the Airflow custom image"
  default     = "3.2.1-py3.11"
}

variable "raw_bucket_name" {
  type        = string
  description = "Override S3 raw bucket name. Empty = auto: <project>-raw-<account_id>"
  default     = ""
}

variable "curated_bucket_name" {
  type        = string
  description = "Override S3 curated bucket name. Empty = auto: <project>-curated-<account_id>"
  default     = ""
}

variable "glue_assets_bucket_name" {
  type        = string
  description = "Override S3 glue-assets bucket name. Empty = auto: <project>-glue-assets-<account_id>"
  default     = ""
}

variable "glue_job_name" {
  type    = string
  default = "polars-etl-prd"
}

variable "polars_version" {
  type    = string
  default = "1.18.0"
}

variable "pyarrow_version" {
  type    = string
  default = "18.1.0"
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "ops_email" {
  type        = string
  description = "Optional email to subscribe to the airflow-ops SNS topic. Empty = no subscription."
  default     = ""
}
