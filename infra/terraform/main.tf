terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }

  # For production, configure an S3 backend with a state lock table.
  # Left as local state intentionally so `terraform init` works out of the box.
  # backend "s3" {
  #   bucket         = "<state-bucket>"
  #   key            = "airflow-ecs/terraform.tfstate"
  #   region         = "ap-northeast-2"
  #   dynamodb_table = "<lock-table>"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  name       = var.project

  raw_bucket         = var.raw_bucket_name != "" ? var.raw_bucket_name : "${var.project}-raw-${local.account_id}"
  curated_bucket     = var.curated_bucket_name != "" ? var.curated_bucket_name : "${var.project}-curated-${local.account_id}"
  glue_assets_bucket = var.glue_assets_bucket_name != "" ? var.glue_assets_bucket_name : "${var.project}-glue-assets-${local.account_id}"
}
