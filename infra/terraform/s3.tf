resource "aws_s3_bucket" "raw" {
  bucket = local.raw_bucket
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "curated" {
  bucket = local.curated_bucket
}

resource "aws_s3_bucket_versioning" "curated" {
  bucket = aws_s3_bucket.curated.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_public_access_block" "curated" {
  bucket                  = aws_s3_bucket.curated.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "glue_assets" {
  bucket = local.glue_assets_bucket
}

resource "aws_s3_bucket_public_access_block" "glue_assets" {
  bucket                  = aws_s3_bucket.glue_assets.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload the Glue script as part of `terraform apply`.
# Replace via CI later if you split deployment lifecycle.
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.glue_assets.id
  key    = "jobs/etl_stub.py"
  source = "${path.module}/../../glue/etl_stub.py"
  etag   = filemd5("${path.module}/../../glue/etl_stub.py")
}
