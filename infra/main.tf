data "aws_caller_identity" "current" {}

locals {
  # Bucket names are globally unique across AWS. The account id is read at plan
  # time rather than hardcoded so the configuration is portable across accounts.
  suffix = data.aws_caller_identity.current.account_id

  # One bucket per layer rather than prefixes in a single bucket: each layer
  # carries a different access policy, retention and blast radius.
  layers = ["bronze", "silver", "gold", "quarantine"]
}

resource "aws_s3_bucket" "layer" {
  for_each = toset(local.layers)

  bucket = "${var.project}-${each.key}-${local.suffix}"
}

# Retains overwritten and deleted objects. Regulatory reporting needs the state
# as of a past date to remain reproducible.
resource "aws_s3_bucket_versioning" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Interrupted multipart uploads leave billable parts that do not appear in the
# bucket listing. Discard them instead of paying for them indefinitely.
resource "aws_s3_bucket_lifecycle_configuration" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  # Reloading a snapshot_date replaces the partition (ADR-002). With versioning on,
  # each replacement leaves the previous object behind as a noncurrent version:
  # invisible to a bucket listing, still billed. Versioning here is an operational
  # undo, not the audit trail - point-in-time reproduction comes from the
  # snapshot_date partitions and Delta history. A 30 day window covers recovery
  # from a bad load without retaining copies indefinitely.
  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}
