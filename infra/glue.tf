# Job code and Glue's scratch space. Not a lakehouse layer, so it sits outside the
# per-layer buckets and their retention.
resource "aws_s3_bucket" "artifacts" {
  bucket = "${var.project}-artifacts-${local.suffix}"
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  dynamic "rule" {
    for_each = ["temp/", "athena-results/"]

    content {
      id     = "expire-${trimsuffix(rule.value, "/")}"
      status = "Enabled"

      filter {
        prefix = rule.value
      }

      expiration {
        days = 7
      }
    }
  }

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# An Iceberg catalog resolves tables under a single warehouse, which would place every
# layer in one bucket. A Glue database carries its own location, so one catalog can
# span the per-layer buckets (ADR-006) with the layer named in the database.
locals {
  databases = {
    silver_credit_risk     = "silver"
    gold_credit_risk       = "gold"
    quarantine_credit_risk = "quarantine"
  }

  # The transaction-monitoring domain shares the same per-layer buckets, separated by
  # a txn_monitoring/ prefix. It includes bronze because its streaming ingestion lands
  # raw transactions in the lakehouse, unlike credit_risk whose bronze is the external
  # snapshot store.
  txn_databases = {
    bronze_txn_monitoring     = "bronze"
    silver_txn_monitoring     = "silver"
    gold_txn_monitoring       = "gold"
    quarantine_txn_monitoring = "quarantine"
  }
}

resource "aws_glue_catalog_database" "domain" {
  for_each = local.databases

  name         = each.key
  location_uri = "s3://${aws_s3_bucket.layer[each.value].id}/credit_risk/"
}

resource "aws_glue_catalog_database" "txn_domain" {
  for_each = local.txn_databases

  name         = each.key
  location_uri = "s3://${aws_s3_bucket.layer[each.value].id}/txn_monitoring/"
}

resource "aws_iam_role" "glue_job" {
  name = "${var.project}-glue-job"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

# Bronze is the source extract and is never written by a job (ADR-001): read only.
data "aws_iam_policy_document" "glue_job" {
  statement {
    sid     = "ReadBronze"
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.layer["bronze"].arn,
      "${aws_s3_bucket.layer["bronze"].arn}/*",
    ]
  }

  statement {
    sid = "WriteDerivedLayers"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:AbortMultipartUpload",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts",
    ]
    resources = flatten([
      for layer in ["silver", "gold", "quarantine"] : [
        aws_s3_bucket.layer[layer].arn,
        "${aws_s3_bucket.layer[layer].arn}/*",
      ]
    ])
  }

  statement {
    sid     = "JobCodeAndScratch"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.artifacts.arn,
      "${aws_s3_bucket.artifacts.arn}/*",
    ]
  }

  # Iceberg records table metadata in the Glue Data Catalog: the job creates and
  # updates tables, so it needs write access to the databases it owns.
  statement {
    sid = "IcebergCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
      "glue:UpdatePartition",
      "glue:CreatePartition",
    ]
    resources = concat(
      ["arn:aws:glue:${var.region}:${local.suffix}:catalog"],
      [for db in aws_glue_catalog_database.domain : db.arn],
      [for db in aws_glue_catalog_database.domain :
      "arn:aws:glue:${var.region}:${local.suffix}:table/${db.name}/*"],
      [for db in aws_glue_catalog_database.txn_domain : db.arn],
      [for db in aws_glue_catalog_database.txn_domain :
      "arn:aws:glue:${var.region}:${local.suffix}:table/${db.name}/*"],
    )
  }

  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.region}:${local.suffix}:log-group:/aws-glue/*"]
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "${var.project}-glue-job"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job.json
}

locals {
  glue_conf = join(" --conf ", [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.lakehouse.warehouse=s3://${aws_s3_bucket.artifacts.id}/warehouse/",
  ])

  glue_defaults = {
    "--datalake-formats" = "iceberg"

    # Glue's Python environment does not carry PyYAML; the contract loader needs it.
    "--additional-python-modules" = "pyyaml==6.0.2"

    "--extra-py-files"                   = "s3://${aws_s3_bucket.artifacts.id}/code/regulated_lakehouse-${var.wheel_version}-py3-none-any.whl"
    "--TempDir"                          = "s3://${aws_s3_bucket.artifacts.id}/temp/"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}

resource "aws_glue_job" "silver_exposure" {
  name              = "${var.project}-silver-exposure"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 20

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.id}/code/silver_exposure.py"
  }

  # One snapshot date per run, so a backfill is a run per date and a failed date is
  # rerun on its own.
  execution_property {
    max_concurrent_runs = 4
  }

  default_arguments = merge(local.glue_defaults, {
    "--conf"             = local.glue_conf
    "--bronze_root"      = "s3://${aws_s3_bucket.layer["bronze"].id}"
    "--table"            = "lakehouse.silver_credit_risk.exposure"
    "--quarantine_table" = "lakehouse.quarantine_credit_risk.exposure"
  })
}

# Athena writes results and metadata to S3. They are derived and re-runnable, so they
# live in artifacts under a lifecycle rule rather than in a lakehouse layer.
resource "aws_athena_workgroup" "main" {
  name = var.project

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.artifacts.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

resource "aws_glue_job" "gold_engine_input" {
  name              = "${var.project}-gold-engine-input"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 20

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.id}/code/gold_engine_input.py"
  }

  execution_property {
    max_concurrent_runs = 4
  }

  default_arguments = merge(local.glue_defaults, {
    "--conf"             = local.glue_conf
    "--silver_table"     = "lakehouse.silver_credit_risk.exposure"
    "--gold_table"       = "lakehouse.gold_credit_risk.engine_input"
    "--quarantine_table" = "lakehouse.quarantine_credit_risk.engine_input"
  })
}

resource "aws_glue_job" "rwa_output" {
  name              = "${var.project}-rwa-output"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 20

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.id}/code/rwa_output.py"
  }

  execution_property {
    max_concurrent_runs = 4
  }

  default_arguments = merge(local.glue_defaults, {
    "--conf"               = local.glue_conf
    "--engine_input_table" = "lakehouse.gold_credit_risk.engine_input"
    "--output_table"       = "lakehouse.gold_credit_risk.rwa_output"
    "--quarantine_table"   = "lakehouse.quarantine_credit_risk.rwa_output"
  })
}

# Airflow runs locally and triggers Glue. It gets its own credentials scoped to
# starting and watching jobs, nothing else. Job execution uses the Glue role above;
# this identity only orchestrates.
resource "aws_iam_user" "airflow" {
  name = "${var.project}-airflow"
}

resource "aws_iam_access_key" "airflow" {
  user = aws_iam_user.airflow.name
}

data "aws_iam_policy_document" "airflow" {
  statement {
    sid = "TriggerAndWatchGlue"
    actions = [
      "glue:StartJobRun",
      "glue:GetJob",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${local.suffix}:job/${var.project}-*",
    ]
  }
}

resource "aws_iam_user_policy" "airflow" {
  name   = "${var.project}-airflow"
  user   = aws_iam_user.airflow.name
  policy = data.aws_iam_policy_document.airflow.json
}
