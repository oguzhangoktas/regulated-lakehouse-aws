"""Spark session construction for local runs.

Glue supplies its own session and carries the Iceberg runtime, so this is used by
local development and tests only. The catalog is named the same in both, so jobs
reference tables identically wherever they run.
"""
from pyspark.sql import SparkSession

# Matches the Iceberg version in the Glue 5.1 runtime (ADR-007). Iceberg ships as a
# Spark runtime jar rather than a Python package, so it is resolved at session start
# instead of being pinned in requirements.
ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"

# The Kafka source for Structured Streaming. Version tracks Spark 3.5 (ADR-007).
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6"

# The AWS bindings for the S3 + Glue Data Catalog variant. Versions track the Glue 5.1
# runtime; iceberg-aws-bundle carries the Glue catalog and S3 file-IO integration.
AWS_BUNDLE = "org.apache.iceberg:iceberg-aws-bundle:1.10.0"
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"


def local_session(
    app_name: str,
    warehouse: str = "data/warehouse",
    shuffle_partitions: int = 8,
    s3: bool = False,
) -> SparkSession:
    """Build a session with an Iceberg catalog.

    With s3=False the catalog is a local directory, used by tests and development. With
    s3=True the same tables are written to S3 under the Glue Data Catalog, the catalog
    the batch domain uses on Glue, so tables are queryable from Athena. Compute is
    local either way.

    The Spark default of 200 shuffle partitions is sized for a cluster. On a local
    sample it produces hundreds of near-empty tasks and scheduling dominates the work.
    """
    packages = ICEBERG_RUNTIME
    if s3:
        packages = f"{packages},{AWS_BUNDLE},{HADOOP_AWS}"

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.ui.showConsoleProgress", "false")
    )

    if s3:
        builder = (
            builder
            .config("spark.sql.catalog.lakehouse.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.lakehouse.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
        )
    else:
        builder = (
            builder
            .config("spark.sql.catalog.lakehouse.type", "hadoop")
            .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
        )

    return builder.getOrCreate()


def streaming_session(
    app_name: str,
    warehouse: str = "data/warehouse",
    checkpoint_root: str = "data/checkpoints",
    s3: bool = False,
) -> SparkSession:
    """A session for Structured Streaming: the Iceberg catalog plus the Kafka source,
    and checkpointing for exactly-once progress.

    Compute is always local. With s3=False the catalog is a local directory, used by
    tests and development. With s3=True the same tables are written to S3 under the
    Glue Data Catalog — the catalog the batch domain uses on Glue — so both domains
    share one catalog and are queryable from Athena. The streaming compute is not moved
    to Glue Streaming; that is a deliberate cost choice, documented in ADR-012.

    Separate from local_session so the batch jobs carry no streaming dependencies.
    Structured Streaming records its Kafka offsets in the checkpoint, so a restart
    resumes where it stopped rather than reprocessing or skipping.
    """
    packages = f"{ICEBERG_RUNTIME},{KAFKA_CONNECTOR}"
    if s3:
        packages = f"{packages},{AWS_BUNDLE},{HADOOP_AWS}"
        # Checkpoints must share the storage scheme of the tables; on S3 the source
        # rejects a local file: checkpoint path.
        if checkpoint_root.startswith("data/"):
            checkpoint_root = "s3://oglh-artifacts-915909866528/txn-checkpoints"

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.streaming.checkpointLocation", checkpoint_root)
        .config("spark.sql.shuffle.partitions", 8)
        .config("spark.ui.showConsoleProgress", "false")
    )

    if s3:
        # Glue Data Catalog for metadata, S3 for storage. The database location_uri set
        # in Terraform routes each layer to its bucket and the txn_monitoring/ prefix.
        # S3FileIO handles the Iceberg tables; the s3a filesystem handles Structured
        # Streaming checkpoints, which go through Hadoop rather than Iceberg.
        builder = (
            builder
            .config("spark.sql.catalog.lakehouse.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.lakehouse.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        )
    else:
        builder = (
            builder
            .config("spark.sql.catalog.lakehouse.type", "hadoop")
            .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
        )

    return builder.getOrCreate()
