"""Bronze: load raw NAV files into a Delta table, one row per line, nothing parsed.

Every line is kept exactly as it is in the file (data rows, category headers, fund
house headers and blank lines), together with its line number, because in the AMFI
format a header line applies to the data rows below it. Parsing happens in Silver.

Run on Databricks (serverless) as a Python script task:

    bronze_nav.py --source-path s3://<bucket>/raw/nav/date=2026-11-11/ --run-id manual-1

Where the files come from (decision D1):
- s3://...    the official source. Read with boto3 on the driver, using a read-only
              IAM user key kept in the Databricks secret scope `mf_nav`. (The Unity
              Catalog IAM-role route is blocked by the AWS free plan's cross-region
              guardrail, so Spark cannot read s3:// paths natively here.)
- /Volumes/...  documented fallback only: read natively by Spark.

Rerunning the same file adds nothing: a file whose (nav_file_date, file_sha256) is
already in the table is skipped, so the job is idempotent.
"""

from __future__ import annotations

import argparse
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE_TABLE = "mf_nav.bronze.nav_raw_lines"
SECRET_SCOPE = "mf_nav"
SECRET_KEY_ID = "s3_reader_access_key_id"
SECRET_KEY_SECRET = "s3_reader_secret_access_key"  # noqa: S105 (a key name, not a secret)
DEFAULT_AWS_REGION = "ap-southeast-2"
FILES_SCHEMA = "path string, content binary"

BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
    source_file    STRING    NOT NULL COMMENT 'Full path of the raw file',
    file_sha256    STRING    NOT NULL COMMENT 'SHA-256 of the whole file, for idempotency',
    nav_file_date  DATE               COMMENT 'Load date from the date=YYYY-MM-DD folder',
    line_number    INT       NOT NULL COMMENT '1-based line position in the file',
    raw_line       STRING    NOT NULL COMMENT 'Line exactly as in the file, minus its line ending',
    ingested_at    TIMESTAMP NOT NULL COMMENT 'When this run wrote the row',
    run_id         STRING    NOT NULL COMMENT 'Airflow or manual run identifier'
)
USING DELTA
COMMENT 'Bronze: raw AMFI NAV file lines, append-only, unparsed'
"""


def split_s3_uri(uri: str) -> tuple[str, str]:
    """'s3://bucket/raw/nav/date=X/' -> ('bucket', 'raw/nav/date=X/')."""
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"not an s3:// URI: {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def read_s3_files(spark: SparkSession, source_path: str, s3) -> DataFrame:
    """Download every .txt object under an s3:// prefix with boto3 (on the driver).

    Returns the same shape as read_raw_files: one row per file, `path` and exact
    `content` bytes. Daily files are 1.5 MB and history chunks about 5 MB, so pulling
    them through the driver is fine. `s3` is a boto3 S3 client (a fake in tests).
    """
    bucket, prefix = split_s3_uri(source_path)
    rows = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".txt"):
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                rows.append((f"s3://{bucket}/{obj['Key']}", bytearray(body)))
    if not rows:
        raise FileNotFoundError(f"no .txt files under {source_path}")
    return spark.createDataFrame(rows, FILES_SCHEMA)


def s3_client_from_secrets(region: str = DEFAULT_AWS_REGION):
    """boto3 S3 client using the read-only key from the Databricks secret scope.

    Values are only passed to boto3, never printed (Databricks would show
    [REDACTED] anyway).
    """
    import boto3  # preinstalled on serverless environment v4
    from databricks.sdk.runtime import dbutils

    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY_ID),
        aws_secret_access_key=dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY_SECRET),
    )


def read_raw_files(spark: SparkSession, source_path: str) -> DataFrame:
    """Read each file under a Volume path as raw bytes (fallback route).

    binaryFile keeps the exact bytes, so the SHA-256 matches the one the downloader
    stored in S3 metadata, and we decide how to split lines ourselves.
    """
    return (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.txt")
        .option("recursiveFileLookup", "true")
        .load(source_path)
        .select("path", "content")
    )


def explode_lines(files: DataFrame, run_id: str) -> DataFrame:
    """Turn one row per file into one row per line, keeping exact file order.

    Splitting a whole file in one place (instead of letting Spark read lines in
    parallel) is what makes line_number reliable. Files here are 1.5 to 5 MB.
    """
    text = F.decode(F.col("content"), "UTF-8")
    folder_date = F.regexp_extract("path", r"date=(\d{4}-\d{2}-\d{2})", 1)
    # Step 1, still one row per file: hash, date and the array of lines. The raw
    # bytes are dropped here on purpose. If `content` (or the array's size) were
    # computed in the same select as posexplode, Spark would copy the whole 1.5 MB
    # file onto each of the ~18,000 exploded rows (about 27 GB) and run out of memory.
    per_file = files.select(
        F.col("path").alias("source_file"),
        F.sha2(F.col("content"), 256).alias("file_sha256"),
        # null (not an error) when the path has no date= folder; serverless runs ANSI mode
        F.when(folder_date != "", F.to_date(folder_date)).alias("nav_file_date"),
        F.split(text, "\r?\n", -1).alias("_parts"),  # -1 keeps empty strings (blank lines)
    ).withColumn("_n_parts", F.size("_parts"))
    # Step 2: one row per line. Only small columns ride along with each line.
    exploded = per_file.select(
        "source_file",
        "file_sha256",
        "nav_file_date",
        "_n_parts",
        F.posexplode("_parts").alias("_pos", "raw_line"),
    )
    # A file that ends with a line break produces one empty string after the last
    # line. That is not a line, so drop it; every other line (even blank) is kept.
    is_trailing_empty = (F.col("_pos") == F.col("_n_parts") - 1) & (F.col("raw_line") == "")
    return exploded.where(~is_trailing_empty).select(
        "source_file",
        "file_sha256",
        "nav_file_date",
        (F.col("_pos") + 1).cast("int").alias("line_number"),
        "raw_line",
        F.current_timestamp().alias("ingested_at"),
        F.lit(run_id).alias("run_id"),
    )


def only_new_files(lines: DataFrame, existing: DataFrame) -> DataFrame:
    """Drop lines of files already loaded: same load date and same content hash.

    Keyed on content, not on the path, so the same day's bytes landed in two places
    (the Volume and S3) are loaded once. A changed file for the same date has a new
    hash and is loaded again as a new version.
    """
    loaded = existing.select("nav_file_date", "file_sha256").distinct()
    return lines.join(loaded, on=["nav_file_date", "file_sha256"], how="left_anti")


ALLOWED_PREFIXES = ("s3://", "/Volumes/", "dbfs:/Volumes/")


def check_source_path(source_path: str) -> None:
    """Refuse paths Databricks cannot read, with a clear message.

    A real incident: Git Bash turned "/Volumes/..." into "C:/Program Files/Git/Volumes/...",
    and serverless only answered "An unexpected error occurred".
    """
    if not source_path.startswith(ALLOWED_PREFIXES):
        raise ValueError(
            f"source path must start with one of {ALLOWED_PREFIXES}, got {source_path!r}. "
            "On Git Bash, run with MSYS_NO_PATHCONV=1."
        )


def run(
    spark: SparkSession,
    source_path: str,
    run_id: str,
    table: str = BRONZE_TABLE,
    s3=None,
    aws_region: str = DEFAULT_AWS_REGION,
) -> int:
    """Load all raw files under source_path into Bronze. Returns rows appended."""
    check_source_path(source_path)
    spark.sql("CREATE SCHEMA IF NOT EXISTS mf_nav.bronze")
    spark.sql(BRONZE_DDL.replace(BRONZE_TABLE, table))
    if source_path.startswith("s3://"):
        files = read_s3_files(spark, source_path, s3 or s3_client_from_secrets(aws_region))
    else:
        files = read_raw_files(spark, source_path)
    lines = explode_lines(files, run_id)
    new_lines = only_new_files(lines, spark.table(table))
    # No df.cache() here: serverless rejects it on older client environments. Instead,
    # isEmpty() is a cheap check, and the row count comes from the Delta commit itself.
    if new_lines.isEmpty():
        print(f"bronze: nothing new under {source_path}; already loaded (run_id={run_id})")
        return 0
    new_lines.write.format("delta").mode("append").saveAsTable(table)
    last_commit = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
    appended = int(last_commit["operationMetrics"]["numOutputRows"])
    print(
        f"bronze: appended {appended} rows from {source_path} "
        f"(run_id={run_id}, table version {last_commit['version']})"
    )
    return appended


def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw NAV files into Bronze")
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--table", default=BRONZE_TABLE)
    parser.add_argument("--aws-region", default=DEFAULT_AWS_REGION)
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    run(spark, args.source_path, args.run_id, args.table, aws_region=args.aws_region)


if __name__ == "__main__":
    main()
