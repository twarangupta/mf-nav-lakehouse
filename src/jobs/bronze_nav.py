"""Bronze: load raw NAV files into a Delta table, one row per line, nothing parsed.

Every line is kept exactly as it is in the file (data rows, category headers, fund
house headers and blank lines), together with its line number, because in the AMFI
format a header line applies to the data rows below it. Parsing happens in Silver.

Run on Databricks (serverless) as a Python script task:

    bronze_nav.py --source-path s3://<bucket>/raw/nav/date=2026-11-11/ --run-id manual-1

Rerunning the same file adds nothing: a file whose (source_file, file_sha256) is
already in the table is skipped, so the job is idempotent.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE_TABLE = "mf_nav.bronze.nav_raw_lines"

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


def read_raw_files(spark: SparkSession, source_path: str) -> DataFrame:
    """Read each file as raw bytes: one row per file with `path` and `content`.

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
    """Drop lines of files that are already loaded (same path and same content hash)."""
    loaded = existing.select("source_file", "file_sha256").distinct()
    return lines.join(loaded, on=["source_file", "file_sha256"], how="left_anti")


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


def run(spark: SparkSession, source_path: str, run_id: str, table: str = BRONZE_TABLE) -> int:
    """Load all raw files under source_path into Bronze. Returns rows appended."""
    check_source_path(source_path)
    spark.sql("CREATE SCHEMA IF NOT EXISTS mf_nav.bronze")
    spark.sql(BRONZE_DDL.replace(BRONZE_TABLE, table))
    lines = explode_lines(read_raw_files(spark, source_path), run_id)
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
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    run(spark, args.source_path, args.run_id, args.table)


if __name__ == "__main__":
    main()
