"""Silver: parse Bronze raw lines into clean, typed, deduplicated NAV rows.

Steps, for every Bronze file not processed yet:
1. Classify each line: column header, blank, category header, fund house header,
   data row.
2. Carry the nearest category and fund house header down onto each data row (a
   "last non-null value" window ordered by line_number, i.e. a forward fill).
3. Parse the 8 fields by the file's own layout (daily NAVAll.txt and the history
   report put the columns in a different order), cast types, standardise text,
   and derive plan (Direct/Regular) and option (Growth/IDCW/Bonus).
4. Data quality, two levels:
   - hard rules: the row goes to silver.nav_quarantine with a reason;
   - soft rule: big day-over-day moves on Growth plans go to silver.dq_warnings,
     the row is kept.
5. The gate: per file, if quarantined rows (excluding known, expected cases) are
   above the threshold (default 1%), or the column header is not a known layout,
   the file is rejected: none of its rows reach silver.nav and the job fails at
   the end. Below the threshold, good rows load and the run reports a warning.
6. MERGE good rows into silver.nav on (scheme_code, nav_date), so reruns never
   double count; refresh silver.dim_scheme from the latest daily file.

Run on Databricks serverless:

    silver_nav.py --run-id manual-1 [--max-bad-ratio 0.01]
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

BRONZE_TABLE = "mf_nav.bronze.nav_raw_lines"
SILVER_SCHEMA = "mf_nav.silver"
NAV_TABLE = f"{SILVER_SCHEMA}.nav"
QUARANTINE_TABLE = f"{SILVER_SCHEMA}.nav_quarantine"
WARNINGS_TABLE = f"{SILVER_SCHEMA}.dq_warnings"
LOAD_LOG_TABLE = f"{SILVER_SCHEMA}.load_log"
DIM_SCHEME_TABLE = f"{SILVER_SCHEMA}.dim_scheme"
STAGING_TABLE = f"{SILVER_SCHEMA}._staging_checked_rows"

DEFAULT_MAX_BAD_RATIO = 0.01  # gate: more than 1% bad rows in a file fails it
DEFAULT_MOVE_THRESHOLD = 0.20  # soft rule: a Growth NAV moving more than 20% in one step

# The two column layouts AMFI uses. Field positions (0-based) after splitting on ";".
LAYOUTS = {
    "daily": {
        "header": "Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;"
        "Scheme Name;Plan;Option;Net Asset Value;Date",
        "fields": {"code": 0, "isin_1": 1, "isin_2": 2, "name": 3, "plan": 4, "option": 5,
                   "nav": 6, "date": 7},
    },
    "history": {
        "header": "Scheme Code;NAV Name;Plan;Option;ISIN Div Payout/ISIN Growth;"
        "ISIN Div Reinvestment;Net Asset Value;Date",
        "fields": {"code": 0, "name": 1, "plan": 2, "option": 3, "isin_1": 4, "isin_2": 5,
                   "nav": 6, "date": 7},
    },
}  # fmt: skip

# Hard-rule reasons. Rows with a reason in KNOWN_REASONS are expected (documented)
# exclusions: they are quarantined for the record but do not count toward the gate.
KNOWN_REASONS = {"zero_nav_segregated_portfolio"}

CATEGORY_RE = r"^(Open Ended|Close Ended|Interval Fund) Schemes\s*\(\s*(.*?)\s*\)\s*$"


# --------------------------------------------------------------------------- parsing


def classify_lines(lines: DataFrame) -> DataFrame:
    """Add line_kind: col_header, blank, category, fund_house or data."""
    raw = F.col("raw_line")
    text = F.trim(raw)
    return lines.withColumn(
        "line_kind",
        F.when(F.col("line_number") == 1, "col_header")
        .when(text == "", "blank")
        .when(raw.contains(";"), "data")
        .when(text.rlike(r"^(Open Ended|Close Ended|Interval Fund) Schemes"), "category")
        .otherwise("fund_house"),
    )


def attach_headers(classified: DataFrame) -> DataFrame:
    """Forward fill: give each line the latest category and fund house above it.

    SQL equivalent: LAST_VALUE(x IGNORE NULLS) OVER (PARTITION BY source_file
    ORDER BY line_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW).
    The column header (line 1) is copied to every line of its file the same way.
    """
    w = (
        Window.partitionBy("source_file")
        .orderBy("line_number")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    kind = F.col("line_kind")
    text = F.trim("raw_line")
    return (
        classified.withColumn("header_category_raw", F.when(kind == "category", text))
        .withColumn("header_fund_house", F.when(kind == "fund_house", text))
        .withColumn("file_header", F.when(kind == "col_header", text))
        .withColumn("header_category_raw", F.last("header_category_raw", True).over(w))
        .withColumn("header_fund_house", F.last("header_fund_house", True).over(w))
        .withColumn("file_header", F.last("file_header", True).over(w))
    )


def _layout_column() -> F.Column:
    expr = F.lit(None).cast("string")
    for name, layout in LAYOUTS.items():
        expr = F.when(F.col("file_header") == layout["header"], name).otherwise(expr)
    return expr


def _field(name: str) -> F.Column:
    """Pick a field by name, using the position for this row's layout."""
    parts = F.col("_f")
    expr = F.lit(None).cast("string")
    for layout_name, layout in LAYOUTS.items():
        expr = F.when(
            F.col("layout") == layout_name, F.trim(parts[layout["fields"][name]])
        ).otherwise(expr)
    return expr


def clean_text(col: F.Column) -> F.Column:
    """Trim and collapse repeated spaces; empty or '-' becomes null."""
    collapsed = F.regexp_replace(F.trim(col), r"\s+", " ")
    return F.when((collapsed == "") | (collapsed == "-"), None).otherwise(collapsed)


def _plan_of(text: F.Column) -> F.Column:
    text = F.lower(text)
    return F.when(text.contains("direct"), "Direct").when(text.contains("regular"), "Regular")


def _option_of(text: F.Column) -> F.Column:
    """IDCW words are checked before "growth" because many fund names contain the word
    Growth ("Growth Opportunities Fund - IDCW"); "dividend yield" is a fund style, not
    a payout option, so it is removed first ("Dividend Yield Fund - Growth")."""
    text = F.regexp_replace(F.lower(text), "dividend yield", "")
    idcw = r"idcw|dividend|income distribution|payout|re-?invest|dcw"
    return (
        F.when(text.rlike(idcw), "IDCW")
        .when(text.contains("bonus"), "Bonus")
        .when(text.rlike(r"growth|cumulative"), "Growth")
    )


def derive_plan(plan_raw: F.Column, name: F.Column) -> F.Column:
    """Direct / Regular / Unknown: the Plan column if it names a plan, else the name."""
    return F.coalesce(_plan_of(plan_raw), _plan_of(name), F.lit("Unknown"))


def derive_option(option_raw: F.Column, name: F.Column) -> F.Column:
    """Growth / IDCW / Bonus / Unknown: the Option column if it names an option (some
    rows hold "Direct" there), else the scheme name."""
    return F.coalesce(_option_of(option_raw), _option_of(name), F.lit("Unknown"))


def parse_rows(with_headers: DataFrame) -> DataFrame:
    """Data lines only, with typed and standardised columns."""
    cat = F.col("header_category_raw")
    data = (
        with_headers.where(F.col("line_kind") == "data")
        .withColumn("layout", _layout_column())
        .withColumn("_f", F.split("raw_line", ";", -1))
        .withColumn("_n_fields", F.size("_f"))
    )
    plan_raw = clean_text(_field("plan"))
    option_raw = clean_text(_field("option"))
    name = clean_text(_field("name"))
    return data.select(
        "source_file",
        "file_sha256",
        "nav_file_date",
        "line_number",
        "raw_line",
        "layout",
        "_n_fields",
        F.when(F.col("source_file").contains("/nav_history/"), "history")
        .otherwise("daily")
        .alias("source_kind"),
        F.expr("try_cast(trim(_f[0]) AS INT)").alias("scheme_code"),
        name.alias("scheme_name"),
        F.expr("to_date(try_to_timestamp(trim(_f[7]), 'dd-MMM-yyyy'))").alias("nav_date"),
        F.trim(F.col("_f")[6]).alias("nav_raw"),
        F.expr("try_cast(trim(_f[6]) AS DECIMAL(20,6))").alias("nav"),
        clean_text(_field("isin_1")).alias("isin_growth_or_payout"),
        clean_text(_field("isin_2")).alias("isin_reinvestment"),
        plan_raw.alias("plan_raw"),
        option_raw.alias("option_raw"),
        derive_plan(plan_raw, name).alias("plan"),
        derive_option(option_raw, name).alias("option"),
        F.regexp_extract(cat, CATEGORY_RE, 1).alias("scheme_type"),
        F.regexp_extract(cat, CATEGORY_RE, 2).alias("category"),
        F.col("header_fund_house").alias("fund_house"),
    )


# ---------------------------------------------------------------------- data quality


def quality_reason(parsed: DataFrame) -> DataFrame:
    """Add dq_reason (null = good row) and dq_counts_toward_gate. First failing rule wins."""
    is_segregated = F.lower(F.coalesce(F.col("scheme_name"), F.lit(""))).contains("segregat")
    reason = (
        F.when(F.col("layout").isNull(), "unknown_column_layout")
        .when(F.col("_n_fields") != 8, "bad_field_count")
        .when(F.col("scheme_code").isNull(), "bad_scheme_code")
        .when(F.col("nav_date").isNull(), "bad_nav_date")
        .when(F.col("nav").isNull(), "nav_not_a_number")
        .when((F.col("nav") <= 0) & is_segregated, "zero_nav_segregated_portfolio")
        .when(F.col("nav") <= 0, "nav_not_positive")
        .when(F.col("category") == "", "missing_category_header")
        .when(F.col("fund_house").isNull(), "missing_fund_house_header")
    )
    # A (scheme_code, nav_date) appearing twice in one file with different NAVs is
    # ambiguous; both copies are quarantined. Exact repeats are harmless (deduped later).
    w = Window.partitionBy("source_file", "scheme_code", "nav_date")
    conflicting = F.size(F.collect_set("nav").over(w)) > 1
    with_reason = parsed.withColumn(
        "dq_reason", F.coalesce(reason, F.when(conflicting, "duplicate_conflicting_nav"))
    )
    return with_reason.withColumn(
        "dq_counts_toward_gate",
        F.col("dq_reason").isNotNull() & ~F.col("dq_reason").isin(*KNOWN_REASONS),
    )


def gate_by_file(checked: DataFrame, max_bad_ratio: float) -> DataFrame:
    """One row per file: counts, bad ratio, and whether the file passes the gate."""
    summary = checked.groupBy("source_file", "file_sha256", "nav_file_date", "source_kind").agg(
        F.count("*").alias("data_rows"),
        F.count_if(F.col("dq_reason").isNull()).alias("good_rows"),
        F.count_if(F.col("dq_counts_toward_gate")).alias("bad_rows"),
        F.count_if(F.col("dq_reason").isin(*KNOWN_REASONS)).alias("known_excluded_rows"),
        F.count_if(F.col("layout").isNull()).alias("unknown_layout_rows"),
    )
    ratio = F.col("bad_rows") / F.col("data_rows")
    return (
        summary.withColumn("bad_ratio", F.round(ratio, 6))
        .withColumn(
            "status",
            F.when(F.col("unknown_layout_rows") > 0, "rejected_schema")
            .when(ratio > max_bad_ratio, "rejected_gate")
            .when(F.col("bad_rows") > 0, "loaded_with_warning")
            .otherwise("loaded"),
        )
        .drop("unknown_layout_rows")
    )


def dedupe_latest(good: DataFrame) -> DataFrame:
    """One row per (scheme_code, nav_date): prefer the newest file, then the last line."""
    w = Window.partitionBy("scheme_code", "nav_date").orderBy(
        F.col("nav_file_date").desc_nulls_last(), F.col("line_number").desc()
    )
    return good.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def big_moves(nav: DataFrame, threshold: float) -> DataFrame:
    """Soft rule: Growth NAV changing more than `threshold` since its previous real NAV date."""
    w = Window.partitionBy("scheme_code").orderBy("nav_date")
    return (
        nav.where(F.col("option") == "Growth")
        .withColumn("prev_nav_date", F.lag("nav_date").over(w))
        .withColumn("prev_nav", F.lag("nav").over(w))
        .where(F.col("prev_nav") > 0)
        .withColumn(
            "pct_change", F.round((F.col("nav") - F.col("prev_nav")) / F.col("prev_nav"), 6)
        )
        .where(F.abs("pct_change") > threshold)
        .select(
            "scheme_code",
            "nav_date",
            "prev_nav_date",
            "prev_nav",
            "nav",
            "pct_change",
            F.lit(f"growth_move_gt_{int(threshold * 100)}pct").alias("rule"),
        )
    )


# ------------------------------------------------------------------------ the job


NAV_DDL = f"""
CREATE TABLE IF NOT EXISTS {NAV_TABLE} (
  scheme_code INT NOT NULL, nav_date DATE NOT NULL, nav DECIMAL(20,6) NOT NULL,
  scheme_name STRING, plan STRING, option STRING, plan_raw STRING, option_raw STRING,
  isin_growth_or_payout STRING, isin_reinvestment STRING,
  scheme_type STRING, category STRING, fund_house STRING,
  source_kind STRING, source_file STRING, file_sha256 STRING, line_number INT,
  silver_loaded_at TIMESTAMP, run_id STRING)
USING DELTA COMMENT 'Silver: one clean NAV per scheme per day (key scheme_code, nav_date)'
"""
QUARANTINE_DDL = f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
  source_file STRING, file_sha256 STRING, nav_file_date DATE, line_number INT,
  raw_line STRING, scheme_code INT, nav_date DATE, nav_raw STRING, scheme_name STRING,
  dq_reason STRING, dq_counts_toward_gate BOOLEAN, quarantined_at TIMESTAMP, run_id STRING)
USING DELTA COMMENT 'Silver: rows that failed a hard data-quality rule, with the reason'
"""
WARNINGS_DDL = f"""
CREATE TABLE IF NOT EXISTS {WARNINGS_TABLE} (
  scheme_code INT, nav_date DATE, prev_nav_date DATE, prev_nav DECIMAL(20,6),
  nav DECIMAL(20,6), pct_change DOUBLE, rule STRING, detected_at TIMESTAMP, run_id STRING)
USING DELTA COMMENT 'Silver: soft-rule warnings; rows stay in silver.nav'
"""
LOAD_LOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {LOAD_LOG_TABLE} (
  source_file STRING, file_sha256 STRING, nav_file_date DATE, source_kind STRING,
  data_rows BIGINT, good_rows BIGINT, bad_rows BIGINT, known_excluded_rows BIGINT,
  bad_ratio DOUBLE, status STRING, processed_at TIMESTAMP, run_id STRING)
USING DELTA COMMENT 'Silver: one row per Bronze file processed, with gate result'
"""

NAV_COLUMNS = [
    "scheme_code", "nav_date", "nav", "scheme_name", "plan", "option", "plan_raw",
    "option_raw", "isin_growth_or_payout", "isin_reinvestment", "scheme_type", "category",
    "fund_house", "source_kind", "source_file", "file_sha256", "line_number",
]  # fmt: skip


class GateFailed(RuntimeError):
    """Raised at the end of a run when at least one file failed the gate."""


def unprocessed_lines(spark: SparkSession) -> DataFrame:
    """Bronze lines of files that have no successful entry in the load log yet."""
    done = (
        spark.table(LOAD_LOG_TABLE)
        .where(F.col("status").isin("loaded", "loaded_with_warning"))
        .select("source_file", "file_sha256")
    )
    return spark.table(BRONZE_TABLE).join(done, ["source_file", "file_sha256"], "left_anti")


def merge_nav(spark: SparkSession, rows: DataFrame) -> None:
    rows.createOrReplaceTempView("silver_nav_updates")
    cols = NAV_COLUMNS + ["silver_loaded_at", "run_id"]
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in cols)
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join("s." + c for c in cols)
    spark.sql(
        f"""
        MERGE INTO {NAV_TABLE} t
        USING silver_nav_updates s
        ON t.scheme_code = s.scheme_code AND t.nav_date = s.nav_date
        WHEN MATCHED AND t.nav <> s.nav THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


def refresh_dim_scheme(spark: SparkSession) -> int:
    """Snapshot of every scheme listed in the latest daily NAVAll.txt (from Bronze).

    The category and fund house from AMFI's header lines in this file are the
    authoritative values for the whole project. Built from Bronze, not silver.nav,
    because silver.nav may hold a matching history row instead of the daily one.
    """
    bronze = spark.table(BRONZE_TABLE)
    daily = bronze.where(~F.col("source_file").contains("/nav_history/"))
    latest_date = daily.agg(F.max("nav_file_date")).first()[0]
    latest_file = (
        daily.where(F.col("nav_file_date") == latest_date)
        .orderBy(F.col("ingested_at").desc())
        .select("source_file", "file_sha256")
        .first()
    )
    lines = bronze.where(
        (F.col("source_file") == latest_file["source_file"])
        & (F.col("file_sha256") == latest_file["file_sha256"])
    )
    parsed = parse_rows(attach_headers(classify_lines(lines))).where(
        F.col("scheme_code").isNotNull()
    )
    w = Window.partitionBy("scheme_code").orderBy(F.col("line_number").desc())
    daily_rows = parsed.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
    # Some daily rows carry only the fund name ("Edelweiss Large Cap Fund") with empty
    # Plan/Option columns; the history report names the same scheme code in full. Fill
    # Unknown plan/option from the latest Silver row that does name them.
    wh = Window.partitionBy("scheme_code").orderBy(F.col("nav_date").desc())
    known = spark.table(NAV_TABLE).where("option <> 'Unknown'")
    from_history = (
        known.withColumn("_rn", F.row_number().over(wh))
        .where("_rn = 1")
        .select(
            "scheme_code",
            F.col("option").alias("_option_h"),
            F.when(F.col("plan") != "Unknown", F.col("plan")).alias("_plan_h"),
        )
    )
    filled = (
        daily_rows.join(from_history, "scheme_code", "left")
        .withColumn(
            "option_source",
            F.when(F.col("option") != "Unknown", "daily_file")
            .when(F.col("_option_h").isNotNull(), "latest_silver_row")
            .otherwise("none"),
        )
        .withColumn(
            "option",
            F.when(
                F.col("option") == "Unknown", F.coalesce("_option_h", F.lit("Unknown"))
            ).otherwise(F.col("option")),
        )
        .withColumn(
            "plan",
            F.when(F.col("plan") == "Unknown", F.coalesce("_plan_h", F.lit("Unknown"))).otherwise(
                F.col("plan")
            ),
        )
    )
    dim = (
        filled.select(
            "scheme_code", "scheme_name", "fund_house", "scheme_type", "category", "plan",
            "option", (F.col("option") == "Growth").alias("is_growth"), "option_source",
            "isin_growth_or_payout", "isin_reinvestment",
            F.col("nav_date").alias("latest_nav_date_in_file"),
            F.col("nav").alias("latest_nav_in_file"),
            F.lit(latest_date).alias("as_of_file_date"),
            F.lit(latest_file["source_file"]).alias("source_file"),
            F.current_timestamp().alias("refreshed_at"),
        )
    )  # fmt: skip
    dim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(DIM_SCHEME_TABLE)
    return spark.table(DIM_SCHEME_TABLE).count()


def refresh_warnings(spark: SparkSession, threshold: float, run_id: str) -> int:
    moves = (
        big_moves(spark.table(NAV_TABLE), threshold)
        .withColumn("detected_at", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
    )
    moves.createOrReplaceTempView("silver_new_warnings")
    spark.sql(
        f"""
        MERGE INTO {WARNINGS_TABLE} t USING silver_new_warnings s
        ON t.scheme_code = s.scheme_code AND t.nav_date = s.nav_date AND t.rule = s.rule
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    return spark.table(WARNINGS_TABLE).count()


def run(
    spark: SparkSession,
    run_id: str,
    max_bad_ratio: float = DEFAULT_MAX_BAD_RATIO,
    move_threshold: float = DEFAULT_MOVE_THRESHOLD,
    full_refresh: bool = False,
) -> dict:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    if full_refresh:
        # Rebuild Silver from Bronze (like dbt --full-refresh). Bronze keeps every raw
        # line, so nothing is lost; the dropped tables' history goes with them.
        for table in (NAV_TABLE, QUARANTINE_TABLE, WARNINGS_TABLE, LOAD_LOG_TABLE,
                      DIM_SCHEME_TABLE, STAGING_TABLE):  # fmt: skip
            spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"silver: full refresh, dropped Silver tables (run_id={run_id})")
    for ddl in (NAV_DDL, QUARANTINE_DDL, WARNINGS_DDL, LOAD_LOG_DDL):
        spark.sql(ddl)

    lines = unprocessed_lines(spark)
    if lines.isEmpty():
        print(f"silver: nothing new in Bronze (run_id={run_id})")
        return {"files": 0}

    # Materialise the checked rows once (serverless cannot cache), then reuse them for
    # the gate, the quarantine and the MERGE instead of re-parsing 12M lines 3 times.
    quality_reason(parse_rows(attach_headers(classify_lines(lines)))).write.mode(
        "overwrite"
    ).option("overwriteSchema", "true").saveAsTable(STAGING_TABLE)
    checked = spark.table(STAGING_TABLE)
    gate = gate_by_file(checked, max_bad_ratio).collect()  # one small row per file
    passed = [g["source_file"] for g in gate if g["status"].startswith("loaded")]
    failed = [g for g in gate if g["status"].startswith("rejected")]
    now = F.current_timestamp()

    # Quarantine: every bad row of every file (also of rejected files, for the record).
    # Insert-only on (file, hash, line), so retrying a rejected file adds no duplicates.
    checked.where(F.col("dq_reason").isNotNull()).select(
        "source_file", "file_sha256", "nav_file_date", "line_number", "raw_line",
        "scheme_code", "nav_date", "nav_raw", "scheme_name", "dq_reason",
        "dq_counts_toward_gate", now.alias("quarantined_at"), F.lit(run_id).alias("run_id"),
    ).createOrReplaceTempView("silver_new_quarantine")  # fmt: skip
    spark.sql(
        f"""
        MERGE INTO {QUARANTINE_TABLE} t USING silver_new_quarantine s
        ON t.source_file = s.source_file AND t.file_sha256 = s.file_sha256
           AND t.line_number = s.line_number
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    if passed:
        good = checked.where(F.col("dq_reason").isNull() & F.col("source_file").isin(passed))
        merge_nav(
            spark,
            dedupe_latest(good)
            .select(*NAV_COLUMNS)
            .withColumn("silver_loaded_at", now)
            .withColumn("run_id", F.lit(run_id)),
        )

    log = spark.createDataFrame([g.asDict() for g in gate]).select(
        "source_file", "file_sha256", "nav_file_date", "source_kind",
        F.col("data_rows").cast("bigint"), F.col("good_rows").cast("bigint"),
        F.col("bad_rows").cast("bigint"), F.col("known_excluded_rows").cast("bigint"),
        F.col("bad_ratio").cast("double"), "status",
        F.current_timestamp().alias("processed_at"), F.lit(run_id).alias("run_id"),
    )  # fmt: skip
    log.write.mode("append").saveAsTable(LOAD_LOG_TABLE)

    schemes = refresh_dim_scheme(spark) if passed else None
    warnings = refresh_warnings(spark, move_threshold, run_id) if passed else None

    summary = {
        "files": len(gate),
        "passed": len(passed),
        "with_warning": sum(g["status"] == "loaded_with_warning" for g in gate),
        "rejected": len(failed),
        "data_rows": sum(g["data_rows"] for g in gate),
        "good_rows": sum(g["good_rows"] for g in gate),
        "bad_rows": sum(g["bad_rows"] for g in gate),
        "known_excluded_rows": sum(g["known_excluded_rows"] for g in gate),
        "dim_scheme_rows": schemes,
        "warnings_total": warnings,
    }
    print(f"silver: {summary} (run_id={run_id})")
    if failed:
        worst = sorted(failed, key=lambda g: -g["bad_ratio"])[:5]
        raise GateFailed(
            f"{len(failed)} file(s) failed the gate (max {max_bad_ratio:.2%}); none of their "
            f"rows reached {NAV_TABLE}. Worst: "
            + "; ".join(f"{g['source_file']} {g['status']} {g['bad_ratio']:.2%}" for g in worst)
        )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze to Silver with data-quality gate")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--max-bad-ratio", type=float, default=DEFAULT_MAX_BAD_RATIO)
    parser.add_argument("--move-threshold", type=float, default=DEFAULT_MOVE_THRESHOLD)
    parser.add_argument(
        "--full-refresh", action="store_true", help="drop Silver and rebuild it from Bronze"
    )
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    run(spark, args.run_id, args.max_bad_ratio, args.move_threshold, args.full_refresh)


if __name__ == "__main__":
    main()
