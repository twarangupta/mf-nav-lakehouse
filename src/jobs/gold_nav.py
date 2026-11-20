"""Gold: fund analytics on Growth plans, built from Silver. Small, dashboard-ready tables.

Tables (all rebuilt on every run, so the job is idempotent):
- gold.fund_metrics            one row per Growth scheme: latest NAV, 1Y return, 3Y and 5Y
                               CAGR, annualised volatility, max drawdown, category ranks
- gold.category_summary        one row per (category, plan): medians and the top fund
- gold.rolling_return_1y       rolling 1-year return for every scheme and NAV date (large;
                               stays in Databricks)
- gold.rolling_return_1y_monthly  the same at each month's last NAV date (small; for charts
                               and the public export)
- gold.dq_daily                rows loaded, quarantined and warned per load date
- gold.kpis                    one row of headline numbers

Rules for honest numbers:
- Growth option plans only (IDCW NAVs drop on every payout, so they are not total-return).
- Daily returns only between consecutive real NAV dates; gaps are never forward filled.
- "NAV n years ago" is an as-of lookup: the last NAV on or before the target date, and
  null if that NAV is more than 7 days older than the target.
- A scheme younger than a window gets null for that window, not a partial value.
- Volatility is annualised with the scheme's own number of NAV observations in the last
  year (median 248 for non-liquid Growth schemes in 2025; 365 for liquid/overnight).
- Gold refuses to run if Silver's latest run rejected a file (the gate), unless --force.

Run on Databricks serverless:

    gold_nav.py --run-id manual-1
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

SILVER_NAV = "mf_nav.silver.nav"
SILVER_DIM = "mf_nav.silver.dim_scheme"
SILVER_LOAD_LOG = "mf_nav.silver.load_log"
SILVER_QUARANTINE = "mf_nav.silver.nav_quarantine"
SILVER_WARNINGS = "mf_nav.silver.dq_warnings"
GOLD = "mf_nav.gold"

MAX_STALE_DAYS = 7  # as-of lookups and "active scheme" tolerance
MIN_OBS_1Y = 200  # fewer NAVs than this in the last year: volatility is null
RESET_MIN_RISE = 1.0  # +100% or more in one step: a reset, not a market move
RESET_MAX_FALL = -0.5  # -50% or less in one step: a reset
FLAT_NAV_MAX_RANGE = 0.001  # NAV range under 0.1% over a year: not a growth series


class SilverGateFailed(RuntimeError):
    """Silver's latest run rejected at least one file; Gold must not be built."""


# ------------------------------------------------------------------ building blocks


def asof_lookup(nav: DataFrame, queries: DataFrame, max_stale_days: int = MAX_STALE_DAYS):
    """For each query (scheme_code, qid, target_date) find the last NAV on or before target.

    Works by putting real NAV rows and query rows in one ordered list per scheme and
    carrying the last real NAV forward onto each query row (a window, no range join).
    At the same date a real row sorts before the query, so "on or before" is inclusive.
    Returns: scheme_code, qid, target_date, asof_nav, asof_date (both null when the
    nearest earlier NAV is more than max_stale_days older than the target, or missing).
    """
    real = nav.select(
        "scheme_code",
        F.col("nav_date").alias("_d"),
        F.lit(0).alias("_is_query"),
        F.lit(None).cast("string").alias("qid"),
        F.col("nav").alias("_nav"),
        F.col("nav_date").alias("_nav_date"),
    )
    asked = queries.select(
        "scheme_code",
        F.col("target_date").alias("_d"),
        F.lit(1).alias("_is_query"),
        F.col("qid").cast("string").alias("qid"),
        F.lit(None).cast(nav.schema["nav"].dataType).alias("_nav"),
        F.lit(None).cast("date").alias("_nav_date"),
    )
    w = (
        Window.partitionBy("scheme_code")
        .orderBy("_d", "_is_query")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    found = (
        real.unionByName(asked)
        .withColumn("_f_nav", F.last("_nav", True).over(w))
        .withColumn("_f_date", F.last("_nav_date", True).over(w))
        .where(F.col("_is_query") == 1)
    )
    fresh = F.datediff(F.col("_d"), F.col("_f_date")) <= max_stale_days
    return found.select(
        "scheme_code",
        "qid",
        F.col("_d").alias("target_date"),
        F.when(fresh, F.col("_f_nav")).alias("asof_nav"),
        F.when(fresh, F.col("_f_date")).alias("asof_date"),
    )


def cagr(end_nav, start_nav, end_date, start_date) -> F.Column:
    """Compound annual growth rate using the real number of days between the two NAVs."""
    years = F.datediff(end_date, start_date) / F.lit(365.25)
    return F.when(
        (start_nav > 0) & (years > 0),
        F.pow(end_nav.cast("double") / start_nav.cast("double"), 1.0 / years) - 1.0,
    )


def with_daily_returns(nav: DataFrame) -> DataFrame:
    """Simple return between each NAV and the scheme's previous real NAV (no filling)."""
    w = Window.partitionBy("scheme_code").orderBy("nav_date")
    prev = F.lag("nav").over(w)
    return nav.withColumn(
        "daily_return", F.when(prev > 0, F.col("nav").cast("double") / prev.cast("double") - 1.0)
    )


def category_group(category: F.Column) -> F.Column:
    """A standard name for AMFI's category text, used for ranking and summaries.

    AMFI's own daily file uses two headers for the same SEBI category, for example
    "Equity Scheme - Large Cap Fund" and "Equity Schemes - Large Cap Fund", or
    "Debt Scheme - Liquid Fund" and "Income/Debt Oriented Schemes - Liquid Fund".
    The group is the asset class plus the sub-category: "Equity - Large Cap".
    """
    parts = F.split(category, r"\s+-\s+", 2)
    prefix = F.lower(F.try_element_at(parts, F.lit(1)))
    asset = (
        F.when(prefix.startswith("equity") | prefix.startswith("elss"), "Equity")
        .when(prefix.rlike("debt|income"), "Debt")
        .when(prefix.startswith("hybrid"), "Hybrid")
        .when(prefix.startswith("solution"), "Solution Oriented")
        .otherwise("Other")
    )
    # try_element_at: null instead of an error when there is no " - " (ANSI mode).
    sub = F.coalesce(F.try_element_at(parts, F.lit(2)), F.try_element_at(parts, F.lit(1)))
    sub = F.trim(F.regexp_replace(sub, r"(?i)\s*\bfunds?\s*$", ""))
    sub = F.regexp_replace(sub, r"\s+", " ")
    sub = F.regexp_replace(sub, r"\s*/\s*", "/")
    # Aliases for the same SEBI category written two ways in AMFI's file.
    low = F.lower(sub)
    sub = (
        F.when(low.rlike(r"^thematic$"), "Sectoral/Thematic")
        .when(low.startswith("elss"), "ELSS")
        .when(low.startswith("banking and psu"), "Banking and PSU")
        .when(low.contains("fund of funds") & low.contains("domestic"), "FoF Domestic")
        .when(low.contains("fund of funds") & low.contains("overseas"), "FoF Overseas")
        .otherwise(sub)
    )
    # initcap lowercases acronyms ("ELSS" -> "Elss"); put the common ones back.
    nice = F.initcap(sub)
    for word, fixed in (("Elss", "ELSS"), ("Fof", "FoF"), ("Etfs?", "ETF"), ("Psu", "PSU")):
        nice = F.regexp_replace(nice, "(^|[^A-Za-z])" + word + "($|[^A-Za-z])", "$1" + fixed + "$2")
    nice = F.regexp_replace(nice, "Sectoral/thematic", "Sectoral/Thematic")
    return F.concat_ws(" - ", asset, nice)


def growth_series(spark: SparkSession) -> DataFrame:
    """Silver NAVs of Growth schemes (per dim_scheme), with scheme attributes."""
    dim = spark.table(SILVER_DIM).where("is_growth")
    return (
        spark.table(SILVER_NAV)
        .select("scheme_code", "nav_date", "nav")
        .join(
            dim.select(
                "scheme_code", "scheme_name", "fund_house", "scheme_type", "category", "plan"
            ).withColumn("category_group", category_group(F.col("category"))),
            "scheme_code",
        )
    )


def reset_breaks(warnings: DataFrame) -> DataFrame:
    """Keep only flagged moves that look like a NAV reset, not a market move.

    Silver warns on any Growth move above 20%. Measured on our data, moves of +100% or
    more, or -50% or less, are unit splits (ratios about 0.01, 0.1, 10) or segregated
    portfolio revaluations (ratios 2 to 4); moves between are mostly real markets, for
    example the silver ETF fall in February 2026. Only resets break a return window.
    """
    big = (F.col("pct_change") >= RESET_MIN_RISE) | (F.col("pct_change") <= RESET_MAX_FALL)
    return warnings.where(big).select("scheme_code", "nav_date")


def nav_breaks(spark: SparkSession) -> DataFrame:
    """Dates of NAV resets (scheme_code, nav_date), from Silver's warnings."""
    return reset_breaks(spark.table(SILVER_WARNINGS))


def fund_metrics(series: DataFrame, breaks: DataFrame | None = None) -> DataFrame:
    """One row per Growth scheme with trailing returns, risk and category ranks.

    `breaks` (scheme_code, nav_date) are NAV discontinuities flagged in Silver, such as
    a 10-to-1 unit split. A metric whose window contains a break is set to null and
    `metrics_nulled_for_break` says so, because a return across a reset is meaningless.
    """
    nav = series.select("scheme_code", "nav_date", "nav")
    w_last = Window.partitionBy("scheme_code").orderBy(F.col("nav_date").desc())
    latest = (
        series.withColumn("_rn", F.row_number().over(w_last))
        .where("_rn = 1")
        .select(
            "scheme_code", "scheme_name", "fund_house", "scheme_type", "category",
            "category_group", "plan",
            F.col("nav_date").alias("latest_nav_date"), F.col("nav").alias("latest_nav"),
        )
    )  # fmt: skip
    as_of_date = latest.agg(F.max("latest_nav_date")).first()[0]

    queries = None
    for years in (1, 3, 5):
        q = latest.select(
            "scheme_code",
            F.lit(f"{years}y").alias("qid"),
            F.add_months("latest_nav_date", -12 * years).alias("target_date"),
        )
        queries = q if queries is None else queries.unionByName(q)
    found = asof_lookup(nav, queries)
    pivot = (
        found.groupBy("scheme_code")
        .pivot("qid", ["1y", "3y", "5y"])
        .agg(F.first("asof_nav").alias("nav"), F.first("asof_date").alias("date"))
    )
    m = latest.join(pivot, "scheme_code", "left")
    end_nav, end_date = F.col("latest_nav"), F.col("latest_nav_date")
    m = (
        m.withColumn(
            "return_1y",
            F.when(
                F.col("1y_nav") > 0, end_nav.cast("double") / F.col("1y_nav").cast("double") - 1
            ),
        )
        .withColumn("cagr_3y", cagr(end_nav, F.col("3y_nav"), end_date, F.col("3y_date")))
        .withColumn("cagr_5y", cagr(end_nav, F.col("5y_nav"), end_date, F.col("5y_date")))
        .withColumnRenamed("1y_date", "nav_1y_ago_date")
        .withColumnRenamed("3y_date", "nav_3y_ago_date")
        .withColumnRenamed("5y_date", "nav_5y_ago_date")
        .drop("1y_nav", "3y_nav", "5y_nav")
    )

    # Risk over windows that start at the as-of NAV date (only schemes old enough).
    starts = m.select("scheme_code", "latest_nav_date", "nav_1y_ago_date", "nav_3y_ago_date")
    rets = with_daily_returns(nav).join(starts, "scheme_code")
    vol = (
        rets.where(
            F.col("nav_1y_ago_date").isNotNull()
            & (F.col("nav_date") > F.col("nav_1y_ago_date"))
            & F.col("daily_return").isNotNull()
        )
        .groupBy("scheme_code")
        .agg(
            F.stddev_samp("daily_return").alias("_sd"),
            F.count("*").alias("obs_1y"),
            ((F.max("nav") - F.min("nav")) / F.min("nav")).cast("double").alias("_range_1y"),
        )
        .withColumn(
            "volatility_1y",
            F.when(F.col("obs_1y") >= MIN_OBS_1Y, F.col("_sd") * F.sqrt(F.col("obs_1y"))),
        )
        .drop("_sd")
    )
    w_peak = (
        Window.partitionBy("scheme_code")
        .orderBy("nav_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    dd = (
        rets.where(
            F.col("nav_3y_ago_date").isNotNull() & (F.col("nav_date") >= F.col("nav_3y_ago_date"))
        )
        .withColumn("_peak", F.max("nav").over(w_peak))
        .withColumn("_dd", F.col("nav").cast("double") / F.col("_peak").cast("double") - 1)
        .groupBy("scheme_code")
        .agg(F.min("_dd").alias("max_drawdown_3y"))
    )
    m = m.join(vol, "scheme_code", "left").join(dd, "scheme_code", "left")

    if breaks is not None:
        last_break = breaks.groupBy("scheme_code").agg(F.max("nav_date").alias("_last_break"))
        m = m.join(last_break, "scheme_code", "left")
        hit = lambda start: F.col("_last_break") > F.col(start)  # noqa: E731
        nulled = []
        for cols, start in (
            (["return_1y", "volatility_1y"], "nav_1y_ago_date"),
            (["cagr_3y", "max_drawdown_3y"], "nav_3y_ago_date"),
            (["cagr_5y"], "nav_5y_ago_date"),
        ):
            for c in cols:
                m = m.withColumn(c, F.when(hit(start), None).otherwise(F.col(c)))
            nulled.append(F.when(hit(start), F.lit(",".join(cols))))
        m = (
            m.withColumn("metrics_nulled_for_break", F.concat_ws(",", *nulled))
            .withColumn(
                "metrics_nulled_for_break",
                F.when(F.col("metrics_nulled_for_break") != "", F.col("metrics_nulled_for_break")),
            )
            .withColumnRenamed("_last_break", "last_nav_break_date")
        )

    # A NAV that hardly moved in a year (e.g. 999.9996 to 1000.0001) is a daily-dividend
    # style plan labelled "Growth" by AMFI, not a total-return series: no return metrics.
    # A real liquid fund grows about 6% a year, so "under 0.1% range" is a safe test.
    m = m.withColumn(
        "is_flat_nav_1y", F.coalesce(F.col("_range_1y") < FLAT_NAV_MAX_RANGE, F.lit(False))
    )
    for c in ("return_1y", "volatility_1y", "cagr_3y", "max_drawdown_3y", "cagr_5y"):
        m = m.withColumn(c, F.when(F.col("is_flat_nav_1y"), None).otherwise(F.col(c)))
    m = m.drop("_range_1y")

    is_active = F.datediff(F.lit(as_of_date), F.col("latest_nav_date")) <= MAX_STALE_DAYS
    m = m.withColumn("is_active", is_active).withColumn("as_of_date", F.lit(as_of_date))
    # Rank within (category_group, plan) among active schemes that have the metric;
    # Direct and Regular are ranked separately because their fees differ.
    for metric, rank_col, count_col in (
        ("cagr_3y", "rank_cagr_3y_in_category", "ranked_in_category_3y"),
        ("return_1y", "rank_return_1y_in_category", "ranked_in_category_1y"),
    ):
        m = m.withColumn("_e", F.col("is_active") & F.col(metric).isNotNull())
        group = Window.partitionBy("category_group", "plan", "_e")
        m = (
            m.withColumn(
                rank_col, F.when(F.col("_e"), F.rank().over(group.orderBy(F.col(metric).desc())))
            )
            .withColumn(count_col, F.when(F.col("_e"), F.count("*").over(group)))
            .drop("_e")
        )
    return m


def category_summary(metrics: DataFrame) -> DataFrame:
    """Medians per (category_group, plan) over active schemes, plus the top 3Y CAGR fund."""
    active = metrics.where("is_active")
    med = lambda c: F.percentile_approx(c, 0.5).alias(f"median_{c}")  # noqa: E731
    summary = active.groupBy("category_group", "plan").agg(
        F.count("*").alias("active_schemes"),
        F.count("cagr_3y").alias("schemes_with_3y"),
        med("return_1y"), med("cagr_3y"), med("cagr_5y"), med("volatility_1y"),
        med("max_drawdown_3y"),
    )  # fmt: skip
    top = active.where("rank_cagr_3y_in_category = 1").select(
        "category_group", "plan",
        F.col("scheme_code").alias("top_scheme_code_3y"),
        F.col("scheme_name").alias("top_scheme_name_3y"),
        F.col("cagr_3y").alias("top_cagr_3y"),
    )  # fmt: skip
    w = Window.partitionBy("category_group", "plan").orderBy("top_scheme_code_3y")
    top = top.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    return summary.join(top, ["category_group", "plan"], "left")


def rolling_return_1y(series: DataFrame, breaks: DataFrame | None = None) -> DataFrame:
    """Rolling 1-year return for every scheme and real NAV date (as-of, 7-day tolerance).

    A point whose 1-year window contains a NAV reset (`breaks`) is dropped, the same
    rule as in fund_metrics; otherwise a 10-to-1 split shows as a year of +900%.
    """
    nav = series.select("scheme_code", "nav_date", "nav")
    queries = nav.select(
        "scheme_code",
        F.col("nav_date").cast("string").alias("qid"),
        F.add_months("nav_date", -12).alias("target_date"),
    )
    found = asof_lookup(nav, queries).select(
        "scheme_code",
        F.col("qid").cast("date").alias("nav_date"),
        F.col("asof_date").alias("nav_1y_ago_date"),
        F.col("asof_nav").alias("nav_1y_ago"),
    )
    rolling = (
        nav.join(found, ["scheme_code", "nav_date"])
        .where(F.col("nav_1y_ago") > 0)
        .withColumn(
            "return_1y", F.col("nav").cast("double") / F.col("nav_1y_ago").cast("double") - 1
        )
    )
    if breaks is None:
        return rolling
    b = breaks.select("scheme_code", F.col("nav_date").alias("_break"))
    spans_break = (
        rolling.join(F.broadcast(b), "scheme_code")
        .where(
            (F.col("_break") > F.col("nav_1y_ago_date")) & (F.col("_break") <= F.col("nav_date"))
        )
        .select("scheme_code", "nav_date")
        .distinct()
    )
    return rolling.join(spans_break, ["scheme_code", "nav_date"], "left_anti")


def month_end(rolling: DataFrame) -> DataFrame:
    """Keep each scheme's last NAV date in every calendar month."""
    w = Window.partitionBy("scheme_code", F.trunc("nav_date", "month")).orderBy(
        F.col("nav_date").desc()
    )
    return (
        rolling.withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
        .withColumn("month", F.trunc("nav_date", "month"))
    )


# --------------------------------------------------------------------------- job


def check_silver_gate(spark: SparkSession) -> None:
    last = spark.table(SILVER_LOAD_LOG).agg(F.max("processed_at")).first()[0]
    rejected = (
        spark.table(SILVER_LOAD_LOG)
        .where((F.col("processed_at") == F.lit(last)) & F.col("status").startswith("rejected"))
        .count()
    )
    if rejected:
        raise SilverGateFailed(
            f"Silver's latest run rejected {rejected} file(s); Gold was not built."
        )


def write(df: DataFrame, name: str, comment: str) -> int:
    table = f"{GOLD}.{name}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    spark = df.sparkSession
    spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")
    return spark.table(table).count()


def run(spark: SparkSession, run_id: str, force: bool = False) -> dict:
    if not force:
        check_silver_gate(spark)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")
    stamp = [F.current_timestamp().alias("computed_at"), F.lit(run_id).alias("run_id")]
    series = growth_series(spark)

    metrics = fund_metrics(series, nav_breaks(spark)).select("*", *stamp)
    counts = {"fund_metrics": write(metrics, "fund_metrics", "Gold: one row per Growth scheme")}
    metrics = spark.table(f"{GOLD}.fund_metrics")
    counts["category_summary"] = write(
        category_summary(metrics).select("*", *stamp),
        "category_summary",
        "Gold: medians per category and plan over active Growth schemes",
    )
    counts["rolling_return_1y"] = write(
        rolling_return_1y(series, nav_breaks(spark)).select("*", *stamp),
        "rolling_return_1y",
        "Gold: rolling 1-year return per Growth scheme and NAV date",
    )
    monthly = month_end(spark.table(f"{GOLD}.rolling_return_1y")).join(
        metrics.where("is_active").select("scheme_code"), "scheme_code"
    )
    counts["rolling_return_1y_monthly"] = write(
        monthly.select("scheme_code", "month", "nav_date", "nav", "return_1y", *stamp),
        "rolling_return_1y_monthly",
        "Gold: rolling 1-year return at each month end, active Growth schemes",
    )
    counts["dq_daily"] = write(dq_daily(spark).select("*", *stamp), "dq_daily",
                               "Gold: data-quality counts per load date")  # fmt: skip
    counts["kpis"] = write(kpis(spark).select("*", *stamp), "kpis", "Gold: headline numbers")
    print(f"gold: {counts} (run_id={run_id})")
    return counts


def dq_daily(spark: SparkSession) -> DataFrame:
    log = (
        spark.table(SILVER_LOAD_LOG)
        .groupBy(F.col("nav_file_date").alias("load_date"), "source_kind")
        .agg(
            F.countDistinct("source_file").alias("files"),
            F.max("data_rows").alias("data_rows"),
            F.max("good_rows").alias("good_rows"),
            F.max("bad_rows").alias("quarantined_counted_rows"),
            F.max("known_excluded_rows").alias("quarantined_known_rows"),
            F.max("status").alias("status"),
        )
    )
    warn = (
        spark.table(SILVER_WARNINGS)
        .groupBy(F.col("nav_date").alias("load_date"))
        .agg(F.count("*").alias("warnings_on_nav_date"))
    )
    return log.join(warn, "load_date", "full_outer").fillna({"warnings_on_nav_date": 0})


def kpis(spark: SparkSession) -> DataFrame:
    m = spark.table(f"{GOLD}.fund_metrics")
    nav = spark.table(SILVER_NAV)
    return m.agg(
        F.max("as_of_date").alias("as_of_date"),
        F.count_if("is_active").alias("active_growth_schemes"),
        F.count_if(F.col("is_active") & F.col("cagr_3y").isNotNull()).alias("schemes_with_3y_cagr"),
        F.count_if(F.col("is_active") & F.col("cagr_5y").isNotNull()).alias("schemes_with_5y_cagr"),
        F.countDistinct("category_group").alias("category_groups"),
        F.count_if(F.col("is_active") & F.col("metrics_nulled_for_break").isNotNull()).alias(
            "active_schemes_with_nav_break"
        ),
        F.count_if(F.col("is_active") & F.col("is_flat_nav_1y")).alias("active_schemes_flat_nav"),
    ).crossJoin(
        nav.agg(
            F.count("*").alias("silver_nav_rows"),
            F.countDistinct("scheme_code").alias("silver_schemes"),
            F.min("nav_date").alias("first_nav_date"),
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver to Gold fund analytics")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--force", action="store_true", help="build even if Silver rejected files")
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    run(spark, args.run_id, args.force)


if __name__ == "__main__":
    main()
