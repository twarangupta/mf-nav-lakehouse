"""Unit tests for Gold analytics on a local SparkSession (no Delta)."""

import math
from datetime import date, timedelta
from decimal import Decimal

import pytest

from src.jobs import gold_nav as gd

NAV_SCHEMA = "scheme_code int, nav_date date, nav decimal(20,6)"


def _nav(spark, rows):
    return spark.createDataFrame([(c, d, Decimal(str(v))) for c, d, v in rows], NAV_SCHEMA)


def _queries(spark, rows):
    return spark.createDataFrame(rows, "scheme_code int, qid string, target_date date")


def test_asof_is_inclusive_takes_last_earlier_nav_and_respects_staleness(spark):
    nav = _nav(
        spark,
        [
            (1, date(2025, 1, 1), 10),
            (1, date(2025, 1, 3), 11),  # holiday on Jan 4 and 5
            (1, date(2025, 1, 6), 12),
        ],
    )
    q = _queries(
        spark,
        [
            (1, "same_day", date(2025, 1, 3)),
            (1, "holiday", date(2025, 1, 5)),
            (1, "too_late", date(2025, 1, 20)),  # 14 days after the last NAV: stale
            (1, "too_early", date(2024, 12, 1)),  # before the scheme existed
        ],
    )
    out = {r.qid: r for r in gd.asof_lookup(nav, q).collect()}
    assert (out["same_day"].asof_nav, out["same_day"].asof_date) == (
        Decimal("11"),
        date(2025, 1, 3),
    )
    assert out["holiday"].asof_date == date(2025, 1, 3)
    assert out["too_late"].asof_nav is None
    assert out["too_early"].asof_nav is None


def test_cagr_uses_real_days(spark):
    df = spark.createDataFrame(
        [(Decimal("200"), Decimal("100"), date(2026, 1, 1), date(2023, 1, 1))],
        "e decimal(20,6), s decimal(20,6), ed date, sd date",
    )
    value = df.select(gd.cagr(df.e, df.s, df.ed, df.sd).alias("c")).first().c
    years = (date(2026, 1, 1) - date(2023, 1, 1)).days / 365.25
    assert value == pytest.approx(2 ** (1 / years) - 1)


def test_daily_returns_only_between_real_dates(spark):
    nav = _nav(spark, [(1, date(2025, 1, 2), 100), (1, date(2025, 1, 6), 110)])
    rows = gd.with_daily_returns(nav).orderBy("nav_date").collect()
    assert rows[0].daily_return is None
    assert rows[1].daily_return == pytest.approx(0.10)  # one return across the gap, no zeros


def _series(spark, nav_rows, category="Equity - Large Cap", plan="Direct", name="F"):
    nav = _nav(spark, nav_rows)
    dim = spark.createDataFrame(
        [(c, f"{name}{c}", "AMC", "Open Ended", category, plan) for c in {r[0] for r in nav_rows}],
        "scheme_code int, scheme_name string, fund_house string, scheme_type string, "
        "category string, plan string",
    )
    return nav.join(
        dim.withColumn("category_group", gd.category_group(dim.category)), "scheme_code"
    )


def _business_days(start, n):
    days, d = [], start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def test_fund_metrics_cagr_young_scheme_and_ranks(spark):
    start = date(2021, 1, 4)
    days = _business_days(start, 260 * 4)  # about 4 years of business days
    # scheme 1 grows 10% a year, scheme 2 grows 20% a year; scheme 3 starts 6 months ago
    rows = []
    for d in days:
        t = (d - start).days / 365.25
        rows.append((1, d, round(10 * 1.10**t, 6)))
        rows.append((2, d, round(10 * 1.20**t, 6)))
    for d in days[-130:]:
        rows.append((3, d, 10))
    m = {r.scheme_code: r for r in gd.fund_metrics(_series(spark, rows)).collect()}
    assert m[1].cagr_3y == pytest.approx(0.10, abs=0.002)
    assert m[2].cagr_3y == pytest.approx(0.20, abs=0.002)
    assert m[1].cagr_5y is None  # only 4 years of data: no partial 5Y number
    assert m[3].return_1y is None and m[3].cagr_3y is None  # younger than the window
    assert (m[2].rank_cagr_3y_in_category, m[1].rank_cagr_3y_in_category) == (1, 2)
    assert m[3].rank_cagr_3y_in_category is None
    assert m[1].ranked_in_category_3y == 2
    assert m[1].obs_1y >= 250 and m[1].volatility_1y is not None
    assert m[1].max_drawdown_3y == pytest.approx(0.0, abs=1e-9)  # never falls


def test_volatility_annualised_with_real_observation_count(spark):
    days = _business_days(date(2023, 1, 2), 520)
    rows, nav = [], 100.0
    for i, d in enumerate(days):
        nav *= 1.01 if i % 2 == 0 else 0.99  # alternating +1% / -1%
        rows.append((1, d, round(nav, 6)))
    m = gd.fund_metrics(_series(spark, rows)).first()
    rets = [1.01 - 1 if i % 2 == 0 else 0.99 - 1 for i in range(m.obs_1y)]
    mean = sum(rets) / len(rets)
    sd = math.sqrt(sum((r - mean) ** 2 for r in rets) / (len(rets) - 1))
    assert m.volatility_1y == pytest.approx(sd * math.sqrt(m.obs_1y), rel=0.02)


def test_max_drawdown(spark):
    days = _business_days(date(2021, 1, 4), 1000)
    rows = []
    for i, d in enumerate(days):
        nav = 100 if i < 600 else (70 if i < 700 else 120 + i / 100)  # falls 30%, recovers
        rows.append((1, d, nav))
    m = gd.fund_metrics(_series(spark, rows)).first()
    assert m.max_drawdown_3y == pytest.approx(-0.30)


def test_rolling_return_and_month_end(spark):
    days = _business_days(date(2024, 1, 1), 400)
    rows = [(1, d, 100 + i) for i, d in enumerate(days)]
    rolling = gd.rolling_return_1y(_series(spark, rows))
    first = rolling.orderBy("nav_date").first()
    assert first.nav_1y_ago_date <= first.nav_date.replace(year=first.nav_date.year - 1)
    monthly = gd.month_end(rolling).collect()
    months = {(r.month.year, r.month.month) for r in monthly}
    assert len(months) == len(monthly)  # one row per month


@pytest.mark.parametrize(
    "category, group",
    [
        ("Equity Scheme - Large Cap Fund", "Equity - Large Cap"),
        ("Equity Schemes - Large Cap Fund", "Equity - Large Cap"),
        ("Debt Scheme - Liquid Fund", "Debt - Liquid"),
        ("Income/Debt Oriented Schemes - Liquid Fund", "Debt - Liquid"),
        ("Equity Scheme - Sectoral/ Thematic", "Equity - Sectoral/thematic"),
        ("Other Scheme - Index Funds", "Other - Index"),
        ("ELSS", "Other - Elss"),
        ("Equity Schemes - Thematic Fund", "Equity - Sectoral/thematic"),
        (
            "Fund of Funds Scheme (Domestic) - Fund of Funds Scheme (Domestic)",
            "Other - Fof Domestic",
        ),
        ("Other Scheme - FoF Domestic", "Other - Fof Domestic"),
    ],
)
def test_category_group_merges_amfi_spellings(spark, category, group):
    df = spark.createDataFrame([(category,)], "c string")
    assert df.select(gd.category_group(df.c).alias("g")).first().g == group


def test_metrics_nulled_when_window_contains_a_nav_break(spark):
    days = _business_days(date(2021, 1, 4), 1040)
    rows = [(1, d, 100.0 if i < 900 else 10.0) for i, d in enumerate(days)]  # 10:1 reset
    breaks = spark.createDataFrame([(1, days[900])], "scheme_code int, nav_date date")
    m = gd.fund_metrics(_series(spark, rows), breaks).first()
    assert m.cagr_3y is None and m.max_drawdown_3y is None  # break is inside 3Y
    assert m.return_1y is None and m.volatility_1y is None  # and inside 1Y
    assert "cagr_3y" in m.metrics_nulled_for_break
    assert m.last_nav_break_date == days[900]
    assert m.rank_cagr_3y_in_category is None


def test_break_outside_window_keeps_metric(spark):
    days = _business_days(date(2021, 1, 4), 1040)
    rows = [(1, d, 100.0 + i) for i, d in enumerate(days)]
    breaks = spark.createDataFrame([(1, days[10])], "scheme_code int, nav_date date")
    m = gd.fund_metrics(_series(spark, rows), breaks).first()
    assert m.return_1y is not None and m.cagr_3y is not None
    assert m.metrics_nulled_for_break is None


def test_flat_nav_plan_gets_no_return_metrics(spark):
    days = _business_days(date(2022, 1, 3), 900)
    rows = [(1, d, 1000 + (i % 3) / 10000) for i, d in enumerate(days)]  # tiny jitter only
    m = gd.fund_metrics(_series(spark, rows)).first()
    assert m.is_flat_nav_1y is True
    assert m.return_1y is None and m.cagr_3y is None and m.volatility_1y is None
    assert m.rank_return_1y_in_category is None
