"""Unit tests for Silver parsing and data quality, on a local SparkSession (no Delta)."""

from datetime import date
from decimal import Decimal

import pytest

from src.jobs import silver_nav as sv

DAILY = "s3://b/raw/nav/date=2026-11-12/NAVAll.txt"
HIST = "s3://b/raw/nav_history/date=2025-11-03/NAVHistory_2025-11-03_2025-11-09.txt"

DAILY_LINES = [
    sv.LAYOUTS["daily"]["header"],
    " ",
    "Open Ended Schemes(Equity Scheme - Large Cap Fund)",
    " ",
    "Axis Mutual Fund",
    " ",
    "120465;INF846K01DP8;-;Axis Large Cap Fund - Direct Plan - Growth;Direct Plan;Growth;60.1200;12-Nov-2026",
    "120466;INF846K01DQ6;INF846K01DR4;Axis Large Cap Fund - Direct Plan - IDCW;Direct Plan;IDCW Option;20.5000;12-Nov-2026",
    "Open Ended Schemes(Debt Scheme - Credit Risk Fund)",
    "Franklin Templeton Mutual Fund",
    "148304;INF090I01VZ8;-;Franklin India Credit Risk Fund (No. of segregated portfolios-3);Direct Plan;Growth;0.0000;12-Nov-2026",
    "150000;INF000X00001;-;Some Fund - Growth;;;N.A.;12-Nov-2026",
]  # fmt: skip

HIST_LINES = [
    sv.LAYOUTS["history"]["header"],
    "",
    "Open Ended Schemes ( Equity Schemes - Large Cap Fund )",
    "",
    "",
    "Axis Mutual Fund",
    "120465;Axis Large Cap Fund - Direct Plan - Growth;Direct Plan;Growth;INF846K01DP8;;55.0000;03-Nov-2025",
    "120465;Axis Large Cap Fund - Direct Plan - Growth;Direct Plan;Growth;INF846K01DP8;;56.0000;04-Nov-2025",
    "120470;Axis Growth Opportunities Fund - IDCW;;;INF000;;10.0000;03-Nov-2025",
]  # fmt: skip


def _bronze(spark, path, lines, file_date):
    rows = [(path, "sha-" + path[-12:], file_date, i + 1, line) for i, line in enumerate(lines)]
    return spark.createDataFrame(
        rows,
        "source_file string, file_sha256 string, nav_file_date date, line_number int, raw_line string",
    )


@pytest.fixture()
def bronze(spark):
    return _bronze(spark, DAILY, DAILY_LINES, date(2026, 11, 12)).unionByName(
        _bronze(spark, HIST, HIST_LINES, date(2025, 11, 3))
    )


def _parsed(bronze):
    return sv.parse_rows(sv.attach_headers(sv.classify_lines(bronze)))


def test_classify_lines(bronze):
    kinds = {
        (r.source_file, r.line_number): r.line_kind for r in sv.classify_lines(bronze).collect()
    }
    assert kinds[(DAILY, 1)] == "col_header"
    assert kinds[(DAILY, 2)] == "blank"
    assert kinds[(DAILY, 3)] == "category"
    assert kinds[(DAILY, 5)] == "fund_house"
    assert kinds[(DAILY, 7)] == "data"
    assert kinds[(HIST, 3)] == "category"


def test_headers_carried_down_and_both_layouts_parsed(bronze):
    rows = {(r.source_file, r.line_number): r for r in _parsed(bronze).collect()}
    d = rows[(DAILY, 7)]
    assert (d.layout, d.scheme_code, d.nav, d.nav_date) == (
        "daily",
        120465,
        Decimal("60.120000"),
        date(2026, 11, 12),
    )
    assert (d.scheme_type, d.category, d.fund_house) == (
        "Open Ended",
        "Equity Scheme - Large Cap Fund",
        "Axis Mutual Fund",
    )
    assert d.isin_growth_or_payout == "INF846K01DP8" and d.isin_reinvestment is None  # "-" is null
    seg = rows[(DAILY, 11)]
    assert (seg.category, seg.fund_house) == (
        "Debt Scheme - Credit Risk Fund",
        "Franklin Templeton Mutual Fund",
    )
    h = rows[(HIST, 7)]
    assert (h.layout, h.scheme_code, h.scheme_name, h.nav) == (
        "history",
        120465,
        "Axis Large Cap Fund - Direct Plan - Growth",
        Decimal("55.000000"),
    )
    assert h.category == "Equity Schemes - Large Cap Fund"  # spaces inside ( ) trimmed


def test_plan_and_option_rules(bronze):
    rows = {(r.source_file, r.line_number): r for r in _parsed(bronze).collect()}
    assert (rows[(DAILY, 7)].plan, rows[(DAILY, 7)].option) == ("Direct", "Growth")
    assert rows[(DAILY, 8)].option == "IDCW"
    # no Plan/Option columns: fall back to the name; "Growth" in the fund name must not win
    assert (rows[(HIST, 9)].plan, rows[(HIST, 9)].option) == ("Unknown", "IDCW")
    assert rows[(DAILY, 12)].option == "Growth"


def test_quality_reasons(bronze):
    checked = {
        (r.source_file, r.line_number): r for r in sv.quality_reason(_parsed(bronze)).collect()
    }
    assert checked[(DAILY, 7)].dq_reason is None
    assert checked[(DAILY, 11)].dq_reason == "zero_nav_segregated_portfolio"
    assert checked[(DAILY, 11)].dq_counts_toward_gate is False
    assert checked[(DAILY, 12)].dq_reason == "nav_not_a_number"
    assert checked[(DAILY, 12)].dq_counts_toward_gate is True


def test_conflicting_duplicate_in_one_file_is_quarantined(spark):
    lines = HIST_LINES[:6] + [
        "120465;X Fund - Growth;Direct Plan;Growth;I;;55.0000;03-Nov-2025",
        "120465;X Fund - Growth;Direct Plan;Growth;I;;99.0000;03-Nov-2025",
    ]
    checked = sv.quality_reason(_parsed(_bronze(spark, HIST, lines, date(2025, 11, 3))))
    assert {r.dq_reason for r in checked.collect()} == {"duplicate_conflicting_nav"}


def test_unknown_header_layout_is_rejected(spark):
    lines = ["Code;Name;NAV", "Axis Mutual Fund", "1;A;2;3;4;5;6;7"]
    checked = sv.quality_reason(_parsed(_bronze(spark, DAILY, lines, date(2026, 11, 12))))
    gate = sv.gate_by_file(checked, 0.01).collect()
    assert gate[0].status == "rejected_schema"


def test_gate_statuses(bronze):
    checked = sv.quality_reason(_parsed(bronze))
    by_file = {g.source_file: g for g in sv.gate_by_file(checked, 0.01).collect()}
    daily = by_file[DAILY]
    # 4 data rows: 2 good, 1 known (segregated zero NAV), 1 bad (N.A.) -> 25% bad: rejected
    assert (daily.data_rows, daily.good_rows, daily.known_excluded_rows, daily.bad_rows) == (
        4,
        2,
        1,
        1,
    )
    assert daily.status == "rejected_gate"
    assert by_file[HIST].status == "loaded"
    relaxed = {g.source_file: g for g in sv.gate_by_file(checked, 0.5).collect()}
    assert relaxed[DAILY].status == "loaded_with_warning"


def test_dedupe_prefers_newest_file(spark, bronze):
    hist_copy = _bronze(
        spark,
        HIST,
        HIST_LINES[:6] + ["120465;A - Growth;Direct Plan;Growth;I;;60.1200;12-Nov-2026"],
        date(2025, 11, 3),
    )
    daily = _bronze(spark, DAILY, DAILY_LINES[:7], date(2026, 11, 12))
    good = sv.quality_reason(_parsed(hist_copy.unionByName(daily))).where("dq_reason IS NULL")
    out = sv.dedupe_latest(good).collect()
    assert len(out) == 1 and out[0].source_file == DAILY


def test_big_moves_growth_only(spark):
    rows = [
        (1, date(2025, 1, 1), Decimal("10"), "Growth"),
        (1, date(2025, 1, 2), Decimal("10.5"), "Growth"),
        (1, date(2025, 1, 3), Decimal("13.0"), "Growth"),  # +23.8%: warning
        (2, date(2025, 1, 1), Decimal("10"), "IDCW"),
        (2, date(2025, 1, 2), Decimal("5"), "IDCW"),  # payout drop: not checked
    ]
    nav = spark.createDataFrame(
        rows, "scheme_code int, nav_date date, nav decimal(20,6), option string"
    )
    out = sv.big_moves(nav, 0.20).collect()
    assert [(r.scheme_code, r.nav_date, r.rule) for r in out] == [
        (1, date(2025, 1, 3), "growth_move_gt_20pct")
    ]


@pytest.mark.parametrize(
    "option_raw, name, expected",
    [
        ("Direct", "HSBC Liquid Fund - Growth Direct", "Growth"),  # Option column says "Direct"
        ("Growth", "ICICI Prudential Dividend Yield Equity Fund - Growth", "Growth"),
        (None, "UTI Dividend Yield Fund - Direct Plan - Growth", "Growth"),
        (None, "Axis Growth Opportunities Fund - IDCW", "IDCW"),
        ("Payout of Income Distribution cum capital withdrawal option", "X Fund", "IDCW"),
        (None, "BANDHAN LIQUID Fund - Unclaimed Redemption", "Unknown"),
        ("Bonus Option", "Y Fund", "Bonus"),
    ],
)
def test_option_rules(spark, option_raw, name, expected):
    df = spark.createDataFrame([(option_raw, name)], "o string, n string")
    assert df.select(sv.derive_option(df.o, df.n).alias("x")).first().x == expected


@pytest.mark.parametrize(
    "plan_raw, name, expected",
    [
        ("Direct Plan", "A", "Direct"),
        (None, "B Fund - Regular Plan - Growth", "Regular"),
        ("Growth", "C Fund - Direct - Growth", "Direct"),  # Plan column without a plan word
        (None, "D Fund - Growth", "Unknown"),
    ],
)
def test_plan_rules(spark, plan_raw, name, expected):
    df = spark.createDataFrame([(plan_raw, name)], "p string, n string")
    assert df.select(sv.derive_plan(df.p, df.n).alias("x")).first().x == expected
