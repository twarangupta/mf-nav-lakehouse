"""Public Streamlit dashboard for the Mutual Fund NAV Lakehouse project.

Reads the small Gold exports in exports/gold/ (written by scripts/export_gold.py),
so it needs no Databricks workspace and no credentials, and it keeps working when
the free workspace is asleep. Tiles mirror the Databricks AI/BI dashboard.

    streamlit run dashboards/streamlit/app.py
"""

from __future__ import annotations

import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

GOLD = Path(__file__).resolve().parents[2] / "exports" / "gold"
REPO_URL = "https://github.com/twarangupta/mf-nav-lakehouse"
DEFAULT_CATEGORY = "Equity - Flexi Cap"
DEFAULT_FUNDS = ["ITI Flexi Cap Fund", "BANK OF INDIA FLEXI CAP FUND", "HDFC Flexi Cap Fund"]

st.set_page_config(page_title="Indian Mutual Funds (AMFI NAV)", page_icon="📈", layout="wide")


@st.cache_data
def load(name: str) -> pd.DataFrame:
    df = pd.read_parquet(GOLD / f"{name}.parquet")
    # NAVs are exact decimals in Delta; floats are fine for charts.
    for col in df.columns:
        if df[col].dtype == object and len(df) and type(df[col].iloc[0]).__name__ == "Decimal":
            df[col] = df[col].astype(float)
    return df


@st.cache_data
def manifest() -> dict:
    path = GOLD / "manifest.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def pct(series: pd.Series) -> pd.Series:
    return (series * 100).round(2)


def kpi_row(kpis: pd.Series) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Active Growth schemes", f"{int(kpis.active_growth_schemes):,}")
    c2.metric("Schemes with a 3-year CAGR", f"{int(kpis.schemes_with_3y_cagr):,}")
    c3.metric("Clean NAV records", f"{int(kpis.silver_nav_rows):,}")
    c4.metric("Latest NAV date", str(kpis.as_of_date))


def top10(metrics: pd.DataFrame) -> None:
    st.markdown("#### Top 10 funds by 3-year CAGR")
    active = metrics[metrics.is_active & metrics.rank_cagr_3y_in_category.notna()]
    categories = sorted(active.category_group.unique())
    left, right = st.columns([3, 1])
    category = left.selectbox(
        "Category",
        categories,
        index=categories.index(DEFAULT_CATEGORY) if DEFAULT_CATEGORY in categories else 0,
    )
    plan = right.selectbox("Plan", ["Direct", "Regular"])
    top = (
        active[
            (active.category_group == category)
            & (active.plan == plan)
            & (active.rank_cagr_3y_in_category <= 10)
        ]
        .sort_values("rank_cagr_3y_in_category")
        .assign(
            rank=lambda d: d.rank_cagr_3y_in_category.astype(int),
            cagr_3y=lambda d: pct(d.cagr_3y),
            cagr_5y=lambda d: pct(d.cagr_5y),
            return_1y=lambda d: pct(d.return_1y),
            volatility_1y=lambda d: pct(d.volatility_1y),
            max_drawdown_3y=lambda d: pct(d.max_drawdown_3y),
        )
    )
    st.dataframe(
        top[
            [
                "rank", "scheme_name", "fund_house", "cagr_3y", "cagr_5y", "return_1y",
                "volatility_1y", "max_drawdown_3y", "latest_nav", "latest_nav_date",
            ]
        ].rename(
            columns={
                "rank": "Rank", "scheme_name": "Fund", "fund_house": "Fund house",
                "cagr_3y": "3Y CAGR %", "cagr_5y": "5Y CAGR %", "return_1y": "1Y return %",
                "volatility_1y": "Volatility 1Y %", "max_drawdown_3y": "Max drawdown 3Y %",
                "latest_nav": "Latest NAV", "latest_nav_date": "NAV date",
            }
        ),
        hide_index=True,
        width="stretch",
    )  # fmt: skip
    st.caption(
        "Ranked within the category and plan among active funds with 3 years of history. "
        "Direct and Regular are ranked separately because Regular plans carry distributor fees."
    )


def rolling_chart(metrics: pd.DataFrame, rolling: pd.DataFrame) -> None:
    st.markdown("#### Rolling 1-year return (month end)")
    names = metrics[metrics.is_active][["scheme_code", "scheme_name", "plan"]].assign(
        fund=lambda d: d.scheme_name + " (" + d.plan + ")"
    )
    data = rolling.merge(names, on="scheme_code")
    options = sorted(data.fund.unique())
    defaults = [f"{n} (Direct)" for n in DEFAULT_FUNDS if f"{n} (Direct)" in options]
    chosen = st.multiselect("Funds to compare", options, default=defaults)
    shown = data[data.fund.isin(chosen)].assign(rolling_1y=lambda d: pct(d.return_1y))
    st.altair_chart(
        alt.Chart(shown)
        .mark_line(point=False)
        .encode(
            x=alt.X("month:T", title=None),
            y=alt.Y("rolling_1y:Q", title="Rolling 1Y return %"),
            color=alt.Color("fund:N", title="Fund"),
            tooltip=["fund", alt.Tooltip("month:T", format="%b %Y"), "rolling_1y"],
        )
        .properties(height=340),
        width="stretch",
    )
    st.caption(
        "Each point compares the NAV at a month end with the last NAV on or before the same "
        "day a year earlier. Points whose window contains a NAV reset (a unit split) are left out."
    )


def risk_return(metrics: pd.DataFrame) -> None:
    st.markdown("#### Risk vs return (each dot is a fund)")
    data = metrics[
        metrics.is_active & metrics.cagr_3y.notna() & metrics.volatility_1y.notna()
    ].assign(
        asset_class=lambda d: d.category_group.str.split(" - ").str[0],
        cagr_3y_pct=lambda d: pct(d.cagr_3y),
        volatility_pct=lambda d: pct(d.volatility_1y),
    )
    st.altair_chart(
        alt.Chart(data)
        .mark_circle(size=40, opacity=0.6)
        .encode(
            x=alt.X("volatility_pct:Q", title="Volatility 1Y %"),
            y=alt.Y("cagr_3y_pct:Q", title="3Y CAGR %"),
            color=alt.Color("asset_class:N", title="Asset class"),
            tooltip=["scheme_name", "plan", "category_group", "cagr_3y_pct", "volatility_pct"],
        )
        .properties(height=380),
        width="stretch",
    )
    st.caption(
        "Debt funds cluster at low volatility and modest returns; equity spreads to the right."
    )


def drawdown_chart(summary: pd.DataFrame) -> None:
    st.markdown("#### Median worst fall over 3 years, by category (Direct plans)")
    data = summary[(summary.plan == "Direct") & (summary.schemes_with_3y >= 5)].assign(
        drawdown_pct=lambda d: pct(d.median_max_drawdown_3y)
    )
    st.altair_chart(
        alt.Chart(data)
        .mark_bar(color="#d9534f")
        .encode(
            x=alt.X("category_group:N", sort="y", title=None),
            y=alt.Y("drawdown_pct:Q", title="Median max drawdown 3Y %"),
            tooltip=["category_group", "drawdown_pct", "schemes_with_3y"],
        )
        .properties(height=360),
        width="stretch",
    )
    st.caption("Categories with at least 5 funds that have 3 years of history.")


def quality_chart(dq: pd.DataFrame) -> None:
    st.markdown("#### Data quality per month")
    monthly = (
        dq.assign(month=pd.to_datetime(dq.load_date).dt.to_period("M").dt.to_timestamp())
        .groupby("month", as_index=False)[["quarantined_counted_rows", "quarantined_known_rows"]]
        .sum()
        .rename(
            columns={
                "quarantined_counted_rows": "Quarantined (bad rows)",
                "quarantined_known_rows": "Known exclusions (zero NAV, segregated)",
            }
        )
        .melt("month", var_name="series", value_name="rows")
    )
    st.altair_chart(
        alt.Chart(monthly)
        .mark_line()
        .encode(
            x=alt.X("month:T", title=None),
            y=alt.Y("rows:Q", title="Rows"),
            color=alt.Color("series:N", title=None),
            tooltip=[alt.Tooltip("month:T", format="%b %Y"), "series", "rows"],
        )
        .properties(height=300),
        width="stretch",
    )
    st.caption(
        "Rows that broke a hard rule are quarantined with a reason; a file with more than 1% bad "
        "rows is rejected before Gold. Zero NAVs of segregated portfolios are expected and "
        "recorded separately."
    )


def main() -> None:
    st.title("📈 Indian Mutual Funds: NAV lakehouse results")
    st.write(
        "Built by **Twaran Gupta**. Daily AMFI NAV files land in AWS S3 and flow through "
        "Databricks Bronze (raw lines), Silver (clean, validated NAVs) and Gold (fund metrics) "
        "with PySpark and Delta Lake. Returns use Growth plans only; 3Y and 5Y are CAGR. "
        "This app reads small exported Gold files, so it needs no credentials."
    )
    kpis = load("kpis").iloc[0]
    metrics = load("fund_metrics")
    kpi_row(kpis)
    top10(metrics)
    rolling_chart(metrics, load("rolling_return_1y_monthly"))
    risk_return(metrics)
    left, right = st.columns(2)
    with left:
        drawdown_chart(load("category_summary"))
    with right:
        quality_chart(load("dq_daily"))

    m = manifest()
    st.divider()
    st.caption(
        f"{m.get('credit', 'Data: AMFI')} · Data as of {m.get('as_of_date', 'n/a')} · "
        f"Exported {m.get('exported_at_utc', 'n/a')} · [Pipeline code]({REPO_URL})"
    )


main()
