"""Phase 5: benchmark table layouts for silver.nav on the SQL warehouse.

Creates four copies of mf_nav.silver.nav in mf_nav.bench (no layout, partitioned by
year, OPTIMIZE + ZORDER BY scheme_code, liquid clustering on scheme_code and
nav_date), runs the same queries on each bypassing the result cache, and reads files
read, bytes read and time from the query history API. Prints a Markdown table and
writes benchmarks/layout_results.csv.

    python scripts/benchmark_layouts.py            # build tables + run
    python scripts/benchmark_layouts.py --runs 3 --skip-build

Reads DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_WAREHOUSE_ID (.env).
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics
import time
from pathlib import Path

import requests

SRC = "mf_nav.silver.nav"
DIM = "mf_nav.silver.dim_scheme"
BENCH = "mf_nav.bench"

LAYOUTS = {
    # silver.nav exactly as the MERGE left it (many files), then a plain OPTIMIZE copy
    "as_merged": [
        f"CREATE OR REPLACE TABLE {BENCH}.nav_as_merged SHALLOW CLONE {SRC}",
    ],
    "optimized": [
        f"CREATE OR REPLACE TABLE {BENCH}.nav_optimized DEEP CLONE {SRC}",
        f"OPTIMIZE {BENCH}.nav_optimized",
    ],
    "baseline": [f"CREATE OR REPLACE TABLE {BENCH}.nav_baseline AS SELECT * FROM {SRC}"],
    "partition_year": [
        f"CREATE OR REPLACE TABLE {BENCH}.nav_partition_year PARTITIONED BY (nav_year) AS "
        f"SELECT *, year(nav_date) AS nav_year FROM {SRC}"
    ],
    "zorder_scheme": [
        f"CREATE OR REPLACE TABLE {BENCH}.nav_zorder_scheme AS SELECT * FROM {SRC}",
        f"OPTIMIZE {BENCH}.nav_zorder_scheme ZORDER BY (scheme_code)",
    ],
    "liquid_scheme_date": [
        f"CREATE OR REPLACE TABLE {BENCH}.nav_liquid_scheme_date "
        f"CLUSTER BY (scheme_code, nav_date) AS SELECT * FROM {SRC}",
        f"OPTIMIZE {BENCH}.nav_liquid_scheme_date",
    ],
}

QUERIES = {
    "one_scheme_history": (
        "SELECT nav_date, nav FROM {t} WHERE scheme_code = 122639 AND nav > -{nonce} "
        "ORDER BY nav_date"
    ),
    "one_category_one_year": (
        "SELECT n.scheme_code, avg(n.nav) AS avg_nav, count(*) AS days FROM {t} n "
        f"JOIN {DIM} d ON n.scheme_code = d.scheme_code "
        "WHERE d.category LIKE '%Flexi Cap%' AND n.nav_date BETWEEN '2025-01-01' AND "
        "'2025-12-31' AND n.nav > -{nonce} GROUP BY n.scheme_code"
    ),
    "full_scan_by_year": (
        "SELECT year(nav_date) AS y, count(*) AS n FROM {t} WHERE nav > -{nonce} GROUP BY 1"
    ),
}


def _api(method: str, path: str, **kw) -> dict:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    headers = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    r = requests.request(method, f"{host}{path}", headers=headers, timeout=300, **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:300]}")
    return r.json()


def sql(statement: str) -> dict:
    res = _api(
        "POST",
        "/api/2.0/sql/statements",
        json={
            "warehouse_id": os.environ["DATABRICKS_WAREHOUSE_ID"],
            "statement": statement,
            "wait_timeout": "50s",
        },
    )
    while res["status"]["state"] in {"PENDING", "RUNNING"}:
        time.sleep(2)
        res = _api("GET", f"/api/2.0/sql/statements/{res['statement_id']}")
    if res["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"failed: {statement[:80]} -> {res['status']}")
    return res


def metrics(statement_id: str) -> dict:
    """Files, bytes and time for one statement from the query history API."""
    for _ in range(30):
        res = _api(
            "GET",
            "/api/2.0/sql/history/queries",
            json={
                "filter_by": {"statement_ids": [statement_id]},
                "include_metrics": True,
                "max_results": 1,
            },
        )
        rows = res.get("res", [])
        if rows and rows[0].get("metrics", {}).get("read_bytes") is not None:
            m = rows[0]["metrics"]
            return {
                "total_ms": m.get("total_time_ms"),
                "execution_ms": m.get("execution_time_ms"),
                "read_files": m.get("read_files_count"),
                "pruned_files": m.get("pruned_files_count"),
                "read_bytes": m.get("read_bytes"),
                "rows_read": m.get("rows_read_count"),
                "from_cache": int(bool(m.get("result_from_cache"))),
            }
        time.sleep(3)
    raise RuntimeError(f"no metrics for {statement_id}")


def table_detail(table: str) -> dict:
    res = sql(f"DESCRIBE DETAIL {table}")
    cols = [c["name"] for c in res["manifest"]["schema"]["columns"]]
    d = dict(zip(cols, res["result"]["data_array"][0], strict=True))
    return {"num_files": int(d["numFiles"]), "size_bytes": int(d["sizeInBytes"])}


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark silver.nav layouts")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()

    sql(f"CREATE SCHEMA IF NOT EXISTS {BENCH}")
    if not args.skip_build:
        for name, statements in LAYOUTS.items():
            began = time.time()
            for s in statements:
                sql(s)
            print(f"built {name} in {time.time() - began:.0f}s")

    rows = []
    for name in LAYOUTS:
        table = f"{BENCH}.nav_{name}"
        detail = table_detail(table)
        for qname, template in QUERIES.items():
            runs = []
            for _ in range(args.runs):
                # The result cache matches on the optimised plan, so a comment or a constant
                # like "123 > 0" is ignored. "nav > -<unique number>" is always true but uses
                # a column, so every run is a new plan and really reads the table.
                stmt = template.format(t=table, nonce=time.time_ns() % 1_000_000_000 + 1)
                runs.append(metrics(sql(stmt)["statement_id"]))
            med = {k: statistics.median([r[k] or 0 for r in runs]) for k in runs[0]}
            rows.append({"layout": name, "query": qname, **detail, **med})
            print(name, qname, med)

    out = Path("benchmarks")
    out.mkdir(exist_ok=True)
    with (out / "layout_results.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print("\n| Layout | Query | Table files | Files read | Files pruned | MB read | Time ms |")
    print("|---|---|---|---|---|---|---|")
    for r in rows:
        print(
            f"| {r['layout']} | {r['query']} | {r['num_files']} | {r['read_files']:.0f} | "
            f"{r['pruned_files']:.0f} | {r['read_bytes'] / 1e6:.1f} | {r['total_ms']:.0f} |"
        )


if __name__ == "__main__":
    main()
