"""Export the small Gold tables to exports/gold/*.parquet plus a manifest.

These files feed the public Streamlit dashboard (decision D38): it reads them from
GitHub, so it needs no Databricks or AWS credentials. Only summary tables are
exported (a few MB in total); the 3-million-row daily rolling series stays in
Databricks.

    python scripts/export_gold.py                 # writes exports/gold/
    python scripts/export_gold.py --out some/dir   # elsewhere (e.g. for a test)

Reads DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_WAREHOUSE_ID from the
environment (.env). Results come from the SQL Statement API as Arrow streams.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import requests

GOLD = "mf_nav.gold"
# table -> columns to sort by, so files are byte-stable when the data has not changed
TABLES = {
    "kpis": ["as_of_date"],
    "fund_metrics": ["scheme_code"],
    "category_summary": ["category_group", "plan"],
    "rolling_return_1y_monthly": ["scheme_code", "month"],
    "dq_daily": ["load_date", "source_kind"],
}
DROP_COLUMNS = {"run_id"}  # internal run identifiers are not useful to the public app
MAX_TOTAL_BYTES = 10_000_000  # keep the repo light; fail loudly if exports grow
CREDIT = "Data: AMFI (Association of Mutual Funds in India), www.amfiindia.com"


def _api(method: str, path: str, **kwargs) -> dict:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    headers = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    r = requests.request(method, f"{host}{path}", headers=headers, timeout=120, **kwargs)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:300]}")
    return r.json()


def run_sql(statement: str, arrow: bool = False) -> dict:
    """Run one statement and wait for it. Arrow results come back as external links."""
    body = {
        "warehouse_id": os.environ["DATABRICKS_WAREHOUSE_ID"],
        "statement": statement,
        "wait_timeout": "30s",
    }
    if arrow:
        body.update({"disposition": "EXTERNAL_LINKS", "format": "ARROW_STREAM"})
    res = _api("POST", "/api/2.0/sql/statements", json=body)
    while res["status"]["state"] in {"PENDING", "RUNNING"}:
        time.sleep(2)
        res = _api("GET", f"/api/2.0/sql/statements/{res['statement_id']}")
    if res["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"SQL failed: {res['status']}")
    return res


def fetch_arrow(table: str, order_by: list[str]):
    """SELECT * as a pyarrow Table, sorted for stable output."""
    import pyarrow as pa
    import pyarrow.ipc as ipc

    res = run_sql(f"SELECT * FROM {GOLD}.{table} ORDER BY {', '.join(order_by)}", arrow=True)
    chunks = []
    for i in range(res["manifest"].get("total_chunk_count", 0)):
        links = _api("GET", f"/api/2.0/sql/statements/{res['statement_id']}/result/chunks/{i}")
        for link in links.get("external_links", []):
            # pre-signed cloud URL: must be fetched WITHOUT the Databricks token
            data = requests.get(link["external_link"], timeout=120).content
            chunks.append(ipc.open_stream(io.BytesIO(data)).read_all())
    if not chunks:
        raise RuntimeError(f"{GOLD}.{table} returned no data")
    t = pa.concat_tables(chunks)
    t = t.drop_columns([c for c in t.column_names if c in DROP_COLUMNS])
    return naive_utc_timestamps(t)


def naive_utc_timestamps(t):
    """Store timestamps as plain UTC (no zone label).

    Databricks returns TIMESTAMP columns tagged "Etc/UTC"; reading a zone-tagged value
    needs a time-zone database, which Windows Python lacks. The values are already UTC.
    """
    import pyarrow as pa

    for i, field in enumerate(t.schema):
        if pa.types.is_timestamp(field.type) and field.type.tz is not None:
            t = t.set_column(i, field.name, t.column(i).cast(pa.timestamp(field.type.unit)))
    return t


def table_version(table: str) -> int:
    res = run_sql(f"SELECT max(version) FROM (DESCRIBE HISTORY {GOLD}.{table})")
    return int(res["result"]["data_array"][0][0])


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_manifest(entries: list[dict], as_of_date: str, exported_at: str) -> dict:
    return {
        "dataset": "mf-nav-lakehouse gold exports",
        "credit": CREDIT,
        "as_of_date": as_of_date,
        "exported_at_utc": exported_at,
        "total_bytes": sum(e["bytes"] for e in entries),
        "tables": entries,
    }


def check_size(manifest: dict, limit: int = MAX_TOTAL_BYTES) -> None:
    if manifest["total_bytes"] > limit:
        raise ValueError(
            f"exports total {manifest['total_bytes']} bytes, above the {limit}-byte limit; "
            "aggregate more before exporting"
        )


def main() -> None:
    import pyarrow.parquet as pq

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="exports/gold")
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    entries, as_of = [], ""
    for table, order_by in TABLES.items():
        data = fetch_arrow(table, order_by)
        path = out / f"{table}.parquet"
        pq.write_table(data, path, compression="zstd")
        if table == "kpis":
            as_of = str(data.column("as_of_date")[0].as_py())
        entries.append(
            {
                "table": f"{GOLD}.{table}",
                "file": path.name,
                "delta_version": table_version(table),
                "rows": data.num_rows,
                "columns": data.column_names,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
        print(f"{table}: {data.num_rows} rows, {path.stat().st_size} bytes")

    exported_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest = build_manifest(entries, as_of, exported_at)
    check_size(manifest)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"manifest: {len(entries)} tables, {manifest['total_bytes']} bytes, as of {as_of}")


if __name__ == "__main__":
    main()
