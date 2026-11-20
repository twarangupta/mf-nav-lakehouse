"""Run SQL statements from a file on the Databricks SQL warehouse and print the results.

Statements are separated by a line containing only ";;". Each result is printed as a
small pipe-separated table (first 200 rows), handy for checks and investigations.

    python scripts/run_sql.py checks.sql

Reads DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_WAREHOUSE_ID from the
environment (.env).
"""

from __future__ import annotations

import os
import sys
import time

import requests

MAX_ROWS = 200


def run(statement: str) -> dict:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    headers = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    body = {
        "warehouse_id": os.environ["DATABRICKS_WAREHOUSE_ID"],
        "statement": statement,
        "wait_timeout": "50s",
    }
    res = requests.post(
        f"{host}/api/2.0/sql/statements", headers=headers, json=body, timeout=120
    ).json()
    while res.get("status", {}).get("state") in {"PENDING", "RUNNING"}:
        time.sleep(3)
        res = requests.get(
            f"{host}/api/2.0/sql/statements/{res['statement_id']}", headers=headers, timeout=60
        ).json()
    return res


def split_statements(text: str) -> list[str]:
    parts, current = [], []
    for line in text.splitlines():
        if line.strip() == ";;":
            parts.append("\n".join(current))
            current = []
        else:
            current.append(line)
    parts.append("\n".join(current))
    return [p.strip() for p in parts if p.strip()]


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    text = open(sys.argv[1], encoding="utf-8").read()
    for stmt in split_statements(text):
        res = run(stmt)
        status = res.get("status", {})
        print(
            f"\n### {stmt.splitlines()[0][:100]}\n{status.get('state')} {status.get('error', '')}"
        )
        cols = [c["name"] for c in res.get("manifest", {}).get("schema", {}).get("columns", [])]
        if cols:
            print(" | ".join(cols))
        for row in ((res.get("result") or {}).get("data_array") or [])[:MAX_ROWS]:
            print(" | ".join("" if v is None else str(v) for v in row))


if __name__ == "__main__":
    main()
