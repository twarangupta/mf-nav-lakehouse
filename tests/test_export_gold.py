"""Unit tests for the Gold export helpers (no Databricks, no pyarrow needed)."""

import importlib.util
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "export_gold", Path(__file__).resolve().parents[1] / "scripts" / "export_gold.py"
)
ex = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ex)


def test_manifest_totals_and_credit():
    entries = [
        {"table": "mf_nav.gold.kpis", "file": "kpis.parquet", "rows": 1, "bytes": 100},
        {
            "table": "mf_nav.gold.fund_metrics",
            "file": "fund_metrics.parquet",
            "rows": 9,
            "bytes": 900,
        },
    ]
    m = ex.build_manifest(entries, "2026-11-15", "2026-11-18T10:00:00Z")
    assert m["total_bytes"] == 1000
    assert m["as_of_date"] == "2026-11-15"
    assert "AMFI" in m["credit"]
    assert [t["file"] for t in m["tables"]] == ["kpis.parquet", "fund_metrics.parquet"]


def test_size_guard():
    ex.check_size({"total_bytes": 5_000_000})
    with pytest.raises(ValueError, match="limit"):
        ex.check_size({"total_bytes": 50_000_000})


def test_only_small_tables_are_exported():
    assert "rolling_return_1y" not in ex.TABLES  # the 3M-row daily series stays in Databricks
    assert set(ex.TABLES) == {
        "kpis", "fund_metrics", "category_summary", "rolling_return_1y_monthly", "dq_daily"
    }  # fmt: skip


def test_sha256_file(tmp_path):
    f = tmp_path / "x.bin"
    f.write_bytes(b"abc")
    assert ex.sha256_file(f).startswith("ba7816bf")


def test_timestamps_lose_zone_label_but_keep_utc_value():
    pa = pytest.importorskip("pyarrow")
    t = pa.table({"computed_at": pa.array([0], type=pa.timestamp("us", tz="Etc/UTC"))})
    out = ex.naive_utc_timestamps(t)
    assert out.schema.field("computed_at").type == pa.timestamp("us")
    assert out.column(0).cast(pa.int64())[0].as_py() == 0
