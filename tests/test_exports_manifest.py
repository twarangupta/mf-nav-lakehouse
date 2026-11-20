"""The committed Gold exports match their manifest (rows, size, SHA-256, credit)."""

import hashlib
import json
from pathlib import Path

import pytest

EXPORTS = Path(__file__).resolve().parents[1] / "exports" / "gold"


def _manifest():
    return json.loads((EXPORTS / "manifest.json").read_text(encoding="utf-8"))


def test_every_manifest_file_exists_with_matching_hash_and_size():
    m = _manifest()
    assert m["credit"].startswith("Data: AMFI")
    assert m["total_bytes"] == sum(t["bytes"] for t in m["tables"])
    for t in m["tables"]:
        data = (EXPORTS / t["file"]).read_bytes()
        assert len(data) == t["bytes"], t["file"]
        assert hashlib.sha256(data).hexdigest() == t["sha256"], t["file"]


def test_row_counts_match_the_parquet_files():
    pq = pytest.importorskip("pyarrow.parquet")
    for t in _manifest()["tables"]:
        assert pq.read_metadata(EXPORTS / t["file"]).num_rows == t["rows"], t["file"]


def test_exports_stay_small():
    assert _manifest()["total_bytes"] < 10_000_000
