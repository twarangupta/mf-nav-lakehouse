"""Unit tests for the history backfill. No network, no AWS: fetch and S3 are fakes."""

from datetime import date

import pytest
import requests

from src.ingest import nav_history as nh

HEADER = (
    b"Scheme Code;NAV Name;Plan;Option;ISIN Div Payout/ISIN Growth;"
    b"ISIN Div Reinvestment;Net Asset Value;Date\r\n"
)
ROW = b"139619;Taurus Investor Education Pool - Growth;;;;;10.0000;03-Nov-2025\r\n"
CHUNK = (
    HEADER + b"\r\nOpen Ended Schemes ( Money Market )\r\n\r\nTaurus Mutual Fund\r\n" + ROW * 9000
)


class FakeClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    class exceptions:  # noqa: N801 (mirrors boto3)
        ClientError = FakeClientError

    def __init__(self, existing=()):
        self.objects = {k: b"" for k in existing}

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise FakeClientError("404")
        return {}

    def put_object(self, Bucket, Key, Body, ContentType, Metadata):  # noqa: N803
        self.objects[Key] = Body
        self.last_metadata = Metadata


def test_chunks_cover_the_range_without_gaps_or_overlap():
    ranges = nh.chunk_ranges(date(2021, 1, 1), date(2021, 1, 20))
    assert ranges == [
        (date(2021, 1, 1), date(2021, 1, 7)),
        (date(2021, 1, 8), date(2021, 1, 14)),
        (date(2021, 1, 15), date(2021, 1, 20)),  # last chunk is shorter
    ]


def test_key_and_url():
    frm, to = date(2025, 11, 3), date(2025, 11, 9)
    assert nh.history_key(frm, to) == (
        "raw/nav_history/date=2025-11-03/NAVHistory_2025-11-03_2025-11-09.txt"
    )
    assert nh.history_url(frm, to).endswith("frmdt=03-Nov-2025&todt=09-Nov-2025")


def test_count_data_rows_ignores_headers_and_blank_lines():
    assert nh.count_data_rows(CHUNK) == 9000


def test_backfill_uploads_missing_and_skips_existing_without_downloading():
    already = nh.history_key(date(2021, 1, 1), date(2021, 1, 7))
    s3 = FakeS3(existing=[already])
    fetched = []

    def fetch(frm, to):
        fetched.append(frm)
        return CHUNK

    results = nh.backfill(
        s3, "b", date(2021, 1, 1), date(2021, 1, 14), fetch=fetch, sleep=lambda s: None
    )
    assert [r.status for r in results] == ["skipped", "uploaded"]
    assert fetched == [date(2021, 1, 8)]  # the existing chunk was never downloaded
    assert s3.objects[results[1].key] == CHUNK  # byte for byte
    assert s3.last_metadata["sha256"] == nh.sha256_hex(CHUNK)
    assert results[1].data_rows == 9000


def test_backfill_retries_then_succeeds_and_waits_politely():
    calls, waits = [], []

    def flaky(frm, to):
        calls.append(frm)
        if len(calls) < 3:
            raise requests.ConnectionError("AMFI timed out")
        return CHUNK

    results = nh.backfill(
        FakeS3(), "b", date(2021, 1, 1), date(2021, 1, 7), fetch=flaky, sleep=waits.append
    )
    assert results[0].status == "uploaded"
    assert len(calls) == 3
    assert waits == [5, 15, 2.0]  # two retry waits, then the normal pause


def test_backfill_marks_chunk_failed_after_all_retries_and_continues():
    def broken(frm, to):
        if frm == date(2021, 1, 1):
            return b"<html>error</html>"
        return CHUNK

    results = nh.backfill(
        FakeS3(), "b", date(2021, 1, 1), date(2021, 1, 14), fetch=broken, sleep=lambda s: None
    )
    assert [r.status for r in results] == ["failed", "uploaded"]
    assert "InvalidNavFile" in results[0].error


def test_end_before_start_is_rejected():
    with pytest.raises(ValueError):
        nh.chunk_ranges(date(2021, 2, 1), date(2021, 1, 1))
