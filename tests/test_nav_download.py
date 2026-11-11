"""Unit tests for the downloader. No network and no AWS: S3 is a small fake."""

from datetime import date

import pytest

from src.ingest import nav_download as nd

HEADER = (
    b"Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;"
    b"Plan;Option;Net Asset Value;Date\r\n"
)
ROW = (
    b"135762;INF846K01WO1;-;Axis Children's Fund;Direct Plan;Growth Option;30.9434;09-Nov-2026\r\n"
)
REAL_LOOKING = HEADER + b" \r\nOpen Ended Schemes(Children's Fund)\r\n" + ROW * 6000


class FakeClientError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    """Just enough of a boto3 S3 client for upload_raw."""

    class exceptions:  # noqa: N801 (mirrors boto3's client.exceptions)
        ClientError = FakeClientError

    def __init__(self, existing_sha=None, head_error=None):
        self.existing_sha = existing_sha
        self.head_error = head_error
        self.puts = []

    def head_object(self, Bucket, Key):  # noqa: N803 (boto3 argument names)
        if self.head_error:
            raise FakeClientError(self.head_error)
        return {"Metadata": {"sha256": self.existing_sha}}

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


def test_raw_key_uses_hive_style_date_folder():
    assert nd.raw_key(date(2026, 11, 11)) == "raw/nav/date=2026-11-11/NAVAll.txt"


def test_validate_accepts_real_looking_file():
    nd.validate_nav_bytes(REAL_LOOKING)


@pytest.mark.parametrize(
    "content, reason",
    [
        (b"<head><title>Document Moved</title></head>", "too small"),
        (b"<html>" + b"x" * 600_000, "unexpected first bytes"),
    ],
)
def test_validate_rejects_error_pages(content, reason):
    with pytest.raises(nd.InvalidNavFile, match=reason):
        nd.validate_nav_bytes(content)


def test_upload_when_object_missing():
    s3 = FakeS3(head_error="404")
    assert nd.upload_raw(s3, "b", "k", b"data", {"sha256": "abc"}) is True
    assert s3.puts[0]["Body"] == b"data"
    assert s3.puts[0]["Metadata"] == {"sha256": "abc"}


def test_upload_when_head_is_forbidden_for_missing_key():
    s3 = FakeS3(head_error="403")
    assert nd.upload_raw(s3, "b", "k", b"data", {"sha256": "abc"}) is True


def test_skip_when_same_bytes_already_landed():
    s3 = FakeS3(existing_sha="abc")
    assert nd.upload_raw(s3, "b", "k", b"data", {"sha256": "abc"}) is False
    assert s3.puts == []


def test_overwrite_when_content_changed():
    s3 = FakeS3(existing_sha="old")
    assert nd.upload_raw(s3, "b", "k", b"data", {"sha256": "new"}) is True


def test_other_errors_are_raised():
    with pytest.raises(FakeClientError):
        nd.upload_raw(FakeS3(head_error="500"), "b", "k", b"data", {"sha256": "abc"})


def test_land_daily_nav_saves_bytes_unchanged(monkeypatch):
    monkeypatch.setattr(nd, "fetch_nav", lambda url: (REAL_LOOKING, "Wed, 11 Nov 2026"))
    s3 = FakeS3(head_error="404")
    result = nd.land_daily_nav(s3, "bucket", date(2026, 11, 11))
    assert s3.puts[0]["Body"] == REAL_LOOKING  # byte for byte, CRLF and headers kept
    assert result.key == "raw/nav/date=2026-11-11/NAVAll.txt"
    assert result.sha256 == nd.sha256_hex(REAL_LOOKING)
    assert result.uploaded is True


def test_validate_rejects_non_utf8_bytes():
    cp1252_apostrophe = HEADER + b"Open Ended Schemes(Children\x92s Fund)\r\n" + ROW * 6000
    with pytest.raises(nd.InvalidNavFile, match="not valid UTF-8"):
        nd.validate_nav_bytes(cp1252_apostrophe)


def test_validate_accepts_utf8_curly_apostrophe():
    nd.validate_nav_bytes(HEADER + "Children’s Fund\r\n".encode() + ROW * 6000)
