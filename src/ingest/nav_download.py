"""Download AMFI's daily NAVAll.txt and land it in S3, byte for byte.

The file is saved exactly as received: no line is removed or changed, including the
category and fund house header lines, because Bronze keeps the raw lines and Silver
needs the file order. Airflow calls `land_daily_nav` in Phase 6; for now it can be
run by hand:

    python -m src.ingest.nav_download --date 2026-11-11

Landing path: s3://<MF_RAW_BUCKET>/raw/nav/date=YYYY-MM-DD/NAVAll.txt, where the date
is the load date (the day we downloaded it, in India time), not the NAV date inside.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import requests

NAV_URL = "https://www.amfiindia.com/spages/NAVAll.txt"
EXPECTED_HEADER = b"Scheme Code;"
MIN_BYTES = 500_000  # a real file is about 1.5 MB; anything far smaller is an error page
USER_AGENT = "mf-nav-lakehouse/0.1 (portfolio data pipeline)"
IST = timezone(timedelta(hours=5, minutes=30), "IST")  # India has no daylight saving


class InvalidNavFile(ValueError):
    """Raised when the downloaded bytes are not a usable NAVAll.txt."""


@dataclass(frozen=True)
class LandingResult:
    key: str
    sha256: str
    size_bytes: int
    line_count: int
    last_modified: str
    uploaded: bool  # False when the same bytes were already there (idempotent rerun)


def raw_key(load_date: date) -> str:
    """S3 key for one day's raw file, Hive-style date folder."""
    return f"raw/nav/date={load_date.isoformat()}/NAVAll.txt"


def validate_nav_bytes(content: bytes) -> None:
    """Fail fast if this is not the real file (e.g. an HTML error or redirect page)."""
    if len(content) < MIN_BYTES:
        raise InvalidNavFile(f"file too small: {len(content)} bytes (expected about 1.5 MB)")
    if not content.startswith(EXPECTED_HEADER):
        raise InvalidNavFile(f"unexpected first bytes: {content[:40]!r}")
    if b";" not in content[len(EXPECTED_HEADER) :]:
        raise InvalidNavFile("no semicolon-separated data rows found")


def sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def fetch_nav(url: str = NAV_URL, timeout: int = 60) -> tuple[bytes, str]:
    """Download the file (following AMFI's 302 redirect). Returns (bytes, Last-Modified)."""
    response = requests.get(
        url, headers={"User-Agent": USER_AGENT}, timeout=timeout, allow_redirects=True
    )
    response.raise_for_status()
    return response.content, response.headers.get("Last-Modified", "")


def upload_raw(s3, bucket: str, key: str, content: bytes, metadata: dict[str, str]) -> bool:
    """Put the bytes at `key` unless an object with the same SHA-256 is already there.

    Returns True if it uploaded, False if it skipped. Never deletes anything (the
    uploader's IAM policy has no delete permission, on purpose).
    """
    try:
        existing = s3.head_object(Bucket=bucket, Key=key)
        if existing.get("Metadata", {}).get("sha256") == metadata["sha256"]:
            return False
    except s3.exceptions.ClientError as err:
        # A missing key gives 404, or 403 when ListBucket does not cover the exact key.
        # Either way we try the upload; a real permission problem fails there loudly.
        code = err.response.get("Error", {}).get("Code")
        if code not in {"404", "NoSuchKey", "NotFound", "403", "Forbidden"}:
            raise
    s3.put_object(Bucket=bucket, Key=key, Body=content, ContentType="text/plain", Metadata=metadata)
    return True


def land_daily_nav(s3, bucket: str, load_date: date, url: str = NAV_URL) -> LandingResult:
    """Download, validate and land one day's NAVAll.txt. Safe to rerun."""
    content, last_modified = fetch_nav(url)
    validate_nav_bytes(content)
    digest = sha256_hex(content)
    key = raw_key(load_date)
    metadata = {"sha256": digest, "source-url": url, "source-last-modified": last_modified}
    uploaded = upload_raw(s3, bucket, key, content, metadata)
    return LandingResult(
        key=key,
        sha256=digest,
        size_bytes=len(content),
        line_count=content.count(b"\n"),
        last_modified=last_modified,
        uploaded=uploaded,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--date", help="load date YYYY-MM-DD (default: today in India time)")
    args = parser.parse_args()

    import boto3  # imported here so unit tests do not need AWS libraries loaded

    load_date = date.fromisoformat(args.date) if args.date else datetime.now(IST).date()
    bucket = os.environ["MF_RAW_BUCKET"]
    result = land_daily_nav(boto3.client("s3"), bucket, load_date)
    action = "uploaded" if result.uploaded else "already present, skipped"
    print(
        f"{action}: s3://{bucket}/{result.key} | {result.size_bytes} bytes | "
        f"{result.line_count} lines | sha256 {result.sha256[:12]} | "
        f"Last-Modified {result.last_modified}"
    )


if __name__ == "__main__":
    main()
