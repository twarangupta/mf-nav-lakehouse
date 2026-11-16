"""Backfill AMFI NAV history into S3, one 7-day chunk per file. Restartable and polite.

    python -m src.ingest.nav_history --start 2021-01-01 --end 2026-11-15

Each chunk is saved byte for byte to
    s3://<MF_RAW_BUCKET>/raw/nav_history/date=<from>/NAVHistory_<from>_<to>.txt
with its SHA-256 in S3 metadata. The `date=` folder is the chunk's first day, so the
Bronze job treats each chunk like one load.

Restartable: a chunk whose object already exists in S3 is skipped without
downloading, so after a crash or a stop you simply run the same command again.
Polite: one request at a time, a pause between requests, and a few retries with a
growing wait when AMFI is slow or errors. Chunks that still fail are listed at the
end and the command exits non-zero; rerunning fetches only those.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import requests

from src.ingest.nav_download import USER_AGENT, InvalidNavFile, sha256_hex, validate_nav_bytes

HISTORY_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={frm}&todt={to}"
CHUNK_DAYS = 7
RETRY_WAITS = (5, 15, 45)  # seconds to wait before retry 1, 2, 3


@dataclass(frozen=True)
class ChunkResult:
    frm: date
    to: date
    key: str
    status: str  # "uploaded", "skipped" or "failed"
    size_bytes: int = 0
    data_rows: int = 0
    seconds: float = 0.0
    error: str = ""


def chunk_ranges(start: date, end: date, days: int = CHUNK_DAYS) -> list[tuple[date, date]]:
    """Consecutive, non-overlapping [from, to] ranges covering start..end inclusive."""
    if end < start:
        raise ValueError("end is before start")
    ranges = []
    frm = start
    while frm <= end:
        to = min(frm + timedelta(days=days - 1), end)
        ranges.append((frm, to))
        frm = to + timedelta(days=1)
    return ranges


def history_key(frm: date, to: date) -> str:
    return (
        f"raw/nav_history/date={frm.isoformat()}/NAVHistory_{frm.isoformat()}_{to.isoformat()}.txt"
    )


def history_url(frm: date, to: date) -> str:
    return HISTORY_URL.format(frm=frm.strftime("%d-%b-%Y"), to=to.strftime("%d-%b-%Y"))


def fetch_history(frm: date, to: date, timeout: int = 180) -> bytes:
    response = requests.get(
        history_url(frm, to), headers={"User-Agent": USER_AGENT}, timeout=timeout
    )
    response.raise_for_status()
    return response.content


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as err:
        if err.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound", "403"}:
            return False
        raise


def count_data_rows(content: bytes) -> int:
    """Lines with 8 semicolon-separated fields, minus the column header."""
    return max(sum(1 for line in content.splitlines() if line.count(b";") == 7) - 1, 0)


def backfill(
    s3,
    bucket: str,
    start: date,
    end: date,
    fetch: Callable[[date, date], bytes] = fetch_history,
    delay: float = 2.0,
    retry_waits: tuple[float, ...] = RETRY_WAITS,
    sleep: Callable[[float], None] = time.sleep,
    on_result: Callable[[ChunkResult], None] | None = None,
) -> list[ChunkResult]:
    """Download and land every chunk in [start, end] that is not already in S3."""
    results = []
    for frm, to in chunk_ranges(start, end):
        key = history_key(frm, to)
        if object_exists(s3, bucket, key):
            result = ChunkResult(frm, to, key, "skipped")
        else:
            result = _download_one(s3, bucket, frm, to, key, fetch, retry_waits, sleep)
            sleep(delay)  # be polite: pause after every real request
        results.append(result)
        if on_result:
            on_result(result)
    return results


def _download_one(s3, bucket, frm, to, key, fetch, retry_waits, sleep) -> ChunkResult:
    attempts = len(retry_waits) + 1
    last_error = ""
    for attempt in range(attempts):
        began = time.monotonic()
        try:
            content = fetch(frm, to)
            validate_nav_bytes(content)
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=content,
                ContentType="text/plain",
                Metadata={
                    "sha256": sha256_hex(content),
                    "source-url": history_url(frm, to),
                    "chunk-from": frm.isoformat(),
                    "chunk-to": to.isoformat(),
                },
            )
            return ChunkResult(
                frm,
                to,
                key,
                "uploaded",
                size_bytes=len(content),
                data_rows=count_data_rows(content),
                seconds=round(time.monotonic() - began, 1),
            )
        except (requests.RequestException, InvalidNavFile) as err:
            last_error = f"{type(err).__name__}: {err}"[:300]
            if attempt < attempts - 1:
                sleep(retry_waits[attempt])
    return ChunkResult(frm, to, key, "failed", error=last_error)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill AMFI NAV history to S3")
    parser.add_argument("--start", required=True, help="first day, YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="last day, YYYY-MM-DD")
    parser.add_argument("--delay", type=float, default=2.0, help="seconds between requests")
    parser.add_argument("--log", default="data/backfill_log.csv", help="local CSV of results")
    args = parser.parse_args()

    import boto3

    bucket = os.environ["MF_RAW_BUCKET"]
    log_path = Path(args.log)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not log_path.exists()
    began = time.monotonic()

    with log_path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if new_file:
            writer.writerow(["from", "to", "status", "size_bytes", "data_rows", "seconds", "error"])

        def log(r: ChunkResult) -> None:
            writer.writerow([r.frm, r.to, r.status, r.size_bytes, r.data_rows, r.seconds, r.error])
            fh.flush()
            print(
                f"{r.frm} to {r.to}: {r.status} {r.size_bytes} B "
                f"{r.data_rows} rows {r.seconds}s {r.error}",
                flush=True,
            )

        results = backfill(
            boto3.client("s3"),
            bucket,
            date.fromisoformat(args.start),
            date.fromisoformat(args.end),
            delay=args.delay,
            on_result=log,
        )

    done = [r for r in results if r.status == "uploaded"]
    failed = [r for r in results if r.status == "failed"]
    print(
        f"chunks: {len(results)} | uploaded {len(done)} | skipped "
        f"{sum(r.status == 'skipped' for r in results)} | failed {len(failed)} | "
        f"bytes {sum(r.size_bytes for r in done)} | data rows {sum(r.data_rows for r in done)} | "
        f"minutes {(time.monotonic() - began) / 60:.1f}"
    )
    if failed:
        print("failed chunks (rerun the same command to retry):", [str(r.frm) for r in failed])
        sys.exit(1)


if __name__ == "__main__":
    main()
