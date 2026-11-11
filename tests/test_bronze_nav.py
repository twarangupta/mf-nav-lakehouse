"""Unit tests for the Bronze line explosion, on a local SparkSession (no Delta, no S3)."""

import hashlib

from src.jobs.bronze_nav import explode_lines, only_new_files

PATH = "s3://bucket/raw/nav/date=2026-11-11/NAVAll.txt"
CONTENT = (
    "Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;"
    "Plan;Option;Net Asset Value;Date\r\n"
    " \r\n"
    "Open Ended Schemes(Children’s Fund - Childrens' Fund)\r\n"
    " \r\n"
    "Axis Mutual Fund\r\n"
    "135762;INF846K01WO1;-;Axis Children's Fund;Direct Plan;Growth Option;30.9434;09-Nov-2026\r\n"
).encode()


def _files(spark, path=PATH, content=CONTENT):
    return spark.createDataFrame([(path, bytearray(content))], "path string, content binary")


def test_one_row_per_line_in_file_order(spark):
    rows = explode_lines(_files(spark), "run-1").orderBy("line_number").collect()
    assert [r.line_number for r in rows] == [1, 2, 3, 4, 5, 6]
    assert rows[0].raw_line.startswith("Scheme Code;")
    assert rows[1].raw_line == " "  # blank separator lines are kept, unchanged
    assert rows[2].raw_line == "Open Ended Schemes(Children’s Fund - Childrens' Fund)"
    assert rows[5].raw_line.endswith(";30.9434;09-Nov-2026")  # no stray \r


def test_trailing_line_break_does_not_create_an_extra_line(spark):
    no_trailing = CONTENT.rstrip(b"\r\n")
    assert explode_lines(_files(spark, content=no_trailing), "r").count() == 6
    assert explode_lines(_files(spark, content=CONTENT), "r").count() == 6


def test_metadata_columns(spark):
    row = explode_lines(_files(spark), "run-42").first()
    assert row.source_file == PATH
    assert row.file_sha256 == hashlib.sha256(CONTENT).hexdigest()  # same as the S3 metadata
    assert str(row.nav_file_date) == "2026-11-11"
    assert row.run_id == "run-42"
    assert row.ingested_at is not None


def test_path_without_date_folder_gives_null_date(spark):
    row = explode_lines(_files(spark, path="/tmp/NAVAll.txt"), "r").first()
    assert row.nav_file_date is None


def test_rerun_of_same_file_adds_nothing(spark):
    lines = explode_lines(_files(spark), "run-1")
    already_loaded = lines  # pretend Bronze already holds this file
    assert only_new_files(lines, already_loaded).count() == 0


def test_changed_file_at_same_path_is_loaded_again(spark):
    old = explode_lines(_files(spark), "run-1")
    new = explode_lines(_files(spark, content=CONTENT + b"extra line\r\n"), "run-2")
    assert only_new_files(new, old).count() == 7


def test_same_bytes_from_another_landing_path_same_date_is_skipped(spark):
    from_volume = explode_lines(
        _files(spark, path="/Volumes/mf_nav/raw/landing/nav/date=2026-11-11/NAVAll.txt"), "r1"
    )
    from_s3 = explode_lines(_files(spark), "r2")  # s3://.../date=2026-11-11/, same bytes
    assert only_new_files(from_s3, from_volume).count() == 0


def test_same_bytes_on_a_different_date_is_loaded(spark):
    day1 = explode_lines(_files(spark), "r1")
    day2 = explode_lines(_files(spark, path="s3://b/raw/nav/date=2026-11-12/NAVAll.txt"), "r2")
    assert only_new_files(day2, day1).count() == 6


def test_source_path_guard_rejects_windows_rewritten_path():
    import pytest

    from src.jobs.bronze_nav import check_source_path

    check_source_path("s3://bucket/raw/nav/date=2026-11-11/")
    check_source_path("/Volumes/mf_nav/raw/landing/nav/date=2026-11-11/")
    with pytest.raises(ValueError, match="MSYS_NO_PATHCONV"):
        check_source_path("C:/Program Files/Git/Volumes/mf_nav/raw/landing/")


class _Body:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class FakeS3Reader:
    """Just enough of boto3's S3 client for read_s3_files (list + get)."""

    def __init__(self, objects):
        self.objects = objects  # {key: bytes}
        self.got = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        objects = self.objects

        class _Pages:
            def paginate(self, Bucket, Prefix):  # noqa: N803 (boto3 argument names)
                keys = [k for k in objects if k.startswith(Prefix)]
                yield {"Contents": [{"Key": k} for k in keys]}

        return _Pages()

    def get_object(self, Bucket, Key):  # noqa: N803
        self.got.append(Key)
        return {"Body": _Body(self.objects[Key])}


def test_split_s3_uri():
    from src.jobs.bronze_nav import split_s3_uri

    assert split_s3_uri("s3://b/raw/nav/date=2026-11-11/") == ("b", "raw/nav/date=2026-11-11/")


def test_read_s3_files_returns_exact_bytes_and_s3_paths(spark):
    from src.jobs.bronze_nav import read_s3_files

    fake = FakeS3Reader(
        {
            "raw/nav/date=2026-11-11/NAVAll.txt": CONTENT,
            "raw/nav/date=2026-11-11/notes.csv": b"ignored",
            "raw/_connection_test/hello.txt": b"never read",
        }
    )
    files = read_s3_files(spark, "s3://bucket/raw/nav/date=2026-11-11/", fake)
    rows = files.collect()
    assert [r.path for r in rows] == [PATH]
    assert bytes(rows[0].content) == CONTENT
    assert fake.got == ["raw/nav/date=2026-11-11/NAVAll.txt"]  # only .txt under the prefix
    lines = explode_lines(files, "r").collect()
    assert len(lines) == 6
    assert lines[0].file_sha256 == hashlib.sha256(CONTENT).hexdigest()


def test_read_s3_files_fails_loudly_when_prefix_is_empty(spark):
    import pytest

    from src.jobs.bronze_nav import read_s3_files

    with pytest.raises(FileNotFoundError):
        read_s3_files(spark, "s3://bucket/raw/nav/date=2030-01-01/", FakeS3Reader({}))
