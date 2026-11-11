"""Shared test fixtures: one small local SparkSession for the whole test run."""

import os
import shutil
import sys

import pytest


def _java_available() -> bool:
    java_home = os.environ.get("JAVA_HOME")
    if java_home and os.path.exists(os.path.join(java_home, "bin")):
        return True
    return shutil.which("java") is not None


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession (1 core, no UI). Needs Java 17.

    Without Java the Spark tests are skipped on a laptop, but CI sets
    REQUIRE_SPARK=1 so a missing Java fails the build instead of hiding tests.
    """
    if not _java_available():
        if os.environ.get("REQUIRE_SPARK") == "1":
            pytest.fail("Java not found but REQUIRE_SPARK=1")
        pytest.skip("Java 17 not found; set JAVA_HOME to run Spark tests")

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("mf-nav-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
