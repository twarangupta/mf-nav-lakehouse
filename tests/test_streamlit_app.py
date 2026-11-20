"""Smoke test: the public Streamlit app runs on the committed exports without errors.

Skipped when streamlit is not installed (CI installs only the dev extra).
"""

from pathlib import Path

import pytest

APP = Path(__file__).resolve().parents[1] / "dashboards" / "streamlit" / "app.py"


def test_app_renders_all_tiles_from_exports():
    testing = pytest.importorskip("streamlit.testing.v1")
    at = testing.AppTest.from_file(str(APP), default_timeout=60).run()
    assert not at.exception, at.exception
    assert [m.label for m in at.metric] == [
        "Active Growth schemes",
        "Schemes with a 3-year CAGR",
        "Clean NAV records",
        "Latest NAV date",
    ]
    assert at.metric[0].value == "3,716"
    assert at.selectbox[0].value == "Equity - Flexi Cap"
    assert len(at.multiselect[0].value) == 3
    assert len(at.dataframe[0].value) == 10
