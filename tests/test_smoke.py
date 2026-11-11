"""Smoke tests: cheap checks that run on every push, with no Spark and no network.

They prove three things:
1. The project package imports.
2. Every environment variable the code reads is documented in .env.example.
3. .env.example never contains a real secret value.
"""

import ast
import importlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CODE_DIRS = [ROOT / "src", ROOT / "dags"]
SECRET_VARS = {"DATABRICKS_TOKEN", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}


def _env_example() -> dict[str, str]:
    """Parse .env.example into {NAME: value}, skipping comments and blank lines."""
    values = {}
    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            name, value = line.split("=", 1)
            values[name.strip()] = value.strip()
    return values


def _env_vars_read_by_code() -> set[str]:
    """Find names used as os.environ["X"], os.environ.get("X") or os.getenv("X")."""
    names = set()
    for folder in CODE_DIRS:
        for path in folder.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                # os.getenv("X") and os.environ.get("X")
                if isinstance(node, ast.Call) and node.args:
                    func = ast.unparse(node.func)
                    first = node.args[0]
                    if func in {"os.getenv", "os.environ.get"} and isinstance(first, ast.Constant):
                        names.add(first.value)
                # os.environ["X"]
                if isinstance(node, ast.Subscript) and ast.unparse(node.value) == "os.environ":
                    if isinstance(node.slice, ast.Constant):
                        names.add(node.slice.value)
    return names


def test_package_imports():
    assert importlib.import_module("src") is not None
    assert importlib.import_module("src.jobs") is not None


def test_env_example_documents_every_variable_the_code_reads():
    missing = _env_vars_read_by_code() - set(_env_example())
    assert not missing, f"Add these to .env.example: {sorted(missing)}"


def test_env_example_has_no_secret_values():
    filled = {name for name in SECRET_VARS if _env_example().get(name)}
    assert not filled, f".env.example must leave these empty: {sorted(filled)}"
