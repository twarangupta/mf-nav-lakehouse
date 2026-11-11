"""Run one of our PySpark job files on Databricks serverless, from the laptop.

Uploads the job file to your workspace, submits a one-time serverless run through the
Jobs API (runs/submit), waits for it, and prints the result. This is the manual
stand-in for what Airflow does in Phase 6.

    python scripts/run_on_databricks.py src/jobs/bronze_nav.py \
        --source-path s3://<bucket>/raw/nav/date=2026-11-11/ --run-id manual-1

Everything after the job file is passed to the job as its command-line arguments.
Reads DATABRICKS_HOST and DATABRICKS_TOKEN from the environment (.env).

Git Bash on Windows rewrites arguments that start with "/" into Windows paths
("/Volumes/x" becomes "C:/Program Files/Git/Volumes/x"). Prefix the command with
MSYS_NO_PATHCONV=1 when passing /Volumes/... paths, or use PowerShell.
"""

from __future__ import annotations

import base64
import os
import sys
import time
from pathlib import Path

import requests


def _api(method: str, path: str, **kwargs) -> dict:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    headers = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    response = requests.request(method, f"{host}{path}", headers=headers, timeout=60, **kwargs)
    if response.status_code >= 400:
        raise RuntimeError(f"{method} {path} -> {response.status_code}: {response.text[:500]}")
    return response.json() if response.content else {}


def upload_job_file(local_file: Path) -> str:
    """Upload to a new folder per deploy (deploy/<UTC timestamp>/), never overwrite.

    Immutable, timestamped deploys record exactly which code each run used.
    """
    user = _api("GET", "/api/2.0/preview/scim/v2/Me")["userName"]
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    folder = f"/Workspace/Users/{user}/mf-nav-lakehouse/deploy/{stamp}"
    _api("POST", "/api/2.0/workspace/mkdirs", json={"path": folder})
    target = f"{folder}/{local_file.name}"
    _api(
        "POST",
        "/api/2.0/workspace/import",
        json={
            "path": target,
            "format": "AUTO",
            "overwrite": True,
            "content": base64.b64encode(local_file.read_bytes()).decode(),
        },
    )
    return target


def submit_and_wait(workspace_file: str, params: list[str], poll_seconds: int = 10) -> dict:
    run = _api(
        "POST",
        "/api/2.2/jobs/runs/submit",
        json={
            # Unique per submission, so runs are easy to find in the Jobs UI.
            "run_name": (
                f"mf-nav {Path(workspace_file).stem.replace('_', '-')} "
                f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
            ),
            "tasks": [
                {
                    "task_key": "main",
                    "spark_python_task": {"python_file": workspace_file, "parameters": params},
                    "environment_key": "default",
                }
            ],
            "environments": [
                {
                    "environment_key": "default",
                    "spec": {"environment_version": os.environ.get("DBX_ENV_VERSION", "4")},
                }
            ],
        },
    )
    run_id = run["run_id"]
    print(f"submitted run {run_id}")
    while True:
        info = _api("GET", "/api/2.2/jobs/runs/get", params={"run_id": run_id})
        state = info["state"]
        if state.get("life_cycle_state") in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            break
        time.sleep(poll_seconds)
    task_run_id = info["tasks"][0]["run_id"]
    output = _api("GET", "/api/2.2/jobs/runs/get-output", params={"run_id": task_run_id})
    seconds = (info.get("end_time", 0) - info.get("start_time", 0)) / 1000
    print(f"result: {state.get('result_state')} in {seconds:.0f}s | {state.get('state_message')}")
    for key in ("logs", "error", "error_trace"):
        if output.get(key):
            print(f"--- {key} ---\n{output[key][-3000:]}")
    print(f"run page: {info.get('run_page_url')}")
    return info


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    job_file = Path(sys.argv[1])
    workspace_file = upload_job_file(job_file)
    info = submit_and_wait(workspace_file, sys.argv[2:])
    sys.exit(0 if info["state"].get("result_state") == "SUCCESS" else 1)


if __name__ == "__main__":
    main()
