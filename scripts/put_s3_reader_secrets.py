"""Copy the read-only S3 key from .env into the Databricks secret scope `mf_nav`.

The Bronze job reads S3 with these two secrets. Values go straight from the
environment to the Databricks Secrets API and are never printed; the script prints
only the key names it wrote.

    python scripts/put_s3_reader_secrets.py

Needs DATABRICKS_HOST, DATABRICKS_TOKEN, S3_READER_ACCESS_KEY_ID and
S3_READER_SECRET_ACCESS_KEY in the environment (.env).
"""

from __future__ import annotations

import os

import requests

SCOPE = "mf_nav"
SECRETS = {
    "s3_reader_access_key_id": "S3_READER_ACCESS_KEY_ID",
    "s3_reader_secret_access_key": "S3_READER_SECRET_ACCESS_KEY",
}


def main() -> None:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    headers = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    scopes = requests.get(f"{host}/api/2.0/secrets/scopes/list", headers=headers, timeout=30)
    scopes.raise_for_status()
    if SCOPE not in {s["name"] for s in scopes.json().get("scopes", [])}:
        requests.post(
            f"{host}/api/2.0/secrets/scopes/create",
            headers=headers,
            json={"scope": SCOPE},
            timeout=30,
        ).raise_for_status()
        print(f"created scope {SCOPE}")
    for secret_key, env_name in SECRETS.items():
        requests.post(
            f"{host}/api/2.0/secrets/put",
            headers=headers,
            json={"scope": SCOPE, "key": secret_key, "string_value": os.environ[env_name]},
            timeout=30,
        ).raise_for_status()
        print(f"wrote {SCOPE}/{secret_key}")


if __name__ == "__main__":
    main()
