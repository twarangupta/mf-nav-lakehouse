# AWS IAM for this project

Replace `REPLACE_WITH_...` placeholders before pasting into the AWS console. No account ids, keys or external ids are stored here.

| File | Attached to | Used for |
|---|---|---|
| `iam_policy_airflow_uploader.json` | IAM user `mf-nav-airflow` (no console) | Airflow / laptop uploads raw files: list, get, put under `raw/`. No delete. |
| `iam_policy_databricks_read.json` | IAM user `mf-nav-databricks-reader` (no console), and role `mf-nav-databricks-role` | Databricks reads raw files: get under `raw/`, list the bucket. No put, no delete. |
| `trust_policy_databricks_step1_placeholder.json` | role `mf-nav-databricks-role` | Step 1 of the Unity Catalog role setup (placeholder external id). |
| `trust_policy_databricks_step3_final.json` | role `mf-nav-databricks-role` | Step 3: Databricks master role + self-assume + the credential's external id. |

## How Databricks reads S3 in this project

**In use: read-only IAM user key in a Databricks secret scope.** The Bronze job reads `s3://<bucket>/raw/nav/...` with boto3, using the key of `mf-nav-databricks-reader` stored in Databricks secret scope `mf_nav` (keys `s3_reader_access_key_id`, `s3_reader_secret_access_key`). The key is never in code or git.

**Built but not used: Unity Catalog storage credential + IAM role.** This is the recommended pattern (no long-lived key, short-lived STS credentials). On this setup the AssumeRole step is refused: the Databricks Free Edition workspace runs in `us-east-2` and the AWS free-plan account is locked to `ap-southeast-2`, and the AWS free plan does not support cross-region features. The role and the storage credential `mf_nav_s3_read` are kept so the project can switch back on a paid account.
