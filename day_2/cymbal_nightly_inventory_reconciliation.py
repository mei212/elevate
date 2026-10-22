"""
Cloud Composer Nightly Orchestration DAG — Cymbal Retail Inventory Reconciliation
=================================================================================
Schedules the Dataproc Serverless 2.3 Lightning Engine PySpark pipeline
(`migrated_inventory_reconciliation_pipeline.py`) nightly at 10:00 PM UTC (`0 22 * * *`),
followed by a BigQuery data quality parity gate asserting 8,400 reconciled rows.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

PROJECT_ID = "elevate-da-meisakurai"
REGION = "us-central1"
SUBNET_URI = f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/cymbal-retail-subnet-{REGION}"
SERVICE_ACCOUNT = f"cymbal-sa-data@{PROJECT_ID}.iam.gserviceaccount.com"
CODE_BUCKET = f"{PROJECT_ID}-module1-bucket"
PYSPARK_URI = f"gs://{CODE_BUCKET}/code/migrated_inventory_reconciliation_pipeline.py"
FEDERATED_CATALOG = f"{PROJECT_ID}.cymbal-lakehouse.elevate_data"
TARGET_GOLD_TABLE = f"{PROJECT_ID}.cymbal_gold.gold_inventory_reconciliation_ledger"

default_args = {
    "owner": "cymbal-data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cymbal_nightly_inventory_reconciliation",
    default_args=default_args,
    description="Nightly Dataproc Serverless 2.3 Lightning Engine Inventory Reconciliation & BigQuery Gold Parity Gate",
    schedule="0 22 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["cymbal-retail", "dataproc-serverless", "lightning-engine", "iceberg", "module-1"],
) as dag:

    run_inventory_reconciliation_lightning = DataprocCreateBatchOperator(
        task_id="run_inventory_reconciliation_lightning",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="recon-lakehouse-{{ ts_nodash | lower }}-{{ ti.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": PYSPARK_URI,
                "args": [
                    f"--catalog={FEDERATED_CATALOG}",
                    f"--target-table={TARGET_GOLD_TABLE}",
                    f"--gcs-staging-bucket={CODE_BUCKET}",
                    "--start-date=2025-01-01",
                    "--until-date=2026-09-10",
                    "--write-mode=overwrite",
                ],
            },
            "runtime_config": {
                "version": "2.3",
                "properties": {
                    "dataproc.tier": "premium",
                    "spark.dataproc.engine": "lightningEngine",
                    "spark.dataproc.lineage.enabled": "true",
                },
            },
            "environment_config": {
                "execution_config": {
                    "service_account": SERVICE_ACCOUNT,
                    "subnetwork_uri": SUBNET_URI,
                }
            },
        },
    )

    validate_gold_table_parity = BigQueryCheckOperator(
        task_id="validate_gold_table_parity",
        sql=f"SELECT COUNT(*) = 8400 FROM `{TARGET_GOLD_TABLE}`",
        use_legacy_sql=False,
        location=REGION,
    )

    run_inventory_reconciliation_lightning >> validate_gold_table_parity
