# FIXME: This script needs a docstring explaining what it does/how it's used
import os
# Removed unused import

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.utils.dates import days_ago  # Sorted imports alphabetically
# Removed unused import

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'pv_rooftop_data_all')

DATASET = "buildings"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="buildings_gcs_2_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    # It can be nice with longer arguments to declare them first and then make the function call with their names
    task_id = f"bq_{DATASET}_external_table_task"
    table_resources = {
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{DATASET}_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": True,
                "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                "sourceUris": [f"gs://{BUCKET}/buildings/*"],
            },
        }

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=task_id,
        table_resource=table_resources,
    )

    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DATASET} \
        AS \
        SELECT * FROM {BIGQUERY_DATASET}.{DATASET}_external_table;"
    )

    # You could do the same trick here as above, though this is much more legible. And ultimately that sort of thing
    # will really come down to choice of your employer/team style
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_{DATASET}_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    # FIXME: It's unclear to me what's happening here. the `>>` operator is a bitwise operator, but this doesn't _look_
    #  like a bitwise operation to me, and my IDE is calling it out as an operation with no used effect. I'm assuming
    #  it does something otherwise you wouldn't have put it here but I'm curious
    bigquery_external_table_task >> bq_create_partitioned_table_job
