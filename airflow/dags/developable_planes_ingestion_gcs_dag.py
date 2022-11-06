import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

from datetime import datetime, timedelta

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'pv_rooftop_data_all')

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
    dag_id="developable_planes_ingestion_gcs_dag",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['dtc-de'],
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=120),
    concurrency=6
) as dag:

    import requests
    from bs4 import BeautifulSoup
    import re
    url = 'https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pv-rooftop/developable-planes/'
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    urls = []
    count = 0
    for link in soup.find_all('a'):
        url = 'https://data.openei.org/'+link.get('href')
        reqs = requests.get(url)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        soup = soup.find('tr', class_="odd")
        while True:
            try:
                url = soup.tr        
                url = url.find('a')['href']
                urls.append(url)
            finally: 
                break
        count = count + 1
        if count == 23:
            break
    urls = [s for s in urls if s.startswith('h')]

    city_year_list = []
    for url in urls:
        city_year = re.search('city_year=(.*)/', url)
        city_year = city_year.group(1)
        city_year_list.append(city_year)

    city_year_dict = {urls[i]: city_year_list[i] for i in range(len(urls))}



    for url, city_year in city_year_dict.items():
        OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + f"/output_developable_planes_{city_year}.parquet"
        TABLE_NAME = f"developable_planes_{city_year}"
        PARQUET_FILE = f"developable_planes_{city_year}.parquet"
        download_dataset_task = BashOperator(
            task_id=f"download_developable_planes_{city_year}_dataset_task",
            bash_command = f"curl -sSLf {url} > {OUTPUT_FILE_TEMPLATE}"
        )

        local_to_gcs_task = PythonOperator(
            task_id=f"local_developable_planes_{city_year}_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"developable_planes/{PARQUET_FILE}",
                "local_file": f"{OUTPUT_FILE_TEMPLATE}",
            },
        )

        rm_task = BashOperator(
            task_id=f"rm_developable_planes_{city_year}_task",
            bash_command=f"rm {OUTPUT_FILE_TEMPLATE}"
        )
        download_dataset_task >> local_to_gcs_task >> rm_task



