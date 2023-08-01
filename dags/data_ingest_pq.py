import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.models import Connection
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
from airflow.operators.dummy import DummyOperator
from airflow import settings
#from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator,BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd
import numpy as np

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'final_project')

dataset_file = "bank_marketing.csv"
dataset_url = f"https://drive.google.com/file/d/1t4IrOjA0xIoTwlpLjkJYnq8V7g-qP4iU"
# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
path_to_local_home = "/opt/airflow"
parquet_file = dataset_file.replace('.csv', '.parquet')
destination_table = os.environ.get("BIGQUERY_TABLE", 'marketing')

#dbt_loc = "/opt/airflow/dbt"
#spark_master = "spark://spark:7077"

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    return pq.write_table(table, src_file.replace('.csv', '.parquet'))

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def add_gcp_connection(ds, **kwargs):
    new_conn = Connection(
    conn_id='my_gcp_conn',
    conn_type='google_cloud_platform',
    extra =
    {"extra__google_cloud_platform__key_path":"/opt/airflow/service-account.json", 
    "extra__google_cloud_platform__project":"gold-episode-394309", 
    "extra__google_cloud_platform__scope":"https://www.googleapis.com/auth/cloud-platform"}
    )

   
    session = settings.Session()
    try:
        if BaseHook.get_connection(new_conn.conn_id):
            session.delete(session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one())
    except AirflowNotFoundException:
        pass

    session.add(new_conn)
    session.commit()

query = """CREATE OR REPLACE EXTERNAL TABLE final_project.marketing
OPTIONS(
  "sourceFormat": "PARQUET",
  "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"], """

# define the DAG pipeline
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion-team2",
    schedule_interval="@weekly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['data_ingestion', 'gcs', 'bigquery', 'parquet'], 
    ) as dag: 
    add_gcp_conn_task = PythonOperator(
        task_id="add_gcp_conn_task",
        python_callable=add_gcp_connection,
    )
    
    start = DummyOperator(task_id='start')

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command="gdrive_connect.sh"
        #bash_command=f"curl -sSL {dataset_url} > '{path_to_local_home}/{dataset_file}'"           # for smaller files
    )

    spark_cleansing_task = BashOperator(
        task_id="spark_cleansing_task",
        bash_command="cd /opt/airflow/spark && python3 spark-cleansing.py"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",                       # for parquet
            #"object_name": f"raw/{dataset_file}",                     # for csv
            "local_file": f"{path_to_local_home}/{parquet_file}",       # for parquet file
            #"local_file": f"{path_to_local_home}/{dataset_file}"      # for csv
        },
    )

    load_gcs_to_bq_task = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq',
        bucket=BUCKET,
        source_objects=[f"raw/{parquet_file}"],
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.{destination_table}",
        source_format= 'PARQUET',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        autodetect=False,
        schema_fields =[
            {"name": "client_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "age", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "job", "type": "STRING", "mode": "NULLABLE"},
            {"name": "marital", "type": "STRING", "mode": "NULLABLE"},
            {"name": "education", "type": "STRING", "mode": "NULLABLE"},
            {"name": "credit", "type": "STRING", "mode": "NULLABLE"},
            {"name": "housing", "type": "STRING", "mode": "NULLABLE"},
            {"name": "loan", "type": "STRING", "mode": "NULLABLE"},
            {"name": "contact", "type": "STRING", "mode": "NULLABLE"},
            {"name": "month", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "day_of_week", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "duration", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "campaign", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "pdays", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "previous", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "poutcome", "type": "STRING", "mode": "NULLABLE"},
            {"name": "emp_var_rate", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "cons_price_idx", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "cons_conf_idx", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "euribor3m", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "nr_employed", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "subcribed", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "date", "type": "DATE", "mode": "NULLABLE"},
            ],
    )

    dbt_init_task = BashOperator(
        task_id="dbt_init_task",
        bash_command= "cd /opt/airflow/dbt/bank_marketing && dbt deps && dbt seed --profiles-dir ."
    )
    run_dbt_task = BashOperator(
        task_id="run_dbt_task",
        bash_command= "cd /opt/airflow/dbt/bank_marketing && dbt deps && dbt run --profiles-dir ."
    )

    finish = DummyOperator(task_id='finish')

    start >> add_gcp_conn_task >> \
    download_dataset_task >>  spark_cleansing_task >> \
    format_to_parquet_task >> local_to_gcs_task >> \
    load_gcs_to_bq_task >> dbt_init_task >> \
    run_dbt_task >> finish 