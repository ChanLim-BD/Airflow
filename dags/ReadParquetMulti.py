import warnings
warnings.filterwarnings(action = "ignore")

import io
import boto3
from datetime import datetime
import pyarrow.parquet as pq

from airflow.contrib.hooks.aws_hook import AwsHook

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3ListOperator
)

AWS_HOOK = AwsHook('chan-aws')
CREDENTIALS = AWS_HOOK.get_credentials()
s3 = boto3.client('s3',
                        aws_access_key_id = CREDENTIALS.access_key,
                        aws_secret_access_key = CREDENTIALS.secret_key,
                        region_name = "ap-northeast-2"
)

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}


def read_parquet_and_print_columns(**kwargs):
    S3keys = kwargs['task_instance'].xcom_pull(task_ids="list_3s_files")
    num_files = len(S3keys)
    bucket_name = 'chan-cdc-test'
    for i in range(num_files):
        if S3keys[i].endswith('.parquet'):
            response = s3.get_object(Bucket=bucket_name, Key=S3keys[i])
            parquet_file = response['Body']

            parquet_file_obj = io.BytesIO(parquet_file.read())
            parquet_table = pq.read_table(parquet_file_obj)

            parquet_schema = parquet_table.schema
            columns = parquet_table.column_names
            print("Columns in the Parquet file:")
            for column in columns:
                print(column)
            print("Columns in the Parquet file Count:")
            print(len(parquet_schema))



with DAG(
    dag_id = 'read_parquets_from_s3',
    default_args=DEFAULT_ARGS,
    schedule_interval='@once',
    catchup=False
) as dag:
    _task_start = DummyOperator(
        task_id = 'Task_Start',
    )

    s3_file = S3ListOperator(
    task_id="list_3s_files",
    bucket="chan-cdc-test",
    prefix="air/ADMIN/CHLEE_TEST/",
    delimiter="/",
    aws_conn_id="chan-aws",
    )

    read_and_print_columns = PythonOperator(
        task_id='read_and_print_columns',
        python_callable=read_parquet_and_print_columns
    )

    _task_start >> s3_file >> read_and_print_columns