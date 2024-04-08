import warnings
warnings.filterwarnings(action = "ignore")

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.operators.s3 import (
    S3ListOperator
)

from datetime import datetime



DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# def read_file_from_s3(**kwargs):
#     s3_hook = S3Hook(aws_conn_id='chan-aws')  # aws_default는 Airflow Connection에서 설정된 AWS 연결 ID입니다.
#     air_key = s3_hook.get_key(key=f"air/ADMIN/CHLEE_TEST/LOAD00000001.parquet", bucket_name='chan-cdc-test')
#     print(air_key)

    # file_content = s3_hook.read_key(key=air_key, bucket_name='chan-cdc-test')
    # # print(file_content)  # 또는 파일 내용을 사용하여 원하는 작업 수행

with DAG(
    dag_id = 's3_get_conn',
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

    # read_file_task = PythonOperator(
    #     task_id='read_file_from_s3',
    #     python_callable=read_file_from_s3,
    #     provide_context=True
    # )

    _task_start >> s3_file