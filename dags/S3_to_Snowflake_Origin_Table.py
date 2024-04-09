import warnings
warnings.filterwarnings(action = "ignore")

import boto3
from datetime import datetime
import pandas as pd

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
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


CreateOrigin = f'''CREATE TABLE IF NOT EXISTS ORIGIN_TEST
                    USING TEMPLATE (
                        SELECT ARRAY_AGG(object_construct(*))
                        FROM TABLE(
                            INFER_SCHEMA(
                            LOCATION=>'@AIR_STAGE'
                            , FILE_FORMAT=>'my_csv_load_format'
                            )
                        ));
            ''' 


CopyIntoOrigin = f'''ALTER TABLE ORIGIN_TEST SET ENABLE_SCHEMA_EVOLUTION = TRUE;

                    COPY INTO ORIGIN_TEST
                    FROM @AIR_STAGE
                    FILE_FORMAT = my_csv_load_format
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

                    ALTER TABLE ORIGIN_TEST SET ENABLE_SCHEMA_EVOLUTION = False;
                '''

CountOriginColumns = f'''SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'AIR_SCHEMA'
                    AND TABLE_NAME = 'ORIGIN_TEST';
            ''' 

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def check_s3_file_count():
    bucket_name = 'chan-cdc-test'
    prefix = '/air/dt/'
    keys = s3.list_keys(bucket_name=bucket_name, prefix=prefix)

    if len(keys) == 1:
        return True
    else:
        return False
    

def read_CSV_and_save_columns(**kwargs):
    S3keys = kwargs['ti'].xcom_pull(task_ids="List_S3_files_Task")
    num_keys = len(S3keys)
    num_files = 0
    bucket_name = 'chan-cdc-test'
    for i in range(num_keys):
        if S3keys[i].endswith('.csv'):
            num_files += 1
            response = s3.get_object(Bucket=bucket_name, Key=S3keys[i])
            csv_file = response['Body']
            
            df = pd.read_csv(csv_file)
            columns = df.columns.tolist()

            kwargs['ti'].xcom_push(key='column_count_' + str(i), value=len(df.columns))
            kwargs['ti'].xcom_push(key='columns_list_' + str(i), value=columns)
    kwargs['ti'].xcom_push(key='num_files', value=num_files)
            

def check_origin_table():
    snowflake_hook = SnowflakeHook(snowflake_conn_id="chan_snow")

    result = snowflake_hook.get_first(f"SHOW TABLES LIKE 'ORIGIN_TEST';")

    if result:
        return "Save_Origin_Columns_Task"
    else:
        return "Init_Origin_Table_Task"


def save_origin_info(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id="chan_snow")

    originColumnCount = snowflake_hook.get_first(CountOriginColumns)
    count_origin_result = originColumnCount[0]
    kwargs['ti'].xcom_push(key='origin_column_count', value=count_origin_result)

    query = f"DESCRIBE TABLE ORIGIN_TEST;"
    columns_info = snowflake_hook.get_records(query)
    column_names = [column[0] for column in columns_info]
    kwargs['ti'].xcom_push(key='origin_column_list', value=column_names)



def check_if_table_needs_recreation(**kwargs):
    ti = kwargs['ti']
    num_files = ti.xcom_pull(task_ids='Read_and_Save_Columns_Task', key='num_files')
    S3_Column_count = ti.xcom_pull(task_ids='Read_and_Save_Columns_Task', key='column_count_' + str(num_files))

    origin_column_count = ti.xcom_pull(task_ids='Save_Origin_Columns_Task', key='origin_column_count')

    if S3_Column_count == origin_column_count:
        return "Column_Count_Equal_Task"
    elif S3_Column_count > origin_column_count:
        return "S3_Columns_Higher_Task"
    else:
        return "S3_Columns_Lower_Task"
    
    
def check_column_list(**kwargs):
    ti = kwargs['ti']
    num_files = ti.xcom_pull(task_ids='Read_and_Save_Columns_Task', key='num_files')
    S3_Columns = ti.xcom_pull(task_ids='Read_and_Save_Columns_Task', key='columns_list_' + str(num_files))

    origin_columns = ti.xcom_pull(task_ids='Save_Origin_Columns_Task', key='origin_column_list')

    if set(S3_Columns) == set(origin_columns):
        return "Task_perfect"
    else:
        return "Task_not_same_column_kind"


with DAG(
    dag_id = 'S3_to_Snowflake_Origin_Table',
    default_args=DEFAULT_ARGS,
    schedule_interval='@once',
    catchup=False
) as dag:
    _task_start = DummyOperator(
        task_id = 'Task_Start',
    )

    s3_file_task = S3ListOperator(
    task_id="List_S3_files_Task",
    bucket="chan-cdc-test",
    prefix="air/dt/",
    delimiter="/",
    aws_conn_id="chan-aws",
    )

    read_and_save_columns_task = PythonOperator(
        task_id='Read_and_Save_Columns_Task',
        python_callable=read_CSV_and_save_columns
    )

    check_origin_table_task = BranchPythonOperator(
        task_id='Check_Origin_Table_Task',
        python_callable=check_origin_table,
    )

    save_origin_info_task = PythonOperator(
        task_id='Save_Origin_Columns_Task',
        python_callable=save_origin_info,
    )   

    init_origin_table_task = SnowflakeOperator(
        task_id='Init_Origin_Table_Task',
        sql=CreateOrigin,
        snowflake_conn_id='chan_snow'
    )

    check_S3_and_origin_columns_task = BranchPythonOperator(
        task_id='Check_S3_and_Origin_Columns_Task',
        python_callable=check_if_table_needs_recreation,
    )

    column_count_equal = BranchPythonOperator(
        task_id = 'Column_Count_Equal_Task',
        python_callable=check_column_list,
    )

    S3_Columns_Higher = SnowflakeOperator(
        task_id = 'S3_Columns_Higher_Task',
        sql=CopyIntoOrigin,
        snowflake_conn_id='chan_snow'
    )

    S3_Columns_Lower = DummyOperator(
        task_id = 'S3_Columns_Lower_Task',
    )

    task_perfect_same = SnowflakeOperator(
        task_id = 'Task_perfect',
        sql=CopyIntoOrigin,
        snowflake_conn_id='chan_snow'
    )

    task_not_same_column_kind = DummyOperator(
        task_id = 'Task_not_same_column_kind',
    )


    _init_origin_task_end = DummyOperator(
        task_id = 'Init_Origin_Task_End',
    )

    _perfect_task_end = DummyOperator(
        task_id = 'Perfect_Task_End',
    )

    _not_same_column_task_end = DummyOperator(
        task_id = 'Not_Same_Column_Task_End',
    )

    _s3_higher_task_end = DummyOperator(
        task_id = 'S3_Higher_Task_End',
    )

    _task_start >> s3_file_task >> read_and_save_columns_task >> check_origin_table_task
    check_origin_table_task >> [save_origin_info_task, init_origin_table_task]
    init_origin_table_task >> _init_origin_task_end
    save_origin_info_task >> check_S3_and_origin_columns_task
    check_S3_and_origin_columns_task >> [column_count_equal, S3_Columns_Higher, S3_Columns_Lower]
    column_count_equal >> [task_perfect_same, task_not_same_column_kind]
    task_perfect_same >> _perfect_task_end
    task_not_same_column_kind >> _not_same_column_task_end
    S3_Columns_Higher >> _s3_higher_task_end
