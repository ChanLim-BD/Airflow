import warnings
warnings.filterwarnings(action = "ignore")

from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator


DynamicCreate = f'''CREATE OR REPLACE DYNAMIC TABLE DTABLETEST
                        TARGET_LAG = '1 minutes'
                        WAREHOUSE = COMPUTE_WH
                        AS
                            SELECT * FROM ORIGIN_TEST;
            '''

CountOriginColumns = f'''SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'PUBLIC'
                    AND TABLE_NAME = 'ORIGIN_TEST';
            ''' 

CountDynamicColumns = f'''SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'PUBLIC'
                    AND TABLE_NAME = 'DTABLETEST';
            ''' 


def get_column_count(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id="chan_snow")

    originResult = snowflake_hook.get_first(CountOriginColumns)
    count_origin_result = originResult[0]
    kwargs['ti'].xcom_push(key='current', value=count_origin_result)

    dynamicResult = snowflake_hook.get_first(CountDynamicColumns)
    count_dynamic_result = dynamicResult[0]
    kwargs['ti'].xcom_push(key='previous', value=count_dynamic_result)



def check_if_table_needs_recreation(**kwargs):
    ti = kwargs['ti']
    previous_count_result = ti.xcom_pull(task_ids='get_column_count', key='previous')
    current_count_result = ti.xcom_pull(task_ids='get_column_count', key='current')

    if current_count_result > previous_count_result:
        return "recreate_table_task"
    else:
        return "no_action_task"



DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'RecreateTest_chan',
    default_args=DEFAULT_ARGS,
    schedule_interval='@once',
    catchup=False
) as dag:
    
    _task_start = DummyOperator(
        task_id = 'Task_Start',
    ) 

    get_column_count_task = PythonOperator(
        task_id='get_column_count',
        python_callable=get_column_count,
    )     
    

    check_table_task = BranchPythonOperator(
        task_id='check_table',
        python_callable=check_if_table_needs_recreation,
    )
    
    recreate_table_task = SnowflakeOperator(
        task_id='recreate_table_task',
        sql=DynamicCreate,
        snowflake_conn_id='chan_snow'
    )

    no_action_task = DummyOperator(
        task_id='no_action_task',
    )
    
    _task_start  >> get_column_count_task >> check_table_task
    check_table_task >> [recreate_table_task, no_action_task]