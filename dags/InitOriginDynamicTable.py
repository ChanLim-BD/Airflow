import warnings
warnings.filterwarnings(action = "ignore")

from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator


CreateOrigin = f'''CREATE OR REPLACE TABLE ORIGIN_TEST
                    USING TEMPLATE (
                        SELECT ARRAY_AGG(object_construct(*))
                        FROM TABLE(
                            INFER_SCHEMA(
                            LOCATION=>'@air_prac2'
                            , FILE_FORMAT=>'my_csv_load_dynamic_format'
                            )
                        ));
            ''' 

CreateDynamic = f'''CREATE OR REPLACE DYNAMIC TABLE DTABLETEST
                    TARGET_LAG = '1 minutes'
                    WAREHOUSE = COMPUTE_WH
                    AS
                        SELECT * FROM ORIGIN_TEST;
            '''

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'init_origin_dyanmic_dag_chan',
    default_args=DEFAULT_ARGS,
    schedule_interval='@once',
    catchup=False
) as dag:
    
    _task_start = DummyOperator(
        task_id = 'Task_Start',
    )       
    
    create_origin = SnowflakeOperator(
        task_id='create-origin',
        sql=CreateOrigin,
        snowflake_conn_id='chan_snow'
    )

    create_dyanmic = SnowflakeOperator(
        task_id='create-dynamic',
        sql=CreateDynamic,
        snowflake_conn_id='chan_snow'
    )    

    trigger_recreate = TriggerDagRunOperator(
        task_id='trigger-recreate',
        trigger_dag_id='RecreateTest_chan',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None
        )
    
    _task_start  >> create_origin >> create_dyanmic >> trigger_recreate