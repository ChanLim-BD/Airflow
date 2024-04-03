import warnings
warnings.filterwarnings(action = "ignore")

from airflow.models import DAG, Variable
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


CountColumns = f'''SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'PUBLIC'
                    AND TABLE_NAME = 'ORIGIN_TEST';
            ''' 


def get_snowflake_column_count(**kwargs):
    # Snowflake 연결 설정
    snowflake_hook = SnowflakeHook(snowflake_conn_id="chan_snow")
    
    # 쿼리 실행
    result = snowflake_hook.get_first(CountColumns)

    # 결과를 변수에 저장
    count_result = result[0]  # 첫 번째 열의 값을 가져옴
    kwargs['ti'].xcom_push(key='prev', value=count_result)



DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'test_dag_chan',
    default_args=DEFAULT_ARGS,
    schedule_interval='@once',
    catchup=False
) as dag:
    
    _task_start = DummyOperator(
        task_id = 'Task_Start',
    )       
    
    get_column_count_task = PythonOperator(
        task_id='get_column_count',
        python_callable=get_snowflake_column_count,
    )
    
    _task_end = DummyOperator(
        task_id = 'Task_End',
    )
    
    _task_start  >> get_column_count_task >> _task_end