import warnings
warnings.filterwarnings(action = "ignore")

from airflow.models import DAG, Variable
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
# from airflow.contrib.operators.snowflake_operator import SnowflakeOperator


CountColumns = f'''SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'PUBLIC'
                    AND TABLE_NAME = 'ORIGIN_TEST';
            ''' 


def get_snowflake_column_count(**kwargs):
    # Snowflake 연결 설정
    snowflake_hook = SnowflakeHook(snowflake_conn_id="chan_snow")

    # 이전 컬럼 수를 가져오거나 없으면 0으로 설정
    previous_count_result = kwargs['ti'].xcom_pull(task_ids='check_table', key='previous', default=0)
    
    # 쿼리 실행
    result = snowflake_hook.get_first(CountColumns)

    # 결과를 변수에 저장
    count_result = result[0]  # 첫 번째 열의 값을 가져옴

    # 현재 컬럼 수를 XCom으로 푸시
    kwargs['ti'].xcom_push(key='current', value=count_result)

    # 이전 컬럼 수가 없는 경우에만 0으로 설정하여 XCom에 푸시
    if previous_count_result == 0:
        kwargs['ti'].xcom_push(key='previous', value=0)



def check_if_table_needs_recreation(**kwargs):
    ti = kwargs['ti']
    previous_count_result = ti.xcom_pull(task_ids='get_column_count', key='previous')
    current_count_result = ti.xcom_pull(task_ids='get_column_count', key='current')

    if current_count_result > previous_count_result:
        kwargs['ti'].xcom_push(key='previous', value=current_count_result)
        return "recreate_table_task"
    else:
        return "no_action_task"


def recreate_table():
    # Code to recreate the dynamic table goes here
    print("Recreating dynamic table...")


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
        python_callable=get_snowflake_column_count,
    )

    check_table_task = BranchPythonOperator(
        task_id='check_table',
        python_callable=check_if_table_needs_recreation,
    )
    
    recreate_table_task = PythonOperator(
        task_id='recreate_table_task',
        python_callable=recreate_table,
    )

    no_action_task = DummyOperator(
        task_id='no_action_task',
    )
    
    _task_end = DummyOperator(
        task_id = 'Task_End',
    )
    
    _task_start  >> get_column_count_task >> check_table_task
    check_table_task >> [recreate_table_task, no_action_task] >> _task_end