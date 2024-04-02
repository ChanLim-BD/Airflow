import warnings
warnings.filterwarnings(action = "ignore")

from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import boto3
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.operators.dms import (
    DmsDeleteTaskOperator,
    DmsStartTaskOperator,
)
from airflow.providers.amazon.aws.sensors.dms import DmsTaskCompletedSensor
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

import json 

'''
* 완성이력 : 
    - 24-03-27 : DMS 및 Snowflake API 활용 
* 수정이력  : 
* 기   능   : 
    - DMS 마이그레이션 태스크 생성, 실행, 삭제, 대기 기능
    - S3 To Snowflake 데이터 이관 기능
* 변수명 규칙 :
    - 전역변수 : 대문자
    - 지역변수 : '_'로 시작 
'''

'''
* AWS_DEFAULT_REGION  : AWS 계정 리전 정보
* CREATE_ASSETS       : DMS API에 사용되는 Dict 기반 정보
    - source_endpoint_arn   : DMS 소스 엔드포인트 ARN 정보
    - target_endpoint_arn   : DMS 타겟 엔드포인트 ARN 정보
    - replication_instance_arn : DMS 복제 인스턴스 ARN 정보
    - migration_type        : 데이터 이관 타입 'full-load' | 'cdc' | 'full-load-and-cdc'
    - aws_conn_id           : Airflow 내의 AWS Connection ID 
'''
AWS_DEFAULT_REGION = "ap-northeast-2"
CREATE_ASSETS = {
    'source_endpoint_arn' : 'arn:aws:dms:ap-northeast-2:{--}',
    'target_endpoint_arn' : 'arn:aws:dms:ap-northeast-2:{--}',
    'replication_instance_arn' : 'arn:aws:dms:ap-northeast-2:{--}',
    'migration_type': 'full-load',      
    'aws_conn_id' : 'chan-aws'
}

'''
* FILTER_DATE     : 데이터 이관 시, 일정 범위의 데이터만 가져올 기준 일자 (현재 샘플로 작성)
* DMS_REPLICATION_TASK_NAME : DMS 마이그레이션 태스크 명(이 변수 값으로 태스크명 생성)
* TABLE_MAPPINGS  : DMS 마이그레이션 태스크에 사용되는 소스 객체의 맵핑 정보 
    - 현재 규칙정보 : Source DB의 ADMIN.SCSDAY1 테이블의 WDATE컬럼에서 FILTER_DATE 값보다 작거나 같은 데이터 추출
    - 이관을 원하는 스키마 및 테이블 정보 포함 가능
    - 열 포함, 제외, 필터 기능 
        - filter-operator :
            - ste : 하나의 값보다 작거나 같음
            - gte : 하나의 값보다 크거나 같음
            - between : 두 값과 같음 또는 두 값 사이의 값임.  
    - 상세 내용 : https://docs.aws.amazon.com/ko_kr/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.html
'''
FILTER_DATE = '2000-02-19'
DMS_REPLICATION_TASK_NAME = 'dms-replication-test1'
TABLE_MAPPINGS = {
    "rules": [
        {
            "rule-type": "selection",
            "rule-id": "1",
            "rule-name": "1",
            "object-locator": {
                "schema-name": "ADMIN",
                "table-name": "CHLEE_TEST"
            },
            "rule-action": "include",
            "filters": []
        }
    ]
}



'''
* 기능 : DMS API를 사용하기 위함.
* DMS_CLIENT : 이 변수 기반으로 DMS API 사용
'''
AWS_HOOK = AwsHook(CREATE_ASSETS['aws_conn_id'])
CREDENTIALS = AWS_HOOK.get_credentials()
DMS_CLIENT = boto3.client("dms",
                          aws_access_key_id = CREDENTIALS.access_key,
                          aws_secret_access_key = CREDENTIALS.secret_key,
                          region_name = AWS_DEFAULT_REGION
)

'''
* 기능 : 
    - DMS 마이그레이션 태스크 생성 및 생성 완료 시까지 대기 
* Parameter : 
    - **context : Airflow context 변수 , 생성한 태스크 ARN 값 저장을 위함.
* Return    : 생성한 태스크 ARN 값.
'''
def Create_DMS_Task(**context):
    response = DMS_CLIENT.create_replication_task(
        ReplicationTaskIdentifier=DMS_REPLICATION_TASK_NAME,
        SourceEndpointArn = CREATE_ASSETS['source_endpoint_arn'],
        TargetEndpointArn = CREATE_ASSETS['target_endpoint_arn'],
        ReplicationInstanceArn= CREATE_ASSETS['replication_instance_arn'],
        MigrationType = CREATE_ASSETS['migration_type'],
        TableMappings = json.dumps(TABLE_MAPPINGS),
    )

    # create_replication_task() 함수로 태스크 생성 후, 생성이 완료될 때 까지 대기하는 함수이다.
    _waiter = DMS_CLIENT.get_waiter('replication_task_ready')
    _waiter.wait(Filters=[
        {
            'Name': "replication-task-arn",
            'Values': [response['ReplicationTask']['ReplicationTaskArn']]
        },
    ])
    context['ti'].xcom_push(key='xcom_push_value', value= response['ReplicationTask']['ReplicationTaskArn'])
  
'''
* 기능 : DMS 태스크로 S3에 적재된 데이터를 Snowflake에 적재하기 위한 쿼리
'''  

SQL_QUERY = f'''COPY INTO AIR_RESULT
                FROM @AIR_PRAC
                FILE_FORMAT = (FORMAT_NAME = MY_PARQUET_FORMAT)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
            '''    

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'dms_test_dag_chan',
    default_args=DEFAULT_ARGS,
    schedule_interval='@once',
    catchup=False
) as dag:
    
    _task_start = DummyOperator(
        task_id = 'Task_Start',
    )       
    
    create_task = PythonOperator(
    task_id='create_dms_task',
    provide_context=True,
    python_callable=Create_DMS_Task,
    )
    
    # Create_DMS_Task() 함수에서 생성한 DMS 태스크 ARN 값. 
    task_arn = '{{ti.xcom_pull(key="xcom_push_value")}}'
    
    start_task = DmsStartTaskOperator(
        task_id="start_task",
        replication_task_arn=task_arn,
        aws_conn_id = CREATE_ASSETS['aws_conn_id']
    )
    
    await_task_start = DmsTaskCompletedSensor(
        task_id="await_task_start",
        replication_task_arn=task_arn,
        aws_conn_id = CREATE_ASSETS['aws_conn_id'],
    )
    
    delete_task = DmsDeleteTaskOperator(
        task_id="delete_task",
        replication_task_arn=task_arn,
        aws_conn_id = CREATE_ASSETS['aws_conn_id'],
    )
    
    s3_to_snow = SnowflakeOperator(
        task_id='s3-to-snowflake',
        sql=SQL_QUERY,
        snowflake_conn_id='chan_snow'
    )
    
    _task_end = DummyOperator(
        task_id = 'Task_End',
    )
    
    _task_start >> create_task >> start_task >>  await_task_start >> delete_task >> s3_to_snow >> _task_end