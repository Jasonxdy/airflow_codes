"""
# HL Holdings Data Mart DAG
- 마트 명: 결품배치 일자별 차수조회(NEW)
- 마트 생성 주기: 매월 첫 Business Day
"""

from itertools import count
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime

with DAG(
    'DM1_마트생성',
    start_date=datetime(2021,1,1),
    default_args={
        'snowflake-connection': 'snowflake',
        'schedule_interval': '0 7 * * 0-5'},
    tags=['snowflake'],
    catchup=False
) as dag:
    snowflake_sproc_dm1 = SnowflakeOperator(
        task_id = 'snowflake_sproc_dm1',
        snowflake_conn_id = 'snowflake-connection',
        sql = '''
            CALL HLHOLDINGS.ODS.SP_DM1();
        '''
    
    )

snowflake_sproc_dm1