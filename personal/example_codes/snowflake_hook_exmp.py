import logging

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="snowflake_automation", default_args=args, schedule_interval=None
)

snowflake_query = [
    """create table public.test_employee (id number, name string);""",
    """insert into public.test_employee values(1, “Sam”),(2, “Andy”),(3, “Gill”);""",
]


def get_row_count(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = dwh_hook.get_first("select count(*) from public.test_employee")
    logging.info("Number of rows in `public.test_employee`  - %s", result[0])

with dag:
    create_insert = SnowflakeOperator(
        task_id="snowfalke_create",
        sql=snowflake_query ,
        snowflake_conn_id="snowflake_conn",
    )

    get_count = PythonOperator(task_id="get_count", python_callable=get_row_count)

create_insert >> get_count