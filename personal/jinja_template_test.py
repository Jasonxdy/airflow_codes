from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import logging

from datetime import datetime
from datetime import timedelta

def printer(d1, d2, d3):
  logging.info("---------")
  logging.info("ds : " + d1)
  logging.info("data_interval_start  : " + d2)
  logging.info("data_interval_end : " + d3)
  logging.info("---------")



test_dag = DAG (
    dag_id = 'checking_template_varialbes',
    start_date = datetime(2022,8,24),
    schedule_interval = '*/1 * * * *',
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 0,
    }
)

printer = PythonOperator(
  task_id = 'printer',
  python_callable = printer,
  op_args=["{{ ds }}", "{{data_interval_start}}", "{{data_interval_end}}"],
  dag = test_dag
)
