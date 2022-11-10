from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2

# 1. open weather api에서 값 받아옴
# 2. snowflake에 값 insert
# 3. 
dag = DAG(
  dag_id = 'openweather_api'
  
)

def test (**conf):
  
  
