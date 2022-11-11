from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
# import pandas as pd


from datetime import datetime
from datetime import timedelta

import requests
import logging
# import psycopg2
import json

# 1. open weather api에서 값 받아옴
# -> 일단 하루치만 받아와서 parsing 후 넣기
# 2. snowflake에 값 insert


snowflake_sql = ''

def get_snowflake_connection():
  hook = SnowflakeHook(snowflake_conn_id = 'snowflake-connection')
  return hook.get_conn()

def calling_weather_api(**context):
  lat = context["params"]["lat"]
  lon = context["params"]["lon"]
  api_key = Variable.get("open_weather_api_key")
  url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric&exclude=current,minutely,hourly,alerts"
  response = requests.get(url)
  data = json.loads(response.text)
  
  logging.info(data)


def parsing_json(**context):
  pass

def insert_into_snowflake(**context):
  #test code
  database = context["params"]["database"]
  schema = context["params"]["schema"]
  table = context["params"]["table"]
  try:
    conn = get_snowflake_connection() 
    cur = conn.cursor()
    # snowflake.connector.cursor.SnowflakeCursor object returned
    # cursor object docs : https://docs.snowflake.com/en/user-guide/python-connector-api.html#object-cursor
    result = cur.execute(f'select * from {database}.{schema}.{table}').fetchall() # list 반환

    # logging.info(result.fetch_pandas_all()) => fail, this is commonly faster than .fetchall()
    # snowflake.connector.errors.ProgrammingError: 255002: 255002: Optional dependency: 'pandas' is not installed, please see the following link for install instructions: https://docs.snowflake.com/en/user-guide/python-connector-pandas.html#installation
    
    # logging.info(type(result)) #success => INFO - <class 'list'>
    logging.info(result)
    
  except:
    raise
  finally:
    conn.close()


with DAG(
    'Snowflake-7days-batch',
    default_args={
      'snowflake-connection': 'snowflake-connection',
      'schedule_interval': '0 7 * * *'
    },
    description='getting weather information on past 7 days and loading it into Snowflake',
    start_date=days_ago(2),
    tags=['snowflake'],
    catchup = False,
) as dag:
  # snowflake_insert = SnowflakeOperator(
  #   task_id = 'snowflake_insert',
  #   snowflake_conn_id = 'snowflake-connection',
  #   sql = snowflake_sql
  # )

  calling_weather_api = PythonOperator(
       task_id="calling_weather_api",
       python_callable=calling_weather_api,
       params = {
        "lat": 37.5665,
        "lon": 126.9780,
    }
  )

  parsing_json = PythonOperator(
      task_id="parsing_json",
      python_callable=parsing_json
  )
  
  insert_into_snowflake = PythonOperator(
    task_id = "insert_into_snowflake",
    python_callable=insert_into_snowflake,
    params = {
      "database" : "SAMPLE_DATABASE_DYC",
      "schema" : "PUBLIC",
      "table" : "SAMPLE_TABLE"
    }
  )
    
 
calling_weather_api >> parsing_json >> insert_into_snowflake




  
  
