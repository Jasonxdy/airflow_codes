from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
# import pandas as pd


from datetime import datetime, timedelta
import pendulum

import requests
import logging
# import psycopg2
import json

# 1. open weather api에서 값 받아옴
# -> 일단 하루치만 받아와서 parsing 후 넣기
# 2. snowflake에 값 insert


#timezone 설정
KST = pendulum.timezone("Asia/Seoul")

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

  """
  data['daily']:List structure

  {
      "dt": 1668135600,
      "sunrise": 1668118024,
      "sunset": 1668155110,
      "moonrise": 1668161280,
      "moonset": 1668127500,
      "moon_phase": 0.59,
      "temp": {
        "day": 19.26,
        "min": 11.41,
        "max": 20.43,
        "night": 15.3,
        "eve": 18.29,
        "morn": 11.66
      },
      "feels_like": {
        "day": 18.42,
        "night": 14.43,
        "eve": 17.45,
        "morn": 10.79
      },
      "pressure": 1025,
      "humidity": 45,
      "dew_point": 7.05,
      "wind_speed": 2.51,
      "wind_deg": 138,
      "wind_gust": 3.83,
      "weather": [
        {
          "id": 800,
          "main": "Clear",
          "description": "clear sky",
          "icon": "01d"
        }
      ],
      "clouds": 3,
      "pop": 0,
      "uvi": 2.6
    }
  
  """
  
  ret = []

  for d in data['daily']:
    day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
    ret.append("('{}',{},{},{}, DEFAULT)".format(day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))
  
  return ret

  

def load_into_snowflake(**context):
  #test code
  database = context["params"]["database"]
  schema = context["params"]["schema"]
  table = context["params"]["table"]
  rows = context["task_instance"].xcom_pull(key="return_value", task_ids="calling_weather_api")

  try:
    conn = get_snowflake_connection() 
    cur = conn.cursor()
    # snowflake.connector.cursor.SnowflakeCursor object returned
    # cursor object docs : https://docs.snowflake.com/en/user-guide/python-connector-api.html#object-cursor
    # result = cur.execute(f'select * from {database}.{schema}.{table}').fetchall() # list 반환

    # logging.info(result.fetch_pandas_all()) => fail, this is commonly faster than .fetchall()
    # snowflake.connector.errors.ProgrammingError: 255002: 255002: Optional dependency: 'pandas' is not installed, please see the following link for install instructions: https://docs.snowflake.com/en/user-guide/python-connector-pandas.html#installation
    
    # logging.info(type(result)) #success => INFO - <class 'list'>

    # 0. 테이블 없으면 생성

    sql_init = f"""
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} (
      date date,
      temp float,
      min_temp float,
      max_temp float,
      updated_date timestamp default GETDATE()
    );
    """
    cur.execute(sql_init)

    # 1. 먼저 임시 테이블 생성 -> 기존 값 전부 넣기
    sql_create = f"""
    BEGIN
    DROP TABLE IF EXISTS {database}.{schema}.temp_{table};
    CREATE TABLE {database}.{schema}.temp_{table} LIKE {database}.{schema}.{table};
    INSERT INTO {database}.{schema}.temp_{table} SELECT * FROM {database}.{schema}.{table};
    END;
    """
    try:
      cur.execute(sql_create)
      cur.execute("COMMIT;")
    except:
      cur.execute("ROLLBACK;")
      raise

    # 2. 임시 테이블에 새로운 값 전부 로딩
    sql_load = f"INSERT INTO {database}.{schema}.temp_{table} VALUES " + ",".join(rows)
    try:
      cur.execute(sql_load)
      cur.execute("COMMIT;")
    except:
      cur.execute("ROLLBACK;")
      raise

    # 3. 기존 테이블 truncate하고 select한 새로운 값 넣기
    sql_select_insert = f"""
    BEGIN
    DELETE FROM {database}.{schema}.{table};
    INSERT INTO {database}.{schema}.{table}
    SELECT date, temp, min_temp, max_temp, updated_date FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY updated_date DESC) seq
      FROM {database}.{schema}.temp_{table}
    )
    WHERE seq = 1;
    END;
    """
    try:
      cur.execute(sql_select_insert)
      cur.execute("COMMIT;")
    except:
      cur.execute("ROLLBACK;")
      raise

  except:
    raise
  finally:
    conn.close()


with DAG(
    'Snowflake-7days-batch',
    default_args={
      'snowflake-connection': 'snowflake-connection'
    },
    start_date=datetime(2022,11, 1, tzinfo=KST),
    schedule_interval= '0 0 * * *',
    description='getting weather information on past 7 days and loading it into Snowflake',
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
  
  load_into_snowflake = PythonOperator(
    task_id = "load_into_snowflake",
    python_callable=load_into_snowflake,
    params = {
      "database" : "SAMPLE_DATABASE_DYC",
      "schema" : "PUBLIC",
      "table" : "SAMPLE_TABLE"
    }
  )
    
 
calling_weather_api >> load_into_snowflake




  
  
