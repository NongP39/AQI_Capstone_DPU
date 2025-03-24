import json
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

import requests


DAG_FOLDER = "/opt/airflow/dags"


def _get_weather_data():
    #assert 1 == 2

    # API_KEY = os.environ.get("WEATHER_API_KEY")
    API_KEY = Variable.get("aqi_api_key")

    payload = {
        "city": "Bangkok",
        "state" : "Bangkok",
        "country" : "Thailand",
        "key": API_KEY
    }
    url = "http://api.airvisual.com/v2/city?"
    response = requests.get(url, params=payload)
    print(response.url)

    data = response.json()
    print(data)

    with open(f"{DAG_FOLDER}/data.json", "w") as f:
        json.dump(data, f)


def _validate_data():
    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)

    assert data.get("status") is not "fail"

def _create_weather_table():
    pg_hook = PostgresHook(
        postgres_conn_id="AQI",
        schema="capstone"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS AQI (
            timpstamp BIGINT NOT NULL,
            auius FLOAT NOT NULL,
            temp FLOAT NOT NULL,
            pressure NUMERIC,
            humidity NUMERIC,
            wind_speed FLOAT,
            wind_direction FLOAT
        )
    """
    cursor.execute(sql)
    connection.commit()


def _load_data_to_postgres():
    pg_hook = PostgresHook(
        postgres_conn_id="AQI",
        schema="capstone"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)

    timestamp = data["current"]["pollution"]["ts"]
    aqius = data["current"]["pollution"]["aqius"]
    temp = data["weather"]["tp"]
    pressure = data["weather"]["pr"]
    humidity = data["weather"]["hu"]
    wind_speed = data["weather"]["ws"]
    wind_direction = data["weather"]["wd"]

    sql = f"""
        INSERT INTO AQI (timestamp,quius,temp,pressure,humidity,wind_speed,wind_direction) VALUES ({timestamp}, {aqius}, {temp},{pressure},{humidity},{wind_speed},{wind_direction})
    """
    cursor.execute(sql)
    connection.commit()


default_args = {
    "email": ["a.panklai2539@gmail.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    "AQI_DAGS",
    default_args=default_args,
    schedule="1 * * * *",
    start_date=timezone.datetime(2025, 3, 24),
    tags=["dpu"],
):
    start = EmptyOperator(task_id="start")

    get_weather_data = PythonOperator(
        task_id="get_weather_data",
        python_callable=_get_weather_data,
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    create_weather_table = PythonOperator(
        task_id="create_weather_table",
        python_callable=_create_weather_table,
    )

    load_data_to_postgres = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
    )

    send_email = EmailOperator(
        task_id="send_email",
        to=["a.panklai2539@gmail.com"],
        subject="Finished getting open weather data",
        html_content="หนูดึงข้อมูลเสร็จแล้วจ้า น้องAIRFLOW",
    )

    end = EmptyOperator(task_id="end")

    start >> get_weather_data >> validate_data >> load_data_to_postgres >> send_email
    start >> create_weather_table >> load_data_to_postgres
    send_email >> end