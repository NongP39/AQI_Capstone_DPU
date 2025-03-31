import json
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
import shutil
import requests

thai_datetime = None
DAG_FOLDER = "/opt/airflow/dags"


def _get_weather_data():
    
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
    
    assert data.get("status") != "fail" 
    #ใส่ไว้เพื่อเวลา API status:fail Airflow จะได้ retry ดึงข้อมูลใหม่ หากไม่ใส่ข้อมูลจะถูก SKIP ไปดึงข้อมูลรอบถัดไป
    

def _validate_data():
    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)
    val = data["data"]["current"]
    
    if not val["pollution"]["aqius"] > 0: # ค่า AQI ไม่ต่ำกว่า 0
            raise AssertionError("ข้อมูล AQI ผิดพลาด")
    if not 1000 < val["weather"]["pr"] < 1200: # ค่าความกดอากาศไม่ต่ำกว่า 1000 และไม่สูงกว่า 1200 ในระดับพื้นผิว
            raise AssertionError("ข้อมูลความกดอากาศผิดพลาด")
    if not 0 < val["weather"]["tp"] < 45: # ค่าอุณหภูมิอากาศของกรุงเทพไม่มีต่ำกว่า 0 และไม่มีสูงกว่า 45 (แต่อาจจะ)
            raise AssertionError("ข้อมูลอุณหภูมิผิดพลาด")
    if not 0 < val["weather"]["hu"] < 100 : # ค่าความชื้นไม่ต่ำกว่า 0 และไม่เกิน 100
            raise AssertionError("ข้อมูลความชื้นผิดพลาด")
    if not 0 < val["weather"]["wd"] < 360 : # ทิศทางลม หน่วยเป็นองศา ค่าอยู่ระหว่าง 0 - 360
            raise AssertionError("ข้อมูลทิศทางลมผิดพลาด")
    #จากทดลองรันแล้วไม่พบ API ที่มี status:success แต่ข้อมูลภายในมีค่าที่ผิดพลาด จึงเขียน code นี้เป็นตัวอย่างไปก่อน

def _create_weather_table():
    pg_hook = PostgresHook(
        postgres_conn_id="AQI",
        schema="capstone"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
    CREATE TABLE IF NOT EXISTS AQI (
        date VARCHAR NOT NULL,
        time VARCHAR NOT NULL,
        aqi NUMERIC,
        temp NUMERIC,
        pressure NUMERIC,
        humidity NUMERIC,
        wind_speed NUMERIC, 
        wind_direction NUMERIC 
    );
"""
# DAGS นี้เป็นการเก็บข้อมูล AQI จึงใส่ column AQI ให้เป็น NOT NULL
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

    timestamp_str =  data["data"]["current"]["pollution"]["ts"]
    utc_datetime = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    thai_timedelta = timedelta(hours=7) #แปลงเวลา UTC ให้เป็น LOCALTIME (Bangkok: +7)
    thai_datetime = utc_datetime + thai_timedelta
    #แปลง timestampe จาก type: DATETIME ให้เป็น String เพราะตอนลองใส่ด้วย type: DATE และ TIME แล้ว ไม่สามารถรัน code ผ่านได้ เนื่องจาก Error DATATYPE
    date = thai_datetime.strftime('%d%m%Y')
    time = thai_datetime.strftime('%H%M')
    aqi = data["data"]["current"]["pollution"]["aqius"]
    temp = data["data"]["current"]["weather"]["tp"]
    pressure = data["data"]["current"]["weather"]["pr"]
    humidity = data["data"]["current"]["weather"]["hu"]
    wind_speed = data["data"]["current"]["weather"]["ws"]
    wind_direction = data["data"]["current"]["weather"]["wd"]
    
    sql = f"""
         INSERT INTO AQI (date, time, aqi, temp, pressure, humidity, wind_speed, wind_direction)
         VALUES ({date},{time},{aqi},{temp},{pressure},{humidity},{wind_speed},{wind_direction});
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
    schedule="25 * * * *",   # ให้ DAGS รันเก็บข้อมูลจาก API ทุกนาทีที่ 25 ของทุกชั่วโมง ที่ดึงข้อมูลช้าเนื่องจากลองดึงเวลาก่อนหน้าแล้วข้อมูลจะได้เป็นข้อมูลของชั่วโมงก่อนหน้า
    start_date=timezone.datetime(2025, 3, 24),
    tags=["dpu","capstone"],
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
        html_content = "DAGS AQI ดำเนินการดึงข้อมูลเรียบร้อย",
    )

    end = EmptyOperator(task_id="end")

    start >> get_weather_data >> validate_data >> load_data_to_postgres >> send_email
    start >> create_weather_table >> load_data_to_postgres
    send_email >> end