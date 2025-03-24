import json
from datetime import timedelta
from datetime import datetime
import requests

def convert_date_string(date_string):
    """แปลง date string 'YYYY-MM-DDTHH:MM:SS.000Z' เป็น 'DDMMYYYY'."""
    dt_object = datetime.fromisoformat(date_string.replace("Z", "+00:00"))
    return dt_object.strftime("%d%m%Y")

def convert_to_1500(date_string):
    """แปลง date string 'YYYY-MM-DDTHH:MM:SS.000Z' เป็น '1500' (UTC+7)."""
    dt_object_utc = datetime.fromisoformat(date_string.replace("Z", "+00:00"))
    dt_object_th = dt_object_utc + timedelta(hours=7)  # ปรับ time zone เป็น UTC+7
    return dt_object_th.strftime("%H%M") # แปลงเป็น 1500

with open(f"/workspaces/DPU_Capstone/dags/data.json", "r") as f:
    data = json.load(f)

date_raw = data["data"]["current"]["pollution"]["ts"]
date = convert_date_string(date_raw)
time_raw =  data["data"]["current"]["pollution"]["ts"]
time = convert_to_1500(time_raw)
aqi = data["data"]["current"]["pollution"]["aqius"]
temp = data["data"]["current"]["weather"]["tp"]
pressure = data["data"]["current"]["weather"]["pr"]
humidity = data["data"]["current"]["weather"]["hu"]
wind_speed = data["data"]["current"]["weather"]["ws"]
wind_direction = data["data"]["current"]["weather"]["wd"]

sql = """
    INSERT INTO AQI (date, time, aqi, temperature, pressure, humidity, wind_speed, wind_direction)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

data_to_insert = (date,time, aqi, temp, pressure, humidity, wind_speed, wind_direction) # Use strftime

print(sql)
print(data_to_insert)