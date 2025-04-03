import json
from datetime import timedelta
from datetime import datetime
import requests




with open(f"/workspaces/DPU_Capstone/dags/data.json", "r") as f:
    data = json.load(f)

timestamp = data["data"]["current"]["pollution"]["ts"]
utc_datetime = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
thai_timedelta = timedelta(hours=7)
thai_datetime = utc_datetime + thai_timedelta


aqi = data["data"]["current"]["pollution"]["aqius"]
temp = data["data"]["current"]["weather"]["tp"]
pressure = data["data"]["current"]["weather"]["pr"]
humidity = data["data"]["current"]["weather"]["hu"]
wind_speed = data["data"]["current"]["weather"]["ws"]
wind_direction = data["data"]["current"]["weather"]["wd"]

print(type(thai_datetime))
print(thai_datetime)
print(type(thai_datetime))
print(thai_datetime)
