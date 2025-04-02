# AQI_Capstone_DPU
**Project Description**
<p align="center">
  <img src="https://img.shields.io/badge/Airflow-v2.10.4-blue" alt="Airflow Version">
  <img src="https://img.shields.io/badge/PostgreSQL-v14-green" alt="PostgreSQL Version">
  <img src="https://img.shields.io/badge/DBT-v1.9.3-orange" alt="DBT Version">
</p>
<p align="center">
   สร้างระบบ Data Pipeline ดึงข้อมูลคุณภาพอากาศจาก Air Visual API ด้วย Apache Airflow สู่ PostgreSQL และวิเคราะห์ข้อมูลด้วย DBT เพื่อการพยากรณ์คุณภาพอากาศในอนาคต! 
</p>

## **ขั้นตอนการดึงข้อมูล**
สร้าง DAGS และทำการรัน DAGS ไว้บน Apache Airflow เพื่อทำการดึงข้อมูลจาก Air Visual ผ่านทาง API ทุกนาทีที่ 25 ของทุกชั่วโมง

## **ขั้นตอนการตรวจสอบข้อมูล**
ในการตรวจสอบความถูกต้องของข้อมูลที่ได้รับมาจาก API ซึ่งได้กำหนด Range ไว้ดังต่อไปนี
-  **AQI** : ค่าไม่ต่ำกว่า 0
-  **Temperature** : มีค่าอยู่ระหว่าง 0 ถึง 45 องศาเซลเซียส (°C)
-  **Pressure** : มีค่าอยู่ระหว่าง 980 ถึง 1050 (ความอากาศในระดับพื้นผิว)
-  **Wind Direction** : มีค่าอยู่ระหว่าง 0 - 360 องศา (มุม)
-  **Humidity(%)** : มีค่าอยู่ที่ 0 - 100 (เปอร์เซ็น)
ซึ่งหากข้อมูลที่ได้รับจาก API มีค่าที่อยู่นอกเหนือจาก Range ที่กำหนด DAGS จะทำได้แจ้งเตือน Error ที่ Airflow ทันที

## **ดึงข้อมูลเข้าฐานข้อมูล PostgreSQL**
ทำการแยกข้อมูล Timestamp UTC ออกเป็น Date และ Time โดยข้อมูลที่แปลงเป็นข้อมูลที่อยู่ในรูปแบบ LocalTime ของประเทศไทย จากนั้นดึงข้อมูลเข้า **Table name** : AQI ใน **schema** : capstone โดยข้อมูลที่ทำการดึงได้แก่
-  **date** : ข้อมูลวัน (ประเทศไทย)
-  **time** : ข้อมูลเวลา (ประเทศไทย)
-  **aqi** : ค่าคุณภาพอากาศ
-  **temp** : อุณหภูมิ (°C)
-  **pressure** : ความกดอากาศ (hPa)
-  **humidity** : ความชื้น (%)
-  **wind_speed** : ความเร็วลม (m/s)
-  **wind_direction** : ทิศทางลม (องศา)

## ❓**Bussiness Questions**
- วันและเวลาที่มีค่าคุณภาพอากาศสูงที่สุด 5 อันดับ (high_aqi_top_5)
- ค่าคุณภาพอากาศ(เฉลี่ย,สูงสุด,ต่ำสุด) ตามช่วงเวลา (aqi_summary_by_time)
- ทิศทางกระแสลมเฉลี่ย และความเร็วลมเฉลี่ย แบ่งตามช่วงเวลา (avg_wind)
-
-

## **DBT**

**หากต้องการรัน DBT ต้องย้าย directory บน terminal ไปที่ folder : AQI ก่อน **
`$cd /AQI`

ได้ทำการ setup DBT และทำการดึงข้อมูลจาก **schema : public** แยกออกไปเป็น 2 ส่วน ได้แก่
-  **ข้อมูล process** ไว้ที่ **schema : dbt_nongp** ใช้สำหรับ process data ก่อนนำขึ้น production
-  **ข้อมูล Production** ไว้ที่ **schema : Production** ใช้สำหรับนำไปทำ Dashboard หรืออื่นๆ

จากนั้นสร้างและเก็บ SQL statement เพื่อตอบคำถาม Bussiness Questions ไว้ที่ `/workspaces/DPU_Capstone/AQI/models`\
และทำการรันด้วยคำสั่ง `dbt run` เพื่อดึงข้อมูลด้วย SQL statement ขึ้นมาอยู่ในข้อมูลในส่วนของ process ก่อน \
เมื่อ**ตรวจสอบ/แก้ไข**ข้อมูลให้พร้อมที่จะนำไปใช้งานให้รันคำสั่ง `dbt run -t production`เพื่อดึงข้อมูลขึ้นไปบน Production


## ⚙️ DBT Setup
```bash
# เปลี่ยน Directory ไปที่ Folder AQI
$ cd /AQI
# รัน DBT เพื่อสร้าง Data Models ใน Schema "dbt_nongp"
$ dbt run
# รัน DBT เพื่อสร้าง Data Models ใน Schema "Production"
$ dbt run -t production
# สร้าง Documentation ของ DBT
$ dbt docs generate
# เปิด DBT Documentation ใน Browser
$ dbt docs serve --port 9090
# รัน DBT Tests
$ dbt test
```

