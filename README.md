# AQI_Capstone_DPU
**Project Description**

**AQI Data Pipeline** ในการดึงข้อมูลจาก **Air Visual** ด้วย Apache Airflow ผ่าน API แล้วนำข้อมูลที่ได้ไปเก็บไว้ในฐานข้อมูล PostgreSQL ซึ่ง Project นี้ เป็นต้นแบบที่จะนำไปต่อยอดในการทำระบบพยากรณ์คุณภาพอากาศในอนาคต

1.  **ขั้นตอนการดึงข้อมูล**
สร้าง DAGS และทำการรัน DAGS ไว้บน Apache Airflow เพื่อทำการดึงข้อมูลจาก Air Visual ผ่านทาง API ทุกนาทีที่ 25 ของทุกชั่วโมง

2.  **ขั้นตอนการตรวจสอบข้อมูล**
ในการตรวจสอบความถูกต้องของข้อมูลที่ได้รับมาจาก API ซึ่งได้กำหนด Range ไว้ดังต่อไปนี
-  **AQI** : ค่าไม่ต่ำกว่า 0
-  **Temperature** : มีค่าอยู่ระหว่าง 0 ถึง 45 องศาเซลเซียส (°C)
-  **Pressure** : มีค่าอยู่ระหว่าง 980 ถึง 1050 (ความอากาศในระดับพื้นผิว)
-  **Wind Direction** : มีค่าอยู่ระหว่าง 0 - 360 องศา (มุม)
-  **Humidity(%)** : มีค่าอยู่ที่ 0 - 100 (เปอร์เซ็น)
ซึ่งหากข้อมูลที่ได้รับจาก API มีค่าที่อยู่นอกเหนือจาก Range ที่กำหนด DAGS จะทำได้แจ้งเตือน Error ที่ Airflow ทันที

3. **ดึงข้อมูลเข้าฐานข้อมูล PostgreSQL**
ทำการแยกข้อมูล Timestamp UTC ออกเป็น Date และ Time โดยข้อมูลที่แปลงเป็นข้อมูลที่อยู่ในรูปแบบ LocalTime ของประเทศไทย จากนั้นดึงข้อมูลเข้า **Table name** : AQI ใน **schema** : capstone โดยข้อมูลที่ทำการดึงได้แก่
-  **date** : ข้อมูลวัน (ประเทศไทย)
-  **time** : ข้อมูลเวลา (ประเทศไทย)
-  **aqi** : ค่าคุณภาพอากาศ
-  **temp** : อุณหภูมิ (°C)
-  **pressure** : ความกดอากาศ (hPa)
-  **humidity** : ความชื้น (%)
-  **wind_speed** : ความเร็วลม (m/s)
-  **wind_direction** : ทิศทางลม (องศา)

4. **Bussiness Questions**
- วันและเวลาที่มีค่าคุณภาพอากาศสูงที่สุด 5 อันดับ (high_aqi_top_5)
- ค่าคุณภาพอากาศ(เฉลี่ย,สูงสุด,ต่ำสุด) ตามช่วงเวลา (aqi_summary_by_time)
-
-
-

5. **DBT**

**หากต้องการรัน DBT ต้องย้าย directory บน terminal ไปที่ folder : AQI ก่อน **
`$cd /AQI`

ได้ทำการ setup DBT และทำการดึงข้อมูลจาก **schema : public** แยกออกไปเป็น 2 ส่วน ได้แก่
-  **ข้อมูล process** ไว้ที่ **schema : dbt_nongp** ใช้สำหรับ process data ก่อนนำขึ้น production
-  **ข้อมูล Production** ไว้ที่ **schema : Production** ใช้สำหรับนำไปทำ Dashboard หรืออื่นๆ

จากนั้นสร้างและเก็บ SQL statement เพื่อตอบคำถาม Bussiness Questions ไว้ที่ `/workspaces/DPU_Capstone/AQI/models`และทำการรันด้วยคำสั่ง `dbt run` เพื่อดึงข้อมูลด้วย SQL statement ขึ้นมาอยู่ในข้อมูลในส่วนของ process ก่อน เมื่อ**ตรวจสอบ/แก้ไข**ข้อมูลให้พร้อมที่จะนำไปใช้งานให้รันคำสั่ง `dbt run -t production`เพื่อดึงข้อมูลขึ้นไปบน Production


dbt docs generate
dbt docs serve --port 9090
dbt test
note
สร้าง dbt เรียบร้อยสามารถเปิดเว็บได้แล้ว เป้าต่อไปทำ ใส่ description dbt, ทำคำถามที่เหลือ, อัดวิดีโอ, ทำ dashboard, export data ที่เก็บไว้เผื่อ...


ชื่อ: นาย-------------- รหัสนิสิต: 67130---