# DPU_Capstone
build data pipeline for AQI
1. build DAGS
   1.1  ปัญหาเรื่อง create table (แก้แล้ว)
   1.2  airflow มีปัญหาความถี่ในการรัน code ที่มากเกินไป (กำลังทดสอบ)
   1.3  ข้อมูลที่ทำการดึงจาก DAGS เป็นข้อมูลปัจจุบันเท่านั้น ไม่สามารถดึงข้อมูลจากอดีตได้
