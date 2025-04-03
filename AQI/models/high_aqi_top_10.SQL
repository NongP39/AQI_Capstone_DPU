SELECT timestamp,
    aqi,
    temp,
    pressure,
    humidity,
    wind_speed
   FROM {{ source('AQI', 'aqi') }}
  ORDER BY aqi DESC
 LIMIT 10