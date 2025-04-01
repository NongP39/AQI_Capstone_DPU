SELECT date,
    "time",
    aqi,
    temp,
    pressure,
    humidity,
    wind_speed
   FROM {{ source('AQI', 'aqi') }}
  ORDER BY aqi DESC
 LIMIT 5