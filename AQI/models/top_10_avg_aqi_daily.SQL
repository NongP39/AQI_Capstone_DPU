SELECT date(timestamp) AS date,
       avg(aqi) as avg_aqi,
       avg(temp) as avg_temp,
       avg(pressure) as avg_pressure,
       avg(humidity) as avg_humidity,
       avg(wind_speed) as avg_wind_speed
FROM {{ source('AQI', 'aqi') }}
GROUP BY date(timestamp)
ORDER BY avg_aqi DESC
LIMIT 10