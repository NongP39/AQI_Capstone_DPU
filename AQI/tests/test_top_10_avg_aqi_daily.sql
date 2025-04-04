SELECT
    date,
    avg_aqi,
    avg_temp,
    avg_pressure,
    avg_humidity,
    avg_wind_speed
FROM {{ ref("top_10_avg_aqi_daily") }}
WHERE
    date IS NULL
    OR avg_aqi < 0
    OR (avg_temp < 0 OR avg_temp > 45)
    OR (avg_pressure < 980 OR avg_pressure > 1050)
    OR (avg_humidity < 0 OR avg_humidity > 100)
    OR avg_wind_speed < 0