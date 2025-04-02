SELECT
    date,
    time,
    aqi,
    temp,
    pressure,
    humidity,
    wind_speed
FROM {{ ref("high_aqi_top_5") }}
WHERE
    date IS NULL
    OR time IS NULL
    OR aqi < 0
    OR (temp < 0 OR temp > 45)
    OR (pressure < 980 OR pressure > 1050)
    OR (humidity < 0 OR humidity > 100)
    OR wind_speed < 0