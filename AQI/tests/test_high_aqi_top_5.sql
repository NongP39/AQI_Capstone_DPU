SELECT
    timestamp,
    aqi,
    temp,
    pressure,
    humidity,
    wind_speed
FROM {{ ref("high_aqi_top_10") }}
WHERE
    timestamp IS NULL
    OR aqi < 0
    OR (temp < 0 OR temp > 45)
    OR (pressure < 980 OR pressure > 1050)
    OR (humidity < 0 OR humidity > 100)
    OR wind_speed < 0