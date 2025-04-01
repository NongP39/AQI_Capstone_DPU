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
    AND time IS NULL
    AND aqi < 0
    AND (temp < 0 OR temp > 45)
    AND (pressure < 980 OR pressure > 1050)
    AND (humidity < 0 OR humidity > 100)
    AND wind_speed < 0