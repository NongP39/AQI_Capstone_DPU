SELECT
    'aqi-temp' AS correlation_pair,
    (SUM((aqi - avg_aqi) * (temp - avg_temp))) / (SQRT(SUM(POWER(aqi - avg_aqi, 2)) * SUM(POWER(temp - avg_temp, 2)))) AS pearson_correlation
FROM (
    SELECT
        aqi,
        temp,
        AVG(aqi) OVER () AS avg_aqi,
        AVG(temp) OVER () AS avg_temp
    FROM {{ source('AQI', 'aqi') }}
) AS subquery
UNION ALL
SELECT
    'aqi-pressure' AS correlation_pair,
    (SUM((aqi - avg_aqi) * (pressure - avg_pressure))) / (SQRT(SUM(POWER(aqi - avg_aqi, 2)) * SUM(POWER(pressure - avg_pressure, 2)))) AS pearson_correlation
FROM (
    SELECT
        aqi,
        pressure,
        AVG(aqi) OVER () AS avg_aqi,
        AVG(pressure) OVER () AS avg_pressure
    FROM {{ source('AQI', 'aqi') }}
) AS subquery
UNION ALL
SELECT
    'aqi-humidity' AS correlation_pair,
    (SUM((aqi - avg_aqi) * (humidity - avg_humidity))) / (SQRT(SUM(POWER(aqi - avg_aqi, 2)) * SUM(POWER(humidity - avg_humidity, 2)))) AS pearson_correlation
FROM (
    SELECT
        aqi,
        humidity,
        AVG(aqi) OVER () AS avg_aqi,
        AVG(humidity) OVER () AS avg_humidity
    FROM {{ source('AQI', 'aqi') }}
) AS subquery
UNION ALL
SELECT
    'aqi-wind_speed' AS correlation_pair,
    (SUM((aqi - avg_aqi) * (wind_speed - avg_wind_speed))) / (SQRT(SUM(POWER(aqi - avg_aqi, 2)) * SUM(POWER(wind_speed - avg_wind_speed, 2)))) AS pearson_correlation
FROM (
    SELECT
        aqi,
        wind_speed,
        AVG(aqi) OVER () AS avg_aqi,
        AVG(wind_speed) OVER () AS avg_wind_speed
    FROM {{ source('AQI', 'aqi') }}
) AS subquery
UNION ALL
SELECT
    'aqi-wind_direction' AS correlation_pair,
    (SUM((aqi - avg_aqi) * (wind_direction - avg_wind_direction))) / (SQRT(SUM(POWER(aqi - avg_aqi, 2)) * SUM(POWER(wind_direction - avg_wind_direction, 2)))) AS pearson_correlation
FROM (
    SELECT
        aqi,
        wind_direction,
        AVG(aqi) OVER () AS avg_aqi,
        AVG(wind_direction) OVER () AS avg_wind_direction
    FROM {{ source('AQI', 'aqi') }}
) AS subquery