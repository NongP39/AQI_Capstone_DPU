SELECT
    DATE(date) AS date,
    round(AVG(aqi),2) AS avg_aqi,
    CASE
        WHEN AVG(aqi) BETWEEN 0 AND 50 THEN 'Good'
        WHEN AVG(aqi) BETWEEN 51 AND 100 THEN 'Moderate'
        WHEN AVG(aqi) BETWEEN 101 AND 150 THEN 'Unhealthy for Sensitive Groups'
        WHEN AVG(aqi) BETWEEN 151 AND 200 THEN 'Unhealthy'
        WHEN AVG(aqi) BETWEEN 201 AND 300 THEN 'Very Unhealthy'
        WHEN AVG(aqi) BETWEEN 301 AND 500 THEN 'Hazardous'
        ELSE 'Unknown'
    END AS aqi_level,
    count(aqi) as count
FROM
    {{ source('AQI', 'aqi') }}
GROUP BY
    DATE(date)
ORDER BY
    DATE(date)