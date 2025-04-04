WITH winddirectioncategories AS (
    SELECT timestamp,
           aqi.wind_direction AS direction_degrees,
           aqi.wind_speed AS wind_speed,
           CASE
               WHEN (aqi.wind_direction >= 337.5) OR (aqi.wind_direction < 22.5) THEN 'N'::text
               WHEN (aqi.wind_direction >= 22.5) AND (aqi.wind_direction < 67.5) THEN 'NE'::text
               WHEN (aqi.wind_direction >= 67.5) AND (aqi.wind_direction < 112.5) THEN 'E'::text
               WHEN (aqi.wind_direction >= 112.5) AND (aqi.wind_direction < 157.5) THEN 'SE'::text
               WHEN (aqi.wind_direction >= 157.5) AND (aqi.wind_direction < 202.5) THEN 'S'::text
               WHEN (aqi.wind_direction >= 202.5) AND (aqi.wind_direction < 247.5) THEN 'SW'::text
               WHEN (aqi.wind_direction >= 247.5) AND (aqi.wind_direction < 292.5) THEN 'W'::text
               WHEN (aqi.wind_direction >= 292.5) AND (aqi.wind_direction < 337.5) THEN 'NW'::text
               ELSE 'Unknown'::text
           END AS wind_direction_cardinal
    FROM {{ source('AQI', 'aqi') }}
)
SELECT timestamp,
       wind_speed,
       direction_degrees AS wind_direction_degrees,
       wind_direction_cardinal AS wind_direction
FROM winddirectioncategories
ORDER BY timestamp desc