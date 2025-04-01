WITH winddirectioncategories AS (
         SELECT aqi."time",
            round(avg(aqi.wind_direction), 2) AS avg_wind_direction_degrees,
            round(avg(aqi.wind_speed), 2) AS avg_wind_speed,
                CASE
                    WHEN ((avg(aqi.wind_direction) >= 337.5) OR (avg(aqi.wind_direction) < 22.5)) THEN 'N'::text
                    WHEN ((avg(aqi.wind_direction) >= 22.5) AND (avg(aqi.wind_direction) < 67.5)) THEN 'NE'::text
                    WHEN ((avg(aqi.wind_direction) >= 67.5) AND (avg(aqi.wind_direction) < 112.5)) THEN 'E'::text
                    WHEN ((avg(aqi.wind_direction) >= 112.5) AND (avg(aqi.wind_direction) < 157.5)) THEN 'SE'::text
                    WHEN ((avg(aqi.wind_direction) >= 157.5) AND (avg(aqi.wind_direction) < 202.5)) THEN 'S'::text
                    WHEN ((avg(aqi.wind_direction) >= 202.5) AND (avg(aqi.wind_direction) < 247.5)) THEN 'SW'::text
                    WHEN ((avg(aqi.wind_direction) >= 247.5) AND (avg(aqi.wind_direction) < 292.5)) THEN 'W'::text
                    WHEN ((avg(aqi.wind_direction) >= 292.5) AND (avg(aqi.wind_direction) < 337.5)) THEN 'NW'::text
                    ELSE 'Unknown'::text
                END AS wind_direction_cardinal
           FROM {{ source('AQI', 'aqi') }}
          GROUP BY aqi."time"
        )
 SELECT "time",
    avg_wind_speed,
    avg_wind_direction_degrees AS wind_direction_degrees,
    wind_direction_cardinal AS wind_direction
    
   FROM winddirectioncategories
  ORDER BY "time"