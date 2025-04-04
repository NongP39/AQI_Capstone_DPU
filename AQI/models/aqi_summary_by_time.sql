SELECT
        CASE
            WHEN ((EXTRACT(hour FROM timestamp) >= (0)::numeric) AND (EXTRACT(hour FROM  timestamp) < (6)::numeric)) THEN '00:00-05:59'::text
            WHEN ((EXTRACT(hour FROM timestamp) >= (6)::numeric) AND (EXTRACT(hour FROM  timestamp) < (12)::numeric)) THEN '06:00-11:59'::text
            WHEN ((EXTRACT(hour FROM timestamp) >= (12)::numeric) AND (EXTRACT(hour FROM  timestamp) < (18)::numeric)) THEN '12:00-17:59'::text
            WHEN ((EXTRACT(hour FROM timestamp) >= (18)::numeric) AND (EXTRACT(hour FROM  timestamp) < (24)::numeric)) THEN '18:00-23:59'::text
            ELSE 'ไม่ระบุ'::text
        END AS time_group,
    max(aqi) AS max_aqi,
    min(aqi) AS min_aqi,
    avg(aqi) AS avg_aqi
   FROM {{ source('AQI', 'aqi') }}
  GROUP BY
        CASE
            WHEN ((EXTRACT(hour FROM timestamp) >= (0)::numeric) AND (EXTRACT(hour FROM  timestamp) < (6)::numeric)) THEN '00:00-05:59'::text
            WHEN ((EXTRACT(hour FROM timestamp) >= (6)::numeric) AND (EXTRACT(hour FROM  timestamp) < (12)::numeric)) THEN '06:00-11:59'::text
            WHEN ((EXTRACT(hour FROM timestamp) >= (12)::numeric) AND (EXTRACT(hour FROM  timestamp) < (18)::numeric)) THEN '12:00-17:59'::text
            WHEN ((EXTRACT(hour FROM timestamp) >= (18)::numeric) AND (EXTRACT(hour FROM  timestamp) < (24)::numeric)) THEN '18:00-23:59'::text
            ELSE 'ไม่ระบุ'::text
        END
  order by time_group