SELECT 
    time_group,
    max_aqi,
    min_aqi,
    avg_aqi
FROM {{ref("aqi_summary_by_time")}}
WHERE 
    max_aqi < 0 or
    min_aqi < 0 or
    avg_aqi < 0 or
    time_group is null