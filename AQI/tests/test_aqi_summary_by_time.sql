SELECT 
    time_group,
    max_aqi,
    min_aqi,
    avg_aqi
FROM {{ref("aqi_summary_by_time")}}
WHERE 
    max_aqi < 0 and
    min_aqi < 0 and
    avg_aqi < 0 and
    time_group is null