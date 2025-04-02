SELECT
    date,
    avg_aqi,
    aqi_level,
    count
FROM
    {{ref("avg_level_of_aqi")}}
WHERE 
    date is null or
    avg_aqi < 0 or
    aqi_level is null or
    count is null