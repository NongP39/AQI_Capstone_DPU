SELECT
    date,
    aqi,
    aqi_level
FROM
    {{ref("level_of_aqi")}}
WHERE 
    date is null or
    aqi < 0 or
    aqi_level is null 