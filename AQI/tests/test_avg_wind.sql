SELECT
    time,
    avg_wind_speed,
    wind_direction_degrees,
    wind_direction
FROM {{ref("avg_wind")}}
WHERE 
    time is null and
    avg_wind_speed < 0 and
    wind_direction_degrees < 0 and
    wind_direction_degrees > 360 
