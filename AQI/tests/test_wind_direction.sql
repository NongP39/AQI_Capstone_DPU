SELECT
    timestamp,
    wind_speed,
    wind_direction_degrees,
    wind_direction
FROM {{ref("wind_direction")}}
WHERE 
    timestamp is null or
    wind_speed < 0 or
    wind_direction_degrees < 0 or
    wind_direction_degrees > 360 
