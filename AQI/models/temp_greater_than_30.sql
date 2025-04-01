select 
    *

from {{ source('AQI', 'aqi') }}
where temp > 30