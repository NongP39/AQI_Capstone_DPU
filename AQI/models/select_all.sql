{{ config(
    materialized='table'
) }}

{% if target.name != 'production' %}
    {# Do nothing when not in production #}
SELECT
    *
   FROM {{ source('AQI', 'aqi') }}
{% endif %}