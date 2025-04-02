SELECT 
    correlation_pair,
    pearson_correlation
FROM {{ref("aqi_correlation")}}
WHERE 
    pearson_correlation > 1 or
    pearson_correlation < -1