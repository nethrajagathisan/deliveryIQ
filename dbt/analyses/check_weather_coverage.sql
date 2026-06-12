-- Analysis (not materialized): weather-orders join completeness check.
-- For every (city, order_date) pair in stg_orders, verify a matching weather
-- record exists in stg_weather. Any rows returned are gaps in weather coverage.

SELECT
    o.city,
    o.order_date,
    COUNT(*) as orders_without_weather
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_weather') }} w
    ON o.city = w.city AND o.order_date = w.weather_date
WHERE w.city IS NULL
GROUP BY 1, 2
HAVING COUNT(*) > 0
ORDER BY 3 DESC
