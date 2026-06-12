{% macro classify_weather(precipitation_col, temp_max_col) %}
  CASE
    WHEN {{ precipitation_col }} > 15 THEN 'heavy_rain'
    WHEN {{ precipitation_col }} > 2.5 THEN 'light_rain'
    WHEN {{ temp_max_col }} > 40 THEN 'extreme_heat'
    WHEN {{ temp_max_col }} > 35 THEN 'hot'
    ELSE 'normal'
  END
{% endmacro %}
