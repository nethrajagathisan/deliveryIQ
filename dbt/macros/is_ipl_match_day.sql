{% macro is_ipl_match_day(date_col, city_col) %}
  EXISTS (
    SELECT 1 FROM {{ ref('stg_ipl_schedule') }} ipl
    WHERE ipl.match_date = {{ date_col }}
      AND ipl.venue_city = {{ city_col }}
      AND ipl.is_deliveryiq_city
  )
{% endmacro %}
{# TRUE when an IPL match is hosted in the given city on the given date #}
