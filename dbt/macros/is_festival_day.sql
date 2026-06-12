{% macro is_festival_day(date_col, major_only=false) %}
  EXISTS (
    SELECT 1 FROM {{ ref('stg_festival_calendar') }} fc
    WHERE fc.festival_date = {{ date_col }}
    {% if major_only %}
      AND fc.is_major
    {% endif %}
  )
{% endmacro %}
{# TRUE when the given date matches a festival/holiday (optionally major-only) #}
