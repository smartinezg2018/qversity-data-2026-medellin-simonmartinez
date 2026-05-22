{% macro date_key(date_column) %}
    cast(to_char({{ date_column }}::date, 'YYYYMMDD') as integer)
{% endmacro %}
