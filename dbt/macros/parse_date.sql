{% macro parse_date(column_expr) %}
    case
        when {{ column_expr }} ~ '^\d{4}-\d{2}-\d{2}$'
            then ({{ column_expr }})::date
        when {{ column_expr }} ~ '^\d{8}$'
            then to_date({{ column_expr }}, 'YYYYMMDD')
        when {{ column_expr }} ~ '^\d{2}-\d{2}-\d{4}$'
            then to_date({{ column_expr }}, 'MM-DD-YYYY')
        when {{ column_expr }} ~ '^\d{2}/\d{2}/\d{4}$'
            then to_date({{ column_expr }}, 'DD/MM/YYYY')
        else null
    end
{% endmacro %}