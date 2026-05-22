{% macro safe_numeric_int(column_expr) %}
    case
        when trim({{ column_expr }}) in ('', 'N/A', 'NA', 'null', 'nan')
            then null
        when trim({{ column_expr }}) ~ '^[+-]?\d+(\.\d+)?$'
            then trim({{ column_expr }})::int
        else null
    end
{% endmacro %}
