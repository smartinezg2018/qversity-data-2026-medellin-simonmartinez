{% macro safe_string(column_expr) %}
    case
        when trim({{column_expr}}) in ('', 'N/A', 'NA', 'null', 'nan')
            then null
        else trim({{column_expr}})
    end
{% endmacro %}