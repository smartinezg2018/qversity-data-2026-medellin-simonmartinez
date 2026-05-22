{% macro clamp_numeric(column_expr, min_val, max_val) %}
    case
        when {{ safe_numeric_signed(column_expr) }} between {{ min_val }} and {{ max_val }}
        then {{ safe_numeric_signed(column_expr) }}
        else null
    end
{% endmacro %}