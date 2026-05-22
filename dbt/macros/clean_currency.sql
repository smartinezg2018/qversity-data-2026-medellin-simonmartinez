{% macro clean_currency(column_expr) %}
    case
        when regexp_replace(
                regexp_replace(
                    trim({{ column_expr }}),
                    '\$|(\s*USD\s*)', '', 'g'
                ),
                ',', '.', 'g'
             ) ~ '^-?[0-9]+(\.[0-9]+)?([Ee][+-]?[0-9]+)?$'
        then regexp_replace(
                regexp_replace(
                    trim({{ column_expr }}),
                    '\$|(\s*USD\s*)', '', 'g'
                ),
                ',', '.', 'g'
             )::numeric
        else null
    end
{% endmacro %}