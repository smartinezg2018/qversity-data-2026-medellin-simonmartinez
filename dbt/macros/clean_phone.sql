{% macro clean_phone(column_expr) %}

    case
        when trim({{ column_expr }}) in ('', 'N/A', 'NA', 'null', 'nan')
            then null
        when regexp_replace(trim({{ column_expr }}), '[\s\-\(\)]', '', 'g')
             ~ '^\+\d{10,15}$'
            then regexp_replace(trim({{ column_expr }}), '[\s\-\(\)]', '', 'g')
        else null
    end
{% endmacro %}
