{% macro proper_case(column_expr) %}

    case
        when trim({{ column_expr }}) in ('', 'N/A', 'NA', 'null', 'nan')
            then null
        else initcap(lower(regexp_replace(trim({{ column_expr }}), '\s+', ' ', 'g')))
    end
{% endmacro %}
