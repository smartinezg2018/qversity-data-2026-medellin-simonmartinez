{% macro map_from_seed(column_name, seed_name, raw_col='raw_value', norm_col='normalized_value') %}

    {% set mapping_query %}
        select {{ raw_col }} as raw_val, {{ norm_col }} as norm_val
        from {{ ref(seed_name) }}
    {% endset %}

    {% set results = run_query(mapping_query) %}

    {% if execute %}
        {% set results_list = results.rows %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}

    case
        {% for row in results_list %}
        when lower(trim({{ column_name }})) = lower('{{ row.raw_val | replace("'", "''") }}')
            then '{{ row.norm_val | replace("'", "''") }}'
        {% endfor %}
        else trim({{ column_name }})
    end

{% endmacro %}