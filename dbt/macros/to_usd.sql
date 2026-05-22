{% macro to_usd(amount_expr, currency_expr) %}

    {% set rates_query %}
        select currency_code, usd_per_unit
        from {{ ref('currency_to_usd') }}
    {% endset %}

    {% set results = run_query(rates_query) %}

    {% if execute %}
        {% set results_list = results.rows %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}

    case
        when {{ amount_expr }} is null then null
        {% for row in results_list %}
        when upper(trim({{ currency_expr }})) = upper('{{ row.currency_code }}')
            then ({{ amount_expr }} * {{ row.usd_per_unit }}::numeric)
        {% endfor %}
        else null
    end

{% endmacro %}
