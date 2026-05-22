{% macro calculate_years(date_column) %}
    case
        when {{ date_column }} is null then null
        else date_part('year', age(current_date, {{ date_column }}))::int
    end
{% endmacro %}
