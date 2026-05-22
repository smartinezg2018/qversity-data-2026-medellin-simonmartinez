{% macro utilization_bucket(utilization_column) %}
    case
        when {{ utilization_column }} is null then 'Unknown'
        when {{ utilization_column }} <= 30 then 'Low'
        when {{ utilization_column }} <= 50 then 'Moderate'
        when {{ utilization_column }} <= 75 then 'High'
        else 'Very High'
    end
{% endmacro %}
