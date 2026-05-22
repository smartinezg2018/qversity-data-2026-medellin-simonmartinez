{% macro risk_tier(risk_score_column) %}
    case
        when {{ risk_score_column }} is null then 'Unknown'
        when {{ risk_score_column }} <= 25 then 'Low'
        when {{ risk_score_column }} <= 50 then 'Medium'
        when {{ risk_score_column }} <= 75 then 'High'
        else 'Critical'
    end
{% endmacro %}
