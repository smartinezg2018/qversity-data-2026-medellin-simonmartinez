{% macro credit_score_bucket(credit_score_column) %}
    case
        when {{ credit_score_column }} is null then 'Unknown'
        when {{ credit_score_column }} < 580 then 'Poor'
        when {{ credit_score_column }} < 670 then 'Fair'
        when {{ credit_score_column }} < 740 then 'Good'
        when {{ credit_score_column }} < 800 then 'Very Good'
        else 'Excellent'
    end
{% endmacro %}
