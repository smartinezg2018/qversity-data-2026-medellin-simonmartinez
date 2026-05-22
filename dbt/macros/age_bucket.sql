{% macro age_bucket(age_column) %}
    case
        when {{ age_column }} is null then 'Unknown'
        when {{ age_column }} < 18 then 'Under 18'
        when {{ age_column }} between 18 and 25 then '18-25'
        when {{ age_column }} between 26 and 35 then '26-35'
        when {{ age_column }} between 36 and 45 then '36-45'
        when {{ age_column }} between 46 and 55 then '46-55'
        when {{ age_column }} between 56 and 65 then '56-65'
        when {{ age_column }} between 66 and 75 then '66-75'
        else '76+'
    end
{% endmacro %}