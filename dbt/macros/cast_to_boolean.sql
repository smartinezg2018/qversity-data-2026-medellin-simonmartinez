{% macro cast_to_boolean(column_name) %}
    case
        when lower(trim({{ column_name }})) in ('true', '1', 'si', 'y', 'yes')  then true
        when lower(trim({{ column_name }})) in ('false', '0', 'no', 'n') then false
        else null
    end
{% endmacro %}
