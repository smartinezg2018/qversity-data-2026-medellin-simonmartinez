{% macro is_valid_email(column_expr) %}
{#
    Validates if a cleaned email address has a valid format.
    
    Returns false if:
    1. It is null
    2. It contains any whitespace character
    3. It starts or ends with a point/dot (.)
    4. It does not contain exactly one '@'
    5. The '@' is at the start or end of the email
    6. It does not match the standard email pattern
    
    Otherwise, returns true.
#}
    case
        when {{ column_expr }} is null then false
        -- Check for spaces (internal or external)
        when {{ column_expr }} ~ '[[:space:]]' then false
        -- Check for leading/trailing dots
        when {{ column_expr }} like '.%' or {{ column_expr }} like '%.' then false
        -- Check for bad positioned '@' (must have exactly one '@', and not at start/end)
        when {{ column_expr }} not like '%@%'
             or {{ column_expr }} like '@%'
             or {{ column_expr }} like '%@'
             or length({{ column_expr }}) - length(replace({{ column_expr }}, '@', '')) != 1 then false
        -- Check standard format (local_part@domain.extension)
        when {{ column_expr }} !~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+[.][a-zA-Z]{2,}$' then false
        else true
    end
{% endmacro %}
