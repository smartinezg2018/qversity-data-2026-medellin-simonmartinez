{% macro clean_email(column_expr) %}
{#
    Cleans raw email addresses by applying the following steps:
    1. Nullifies empty / sentinel values
    2. Lowercases the entire string
    3. Strips ALL whitespace (spaces in domain or local part)
    4. Removes '#' characters (noise)
    5. Collapses multiple '@' into a single '@'
    6. Trims leading/trailing dots

    Emails that remain structurally invalid after cleaning
    are caught downstream by the is_valid_email() macro.
#}
    case
        when trim({{ column_expr }}) in ('', 'N/A', 'NA', 'null', 'nan') then null
        else
            trim(both '.' from
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower(trim({{ column_expr }})),
                            '\s+', '', 'g'
                        ),
                        '#', '', 'g'
                    ),
                    '@+', '@', 'g'
                )
            )
    end
{% endmacro %}
