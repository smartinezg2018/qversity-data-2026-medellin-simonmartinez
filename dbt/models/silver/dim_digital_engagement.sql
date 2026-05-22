{{ config(materialized='table') }}

with base as (
    select
        customer_id,
        digital_engagement::json as de
    from {{ source('silver', 'stg_customers') }}
    where digital_engagement is not null
      and trim(digital_engagement) not in ('', 'null', '{}')
),

cleaned as (
    select
        customer_id,
        {{ cast_to_boolean("de->>'mobile_app_registered'") }}              as mobile_app_registered,
        {{ cast_to_boolean("de->>'web_banking_registered'") }}             as web_banking_registered,
        {{ parse_date("de->>'last_login_date'") }}                          as last_login_date,
        {{ safe_numeric("de->>'avg_monthly_logins'") }}                   as avg_monthly_logins,
        {{ cast_to_boolean("de->>'push_notifications'") }}                  as push_notifications,
        {{ cast_to_boolean("de->>'paperless_statements'") }}                as paperless_statements,
        de->>'preferred_channel'                                            as preferred_channel
    from base
)

select
    customer_id,
    mobile_app_registered,
    web_banking_registered,
    last_login_date,
    {{ date_key('last_login_date') }}                                       as last_login_date_key,
    avg_monthly_logins,
    push_notifications,
    paperless_statements,
    preferred_channel
from cleaned
