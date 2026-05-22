{{ config(materialized='table') }}

with base as (
    select
        customer_id,
        credit_info::json as de
    from {{ source('silver', 'stg_customers') }}
    where credit_info is not null
      and trim(credit_info) not in ('', 'null', '{}')
),

cleaned as (
    select
        customer_id,
        {{ clamp_numeric_int("de->>'credit_score'", 350, 850) }}            as credit_score,
        {{ clamp_numeric("de->>'utilization_pct'", 0, 100) }}                 as utilization_pct,
        {{ clean_currency("de->>'total_limit'") }}                            as total_limit,
        {{ clean_currency("de->>'total_used'") }}                             as total_used,
        {{ safe_numeric_int("de->>'num_credit_accounts'") }}                as num_credit_accounts,
        {{ safe_numeric_int("de->>'oldest_account_age_months'") }}           as oldest_account_age_months,
        {{ safe_numeric_int("de->>'late_payments_12m'") }}                    as late_payments_12m,
        {{ safe_numeric_int("de->>'inquiries_6m'") }}                         as inquiries_6m,
        {{ cast_to_boolean("de->>'bankruptcy_flag'") }}                       as bankruptcy_flag
    from base
)

select
    customer_id,
    credit_score,
    {{ credit_score_bucket('credit_score') }}                               as credit_score_bucket,
    utilization_pct,
    {{ utilization_bucket('utilization_pct') }}                             as utilization_bucket,
    total_limit,
    total_used,
    num_credit_accounts,
    oldest_account_age_months,
    late_payments_12m,
    inquiries_6m,
    bankruptcy_flag
from cleaned
