-- depends_on: {{ ref('account_status_mapping') }}
-- depends_on: {{ ref('currency_to_usd') }}
{{ config(materialized='table') }}

with base as (
    select *
    from {{ source('silver', 'stg_accounts') }}
),

cleaned as (
    select
        customer_id,
        account_id,
        {{ safe_string('account_type') }}                                   as account_type,
        currency,
        {{ clean_currency('balance') }}                                     as balance,
        {{ clean_currency('credit_limit') }}                                as credit_limit,
        {{ clamp_numeric('interest_rate', 0, 100) }}                        as interest_rate,
        {{ parse_date('opened_date') }}                                     as opened_date,
        {{ map_from_seed('status', 'account_status_mapping') }}             as status,
        branch_code
    from base
)

select
    customer_id,
    account_id,
    account_type,
    currency,
    balance,
    {{ to_usd('balance', 'currency') }}                                       as balance_usd,
    credit_limit,
    {{ to_usd('credit_limit', 'currency') }}                                  as credit_limit_usd,
    interest_rate,
    opened_date,
    {{ date_key('opened_date') }}                                           as opened_date_key,
    status,
    branch_code
from cleaned
