-- depends_on: {{ ref('loan_type_mapping') }}
-- depends_on: {{ ref('loan_status_mapping') }}
-- depends_on: {{ ref('country_to_currency') }}
-- depends_on: {{ ref('currency_to_usd') }}
{{ config(materialized='table') }}

with base as (
    select *
    from {{ source('silver', 'stg_loans') }}
),

cleaned as (
    select
        customer_id,
        loan_id,
        {{ map_from_seed('type', 'loan_type_mapping') }}                    as type,
        {{ clean_currency('principal') }}                                   as principal,
        {{ clean_currency('outstanding_balance') }}                         as outstanding_balance,
        {{ clamp_numeric('interest_rate', 0, 100) }}                        as interest_rate,
        {{ safe_numeric_int('term_months') }}                               as term_months,
        {{ clean_currency('monthly_payment') }}                           as monthly_payment,
        {{ parse_date('start_date') }}                                      as start_date,
        {{ parse_date('end_date') }}                                        as end_date,
        {{ map_from_seed('status', 'loan_status_mapping') }}                as status,
        {{ safe_numeric_int('days_past_due') }}                             as days_past_due,
        {{ safe_string('collateral_type') }}                                as collateral_type
    from base
),

loans_with_country as (
    select
        l.*,
        c.country
    from cleaned l
    inner join {{ ref('dim_customer') }} c using (customer_id)
),

loans_with_currency as (
    select
        l.*,
        cc.currency_code
    from loans_with_country l
    inner join {{ ref('country_to_currency') }} cc
        on l.country = cc.country
)

select
    customer_id,
    loan_id,
    type,
    country,
    currency_code,
    {{ to_usd('principal', 'currency_code') }}                              as principal_usd,
    {{ to_usd('outstanding_balance', 'currency_code') }}                    as outstanding_balance_usd,
    interest_rate,
    term_months,
    monthly_payment,
    start_date,
    {{ date_key('start_date') }}                                            as start_date_key,
    end_date,
    {{ date_key('end_date') }}                                              as end_date_key,
    status,
    days_past_due,
    collateral_type
from loans_with_currency
