-- depends_on: {{ ref('transaction_type_mapping') }}
-- depends_on: {{ ref('status_mapping') }}
{{ config(materialized='table') }}

with base as (
    select *
    from {{ source('silver', 'stg_transactions') }}
),

cleaned as (
    select
        customer_id,
        transaction_id,
        account_id,
        {{ parse_date('date') }}                                            as date,
        {{ clean_currency('amount') }}                                      as amount,
        currency,
        {{ map_from_seed('type', 'transaction_type_mapping') }}             as type,
        {{ safe_string('category') }}                                       as category,
        {{ safe_string('merchant') }}                                       as merchant,
        channel,
        {{ map_from_seed('status', 'status_mapping') }}                     as status,
        {{ safe_string('description') }}                                    as description
    from base
)

select
    customer_id,
    transaction_id,
    account_id,
    date,
    {{ date_key('date') }}                                                  as transaction_date_key,
    amount,
    currency,
    type,
    category,
    merchant,
    channel,
    status,
    description
from cleaned
