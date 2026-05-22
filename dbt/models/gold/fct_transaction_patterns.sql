{{ config(materialized='table') }}

select
    t.transaction_id,
    t.customer_id,
    t.account_id,
    c.country                                       as customer_country,
    c.customer_segment,
    t.date                                          as transaction_date,
    to_char(t.date, 'Day')                          as day_of_week,
    extract(dow from t.date)::int                   as day_of_week_num,
    t.amount_usd                                                          as amount,
    a.currency,
    t.amount_usd,
    t.type,
    t.category,
    t.channel,
    t.status,
    t.merchant,
    (t.status = 'Failed')                           as is_failed,
    (t.type = 'transfer')                           as is_transfer
from {{ ref('fct_transactions') }} t
join {{ ref('dim_customer') }} c using (customer_id)
join {{ ref('fct_account') }} a using (account_id)
