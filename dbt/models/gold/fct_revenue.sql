{{ config(materialized='table') }}

with loan_revenue as (
    select
        customer_id,
        'loan' as channel,
        (outstanding_balance_usd * (interest_rate/100) / 12.0) as revenue_amount,
        loan_id as source_id
    from {{ ref('fct_loans') }}
    where outstanding_balance_usd is not null
      and interest_rate is not null
),

credit_card_revenue as (
    select
        customer_id,
        'credit_card' as channel,
        (balance_usd * (interest_rate/100) /12.0) as revenue_amount,
        account_id as source_id
    from {{ ref('fct_account') }}
    where account_type = 'credit_card'
      and balance_usd is not null
      and interest_rate is not null
),

fee_revenue as (
    select
        customer_id,
        'fee' as channel,
        amount_usd as revenue_amount,
        transaction_id as source_id
    from {{ ref('fct_transactions') }}
    where type = 'fee'
      and status = 'Completed'
      and amount_usd is not null
),

revenue_sources as (
    select customer_id, channel, revenue_amount, source_id
    from loan_revenue
    union all
    select customer_id, channel, revenue_amount, source_id
    from credit_card_revenue
    union all
    select customer_id, channel, revenue_amount, source_id
    from fee_revenue
)

select
    r.customer_id,
    c.customer_segment,
    c.country,
    r.channel,
    round(sum(r.revenue_amount), 2) as total_revenue_usd,
    count(*) as transaction_count
from revenue_sources r
join {{ ref('dim_customer') }} c using (customer_id)
group by r.customer_id, c.customer_segment, c.country, r.channel
