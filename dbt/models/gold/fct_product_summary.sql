{{ config(materialized='table') }}

with loan_counts as (
    select
        customer_id,
        count(*) as loan_count
    from {{ ref('fct_loans') }}
    group by customer_id
),

account_counts as (
    select
        customer_id,
        count(*) as account_count
    from {{ ref('fct_account') }}
    group by customer_id
)

select
    a.account_id,
    a.customer_id,
    c.country,
    c.customer_segment,
    a.account_type,
    a.currency,
    a.balance,
    a.balance_usd,
    a.credit_limit,
    a.credit_limit_usd,
    a.interest_rate,
    a.status                                                        as account_status,
    a.opened_date,
    ac.account_count,
    coalesce(lc.loan_count, 0)                                      as loan_count,
    ac.account_count + coalesce(lc.loan_count, 0)                   as total_product_count
from {{ ref('fct_account') }} a
join {{ ref('dim_customer') }} c using (customer_id)
join account_counts ac using (customer_id)
left join loan_counts lc using (customer_id)
