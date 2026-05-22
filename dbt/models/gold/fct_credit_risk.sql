{{ config(materialized='table') }}

with loan_delinquency as (
    select
        customer_id,
        bool_or(status in ('Delinquent', 'Default')) as has_delinquent_loan
    from {{ ref('fct_loans') }}
    group by customer_id
)

select
    cr.customer_id,
    c.country,
    c.customer_segment,
    c.risk_score,
    c.risk_tier,
    cr.credit_score,
    cr.credit_score_bucket,
    cr.utilization_pct,
    cr.utilization_bucket,
    cr.total_limit,
    cr.total_used,
    cr.num_credit_accounts,
    cr.oldest_account_age_months,
    cr.late_payments_12m,
    cr.inquiries_6m,
    cr.bankruptcy_flag,
    coalesce(ld.has_delinquent_loan, false) as has_delinquent_loan
from {{ ref('dim_credit_info') }} cr
join {{ ref('dim_customer') }} c using (customer_id)
left join loan_delinquency ld using (customer_id)
