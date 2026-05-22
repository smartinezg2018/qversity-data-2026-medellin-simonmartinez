{{ config(materialized='table') }}

select
    l.loan_id,
    l.customer_id,
    c.customer_segment,
    l.type                                                          as loan_type,
    l.principal_usd                                                       as principal,
    l.outstanding_balance_usd,
    l.interest_rate,
    l.monthly_payment,
    l.term_months,
    l.status                                                        as loan_status,
    l.days_past_due,
    l.collateral_type,
    l.start_date,
    l.end_date,
    round((l.outstanding_balance_usd * l.interest_rate / 100), 2)  as annual_interest_income,
    case
        when l.status in ('Delinquent', 'Default') then true
        else false
    end                                                             as is_delinquent,
    case
        when l.days_past_due = 0                then 'current'
        when l.days_past_due between 1  and 30  then '1-30 days'
        when l.days_past_due between 31 and 60  then '31-60 days'
        when l.days_past_due between 61 and 90  then '61-90 days'
        else                                         '90+ days'
    end                                                             as dpd_bucket
from {{ ref('fct_loans') }} l
join {{ ref('dim_customer') }} c using (customer_id)
