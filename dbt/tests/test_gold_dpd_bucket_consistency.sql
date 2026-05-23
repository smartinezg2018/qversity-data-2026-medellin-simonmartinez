-- Gold: dpd_bucket must match days_past_due bands
select *
from {{ ref('fct_loan_portfolio') }}
where (days_past_due = 0 and dpd_bucket != 'current')
   or (days_past_due between 1 and 30 and dpd_bucket != '1-30 days')
   or (days_past_due between 31 and 60 and dpd_bucket != '31-60 days')
   or (days_past_due between 61 and 90 and dpd_bucket != '61-90 days')
   or (days_past_due > 90 and dpd_bucket != '90+ days')
