-- Test: loan end_date should be on or after start_date
select *
from {{ ref('fct_loans') }}
where start_date is not null
  and end_date is not null
  and end_date < start_date
