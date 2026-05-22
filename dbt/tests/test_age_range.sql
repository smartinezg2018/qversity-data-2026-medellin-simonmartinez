-- Test: customer age should be non-negative and reasonable (0-120)
select *
from {{ ref('dim_customer') }}
where age is not null
  and (age < 0 or age > 120)
