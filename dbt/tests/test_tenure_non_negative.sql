-- Test: customer tenure should be non-negative
select *
from {{ ref('dim_customer') }}
where tenure is not null
  and tenure < 0
