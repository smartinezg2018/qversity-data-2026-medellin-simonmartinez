-- Test: customer latitude must be clamped to [-90, 90]
select *
from {{ ref('dim_customer') }}
where lat is not null
  and (lat < -90 or lat > 90)
