-- Test: customer longitude must be clamped to [-180, 180]
select *
from {{ ref('dim_customer') }}
where lon is not null
  and (lon < -180 or lon > 180)
