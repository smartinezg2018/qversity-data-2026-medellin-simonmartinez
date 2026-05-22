-- Test: utilization_pct must be within [0, 100] after clamping
select *
from {{ ref('dim_credit_info') }}
where utilization_pct < 0 or utilization_pct > 100
