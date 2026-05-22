-- Test: risk_score must be within [0, 100] after clamping
select *
from {{ ref('dim_customer') }}
where risk_score < 0 or risk_score > 100
