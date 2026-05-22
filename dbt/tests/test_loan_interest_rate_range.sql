-- Test: loan interest rate must be within [0, 100] after clamping
select *
from {{ ref('fct_loans') }}
where interest_rate < 0 or interest_rate > 100
